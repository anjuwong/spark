/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.fs.{BlockLocation, FileStatus, LocatedFileStatus, Path}

import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.planning.SampledPhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{DataSourceScanExec, SparkPlan}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

/**
 * A strategy for planning scans over collections of files that might be partitioned or bucketed
 * by user specified columns.
 *
 * At a high level planning occurs in several phases:
 *  - Split filters by when they need to be evaluated.
 *  - Prune the schema of the data requested based on any projections present. Today this pruning
 *    is only done on top level columns, but formats should support pruning of nested columns as
 *    well.
 *  - Construct a reader function by passing filters and the schema into the FileFormat.
 *  - Using an partition pruning predicates, enumerate the list of files that should be read.
 *  - Split the files into tasks and construct a FileScanRDD.
 *  - Add any projection or filters that must be evaluated after the scan.
 *
 * Files are assigned into tasks using the following algorithm:
 *  - If the table is bucketed, group files by bucket id into the correct number of partitions.
 *  - If the table is not bucketed or bucketing is turned off:
 *   - If any file is larger than the threshold, split it into pieces based on that threshold
 *   - Sort the files by decreasing file size.
 *   - Assign the ordered files to buckets using the following algorithm.  If the current partition
 *     is under the threshold with the addition of the next file, add it.  If not, open a new bucket
 *     and add it.  Proceed to the next file.
 */
private[sql] object FileSourceStrategy extends Strategy with Logging {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {

    case PhysicalOperation(projects, filters, l @ LogicalRelation(files: HadoopFsRelation, _, _)) =>
      logInfo("=== FILTERS ===")
      logInfo(filters.toString)
      val (readDataColumns,
        partitionColumns,
        readFile,
        finalPartitions,
        pushedDownFilters,
        prunedDataSchema,
        afterScanFilters): (Seq[Attribute],
          Seq[Attribute],
          PartitionedFile => Iterator[InternalRow],
          Seq[FilePartition],
          Seq[Filter],
          StructType,
          Seq[Expression]) = splitPartitions(projects, filters, l, files)
      scanFiles(readDataColumns,
        partitionColumns,
        files,
        readFile,
        finalPartitions,
        pushedDownFilters,
        prunedDataSchema,
        afterScanFilters,
        projects)

    case SampledPhysicalOperation(projects, filters,
      SampledLogicalRelation(sl @ LogicalRelation(files: HadoopFsRelation, _, _),
        numSamples, invertFlag)) =>
      logInfo("=== FILTERS ===")
      logInfo(filters.toString)
      logInfo("=== RELATION ===")
      logInfo(files.toString)
      val (readDataColumns,
        partitionColumns,
        readFile,
        plannedPartitions,
        pushedDownFilters,
        prunedDataSchema,
        afterScanFilters): (Seq[Attribute],
          Seq[Attribute],
          PartitionedFile => Iterator[InternalRow],
          Seq[FilePartition],
          Seq[Filter],
          StructType,
          Seq[Expression]) = splitPartitions(projects, filters, sl, files)
      val finalPartitions =
        if (numSamples < 5) {
          plannedPartitions
        } else {
          val samplePartitionLimit = math.ceil(plannedPartitions.size / numSamples.toDouble).toInt
          if (invertFlag) {
            plannedPartitions.slice(samplePartitionLimit, plannedPartitions.size)
          }
          else {
            plannedPartitions.slice(0, samplePartitionLimit)
          }
        }

      scanFiles(readDataColumns,
        partitionColumns,
        files,
        readFile,
        finalPartitions,
        pushedDownFilters,
        prunedDataSchema,
        afterScanFilters,
        projects)

    case _ => Nil
  }

  private def splitPartitions(projects: Seq[NamedExpression],
    filters: Seq[Expression],
    l: LogicalPlan,
    files: HadoopFsRelation): (Seq[Attribute],
    Seq[Attribute],
    PartitionedFile => Iterator[InternalRow],
    Seq[FilePartition],
    Seq[Filter],
    StructType,
    Seq[Expression]) = {
      // Filters on this relation fall into four categories based on where we can use them to avoid
      // reading unneeded data:
      //  - partition keys only - used to prune directories to read
      //  - bucket keys only - optionally used to prune files to read
      //  - keys stored in the data only - optionally used to skip groups of data in files
      //  - filters that need to be evaluated again after the scan
      val filterSet = ExpressionSet(filters)

      // The attribute name of predicate could be different than the one in schema in case of
      // case insensitive, we should change them to match the one in schema, so we donot need to
      // worry about case sensitivity anymore.
      logInfo("=== SPLITTING THE PARTITIONS ===")
      logInfo(l.toString)
      val normalizedFilters = filters.map { e =>
        e transform {
          case a: AttributeReference =>
            a.withName(l.output.find(_.semanticEquals(a)).get.name)
        }
      }

      val partitionColumns =
        l.resolve(files.partitionSchema, files.sparkSession.sessionState.analyzer.resolver)
      val partitionSet = AttributeSet(partitionColumns)
      val partitionKeyFilters =
        ExpressionSet(normalizedFilters.filter(_.references.subsetOf(partitionSet)))
      logInfo(s"Pruning directories with: ${partitionKeyFilters.mkString(",")}")

      val dataColumns =
        l.resolve(files.dataSchema, files.sparkSession.sessionState.analyzer.resolver)

      // Partition keys are not available in the statistics of the files.
      val dataFilters = normalizedFilters.filter(_.references.intersect(partitionSet).isEmpty)

      // Predicates with both partition keys and attributes need to be evaluated after the scan.
      val afterScanFilters = filterSet -- partitionKeyFilters
      logInfo(s"Post-Scan Filters: ${afterScanFilters.mkString(",")}")

      val selectedPartitions = files.location.listFiles(partitionKeyFilters.toSeq)

      val filterAttributes = AttributeSet(afterScanFilters)
      val requiredExpressions: Seq[NamedExpression] = filterAttributes.toSeq ++ projects
      val requiredAttributes = AttributeSet(requiredExpressions)

      val readDataColumns =
        dataColumns
          .filter(requiredAttributes.contains)
          .filterNot(partitionColumns.contains)
      val prunedDataSchema = readDataColumns.toStructType
      logInfo(s"Pruned Data Schema: ${prunedDataSchema.simpleString(5)}")

      val pushedDownFilters = dataFilters.flatMap(DataSourceStrategy.translateFilter)
      logInfo(s"Pushed Filters: ${pushedDownFilters.mkString(",")}")

      val readFile = files.fileFormat.buildReader(
        sparkSession = files.sparkSession,
        dataSchema = files.dataSchema,
        partitionSchema = files.partitionSchema,
        requiredSchema = prunedDataSchema,
        filters = pushedDownFilters,
        options = files.options,
        hadoopConf = files.sparkSession.sessionState.newHadoopConfWithOptions(files.options))

      val plannedPartitions = files.bucketSpec match {
        case Some(bucketing) if files.sparkSession.sessionState.conf.bucketingEnabled =>
          logInfo(s"Planning with ${bucketing.numBuckets} buckets")
          val bucketed =
            selectedPartitions.flatMap { p =>
              p.files.map { f =>
                val hosts = getBlockHosts(getBlockLocations(f), 0, f.getLen)
                PartitionedFile(p.values, f.getPath.toUri.toString, 0, f.getLen, hosts)
              }
            }.groupBy { f =>
              BucketingUtils
                .getBucketId(new Path(f.filePath).getName)
                .getOrElse(sys.error(s"Invalid bucket file ${f.filePath}"))
            }

          (0 until bucketing.numBuckets).map { bucketId =>
            FilePartition(bucketId, bucketed.getOrElse(bucketId, Nil))
          }

        // @anjuwong Here, we actually read a file to RDD
        case _ =>
          val defaultMaxSplitBytes = files.sparkSession.sessionState.conf.filesMaxPartitionBytes
          val openCostInBytes = files.sparkSession.sessionState.conf.filesOpenCostInBytes
          val defaultParallelism = files.sparkSession.sparkContext.defaultParallelism
          val totalBytes = selectedPartitions.flatMap(_.files.map(_.getLen + openCostInBytes)).sum
          val bytesPerCore = totalBytes / defaultParallelism
          val maxSplitBytes = Math.min(defaultMaxSplitBytes,
            Math.max(openCostInBytes, bytesPerCore))
          logInfo(s"Planning scan with bin packing, max size: $maxSplitBytes bytes, " +
            s"open cost is considered as scanning $openCostInBytes bytes.")


          // @anjuwong I think this is where the multiple partitions are read
          val splitFiles = selectedPartitions.flatMap { partition =>
            partition.files.flatMap { file =>
              val blockLocations = getBlockLocations(file)

              // @anjuwong For each file, get the locations of the files and
              //   iterate through each file. offset is the start of a given
              //   block, remaining defines how much of the file remains,
              //   returns the PartitionedFile chunk, which gets flatMapped
              //   splitFiles is a Seq[PartitionedFile], at most maxSplitBytes
              // * I think this only reads partitions from file and doesn't
              //   work with .repartition() at all
              (0L until file.getLen by maxSplitBytes).map { offset =>
                val remaining = file.getLen - offset
                val size = if (remaining > maxSplitBytes) maxSplitBytes else remaining
                val hosts = getBlockHosts(blockLocations, offset, size)
                PartitionedFile(partition.values, file.getPath.toUri.toString, offset, size, hosts)
              }
            }
          }.toArray.sortBy(_.length)(implicitly[Ordering[Long]].reverse)
          // logInfo("=== HERE ARE THE FILE PARTITIONS ===")
          // splitFiles.foreach({ file => logInfo(file.toString)})
          logInfo("=== HERE ARE THE FILE COUNTS plannedPartitions ===")
          logInfo(splitFiles.length + " splitFiles")
          val partitions = new ArrayBuffer[FilePartition]
          val currentFiles = new ArrayBuffer[PartitionedFile]
          var currentSize = 0L

          /** Add the given file to the current partition. */
          def addFile(file: PartitionedFile): Unit = {
            currentSize += file.length + openCostInBytes
            currentFiles.append(file)
          }

          /** Close the current partition and move to the next. */
          def closePartition(): Unit = {
            if (currentFiles.nonEmpty) {
              val newPartition =
                FilePartition(
                  partitions.size,
                  currentFiles.toArray.toSeq) // Copy to a new Array.
              partitions.append(newPartition)
            }
            currentFiles.clear()
            currentSize = 0
          }

          // Assign files to partitions using "First Fit Decreasing" (FFD)
          // TODO: consider adding a slop factor here?
          splitFiles.foreach { file =>
            if (currentSize + file.length > maxSplitBytes) {
              closePartition()
            }
            addFile(file)
          }
          closePartition()
          logInfo("=== HERE ARE THE PARTITION COUNTS plannedPartitions ===")
          logInfo(partitions.size + " partitions")
          partitions
      }
      (readDataColumns,
      partitionColumns,
      readFile,
      plannedPartitions,
      pushedDownFilters,
      prunedDataSchema,
      afterScanFilters.toSeq)
  }

  /* @anjuwong Create the scan execution with the proper filters and projections */
  private def scanFiles(
    readDataColumns: Seq[Attribute],
    partitionColumns: Seq[Attribute],
    files: HadoopFsRelation,
    readFile: PartitionedFile => Iterator[InternalRow],
    finalPartitions: Seq[FilePartition],
    pushedDownFilters: Seq[Filter],
    prunedDataSchema: StructType,
    afterScanFilters: Seq[Expression],
    projects: Seq[NamedExpression]): Seq[SparkPlan] = {
    val scan =
      DataSourceScanExec.create(
        readDataColumns ++ partitionColumns,
        new FileScanRDD(
          files.sparkSession,
          readFile,
          finalPartitions),
        files,
        Map(
          "Format" -> files.fileFormat.toString,
          "PushedFilters" -> pushedDownFilters.mkString("[", ", ", "]"),
          "ReadSchema" -> prunedDataSchema.simpleString))

    val afterScanFilter = afterScanFilters.toSeq.reduceOption(expressions.And)
    val withFilter = afterScanFilter.map(execution.FilterExec(_, scan)).getOrElse(scan)
    val withProjections = if (projects == withFilter.output) {
      withFilter
    } else {
      execution.ProjectExec(projects, withFilter)
    }

    withProjections :: Nil
  }

  private def getBlockLocations(file: FileStatus): Array[BlockLocation] = file match {
    case f: LocatedFileStatus => f.getBlockLocations
    case f => Array.empty[BlockLocation]
  }

  // Given locations of all blocks of a single file, `blockLocations`, and an `(offset, length)`
  // pair that represents a segment of the same file, find out the block that contains the largest
  // fraction the segment, and returns location hosts of that block. If no such block can be found,
  // returns an empty array.
  private def getBlockHosts(
      blockLocations: Array[BlockLocation], offset: Long, length: Long): Array[String] = {
    val candidates = blockLocations.map {
      // The fragment starts from a position within this block
      case b if b.getOffset <= offset && offset < b.getOffset + b.getLength =>
        b.getHosts -> (b.getOffset + b.getLength - offset).min(length)

      // The fragment ends at a position within this block
      case b if offset <= b.getOffset && offset + length < b.getLength =>
        b.getHosts -> (offset + length - b.getOffset).min(length)

      // The fragment fully contains this block
      case b if offset <= b.getOffset && b.getOffset + b.getLength <= offset + length =>
        b.getHosts -> b.getLength

      // The fragment doesn't intersect with this block
      case b =>
        b.getHosts -> 0L
    }.filter { case (hosts, size) =>
      size > 0L
    }

    if (candidates.isEmpty) {
      Array.empty[String]
    } else {
      val (hosts, _) = candidates.maxBy { case (_, size) => size }
      hosts
    }
  }
}
