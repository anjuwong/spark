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

 // scalastyle:off
 /*
 * @author: Andrew Wong (anjuwong)
 * UCLA CS188, Spring 2016
 * Professor Paul Eggert
 */
 // scalastyle:on

package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeProjection}
import org.apache.spark.sql.execution.metric.SQLMetrics


/**
 * Physical plan node for scanning a subset of data from a local collection.
 */
private[sql] case class LocalTableSampleScanExec(
    output: Seq[Attribute],
    rows: Seq[InternalRow],
    invertFlag: Boolean = false) extends LeafExecNode {

  private[sql] override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  private val unsafeRows: Array[InternalRow] = {
    val proj = UnsafeProjection.create(output, output)
    rows.map(r => proj(r).copy()).toArray
  }

// scalastyle:off
  /* First partition into the chunks to be partitioned on at the caller level */
  private lazy val rddParallels = sqlContext.sparkContext.parallelize(unsafeRows, 5)

  /* Get the desired chunks */
  private lazy val rdd = getManyRDDChunks(rddParallels, partitionIndices, sqlContext.sparkContext.emptyRDD[InternalRow])

  /* If the invert flag is on, return the complement of the first partition, otherwise return the first partition*/
  private lazy val partitionIndices = if (invertFlag) {
    /* TODO: don't hardcode these. Take in integer partitionNumber as input */
    List(1, 2, 3, 4)
  } else {
    List(0)
  }

  def getManyRDDChunks(r: RDD[InternalRow], partitionIndices: List[Int], retRDD: RDD[InternalRow]): RDD[InternalRow] = partitionIndices match {
    /* TODO: Add error checking on the bounds of num */
    case num :: rest =>
      val rddChunk: RDD[InternalRow] = r.mapPartitionsWithIndex((idx, iter:Iterator[InternalRow]) => 
        if (idx == num) iter else Iterator())
      getManyRDDChunks(r, rest, retRDD.union(rddChunk))
    case _ => retRDD
  }
// scalastyle:on

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    rdd.map { r =>
      numOutputRows += 1
      r
    }
  }

  override def executeCollect(): Array[InternalRow] = {
    unsafeRows
  }

  override def executeTake(limit: Int): Array[InternalRow] = {
    unsafeRows.take(limit)
  }
}
