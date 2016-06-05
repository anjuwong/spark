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

package org.apache.spark.sql.execution

import java.nio.charset.StandardCharsets
import java.sql.Timestamp

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{AnalysisException, Row, SparkSession, SQLContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.UnsupportedOperationChecker
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LocalRelationSample, LogicalPlan, ReturnAnswer}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.command.{DescribeTableCommand, ExecutedCommandExec}
import org.apache.spark.sql.execution.exchange.{EnsureRequirements, ReuseExchange}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BinaryType, DateType, DecimalType, TimestampType, _}


/**
 * The primary workflow for executing relational queries using Spark.  Designed to allow easy
 * access to the intermediate phases of query execution for developers.
 *
 * While this is not a public class, we should avoid changing the function names for the sake of
 * changing them, because a lot of developers use the feature for debugging.
 */
class QueryExecution(val sparkSession: SparkSession, val logical: LogicalPlan) extends Logging {

  // TODO: Move the planner an optimizer into here from SessionState.
  protected def planner = sparkSession.sessionState.planner

  def assertAnalyzed(): Unit = {
    try sparkSession.sessionState.analyzer.checkAnalysis(analyzed) catch {
      case e: AnalysisException =>
        val ae = new AnalysisException(e.message, e.line, e.startPosition, Some(analyzed))
        ae.setStackTrace(e.getStackTrace)
        throw ae
    }
  }

  def assertSupported(): Unit = {
    if (sparkSession.sessionState.conf.getConf(SQLConf.UNSUPPORTED_OPERATION_CHECK_ENABLED)) {
      UnsupportedOperationChecker.checkForBatch(analyzed)
    }
  }

  lazy val analyzed: LogicalPlan = {
    SQLContext.setActive(sparkSession.wrapped)
    sparkSession.sessionState.analyzer.execute(logical)
  }

  lazy val withCachedData: LogicalPlan = {
    assertAnalyzed()
    assertSupported()
    sparkSession.cacheManager.useCachedData(analyzed)
  }

  lazy val optimizedPlan: LogicalPlan = sparkSession.sessionState.optimizer.execute(withCachedData)


// TODO: move back to separate sample files and create a new package for them
// scalastyle: off
  /**
   * @author anjuwong
   * Checks whether or not the given plan is a multijoin
   */
  val empty = if (checkMultiJoins(optimizedPlan)) {
    logInfo("==== EXECUTING A MULTI-JOIN ====")
    logInfo(optimizedPlan.toString)
  }
  else {
    logInfo("==== PLAN IS NOT A MULTI-JOIN ====")
    logInfo(optimizedPlan.toString)
  }

  def isMultiJoin(plan: LogicalPlan): Boolean = plan match {
    case GlobalLimit(_, Project(_, Join(Project(_, Join(_, _, Inner, _)), _, Inner, _))) => true
    case LocalLimit(_, Project(_, Join(Project(_, Join(_, _, Inner, _)), _, Inner, _))) => true
    /* These are the cases that fixed the camel's back */
    case GlobalLimit(_, Project(_, Join(Join(_, _, Inner, _), _, Inner, _))) => true
    case LocalLimit(_, Project(_, Join(Join(_, _, Inner, _), _, Inner, _))) => true
    case Project(_, Join(Project(_, Join(_, _, Inner, _)), _, Inner, _)) => true
    case Join(Project(_, Join(_, _, Inner, _)), _, Inner, _) => true
    case Join(Join(_, _, Inner, _), _, Inner, _) => true
    case _ => false
  }

  def swapJoins(plan: LogicalPlan): LogicalPlan = plan match {
    case GlobalLimit(e, Project(pl1, Join(Project(pl2, Join(a, b, Inner, cd1)), c, Inner, cd2))) =>
      GlobalLimit(e, Project(pl1, Join(Project(pl2, Join(a, c, Inner, cd2)), b, Inner, cd1)))
    case LocalLimit(e, Project(pl1, Join(Project(pl2, Join(a, b, Inner, cd1)), c, Inner, cd2))) =>
      LocalLimit(e, Project(pl1, Join(Project(pl2, Join(a, c, Inner, cd2)), b, Inner, cd1)))
    /* These are the cases that fixed the camel's back */
    case GlobalLimit(e, Project(pl, Join(Join(a, b, Inner, cd1), c, Inner, cd2))) =>
      GlobalLimit(e, Project(pl, Join(Join(a, c, Inner, cd2), b, Inner, cd1)))
    case LocalLimit(e, Project(pl, Join(Join(a, b, Inner, cd1), c, Inner, cd2))) =>
      LocalLimit(e, Project(pl, Join(Join(a, c, Inner, cd2), b, Inner, cd1)))
    case Project(pl1, Join(Project(pl2, Join(a, b, Inner, cd1)), c, Inner, cd2)) =>
      Project(pl1, Join(Project(pl2, Join(a, c, Inner, cd2)), b, Inner, cd1))
    case Join(Project(pl, Join(a, b, Inner, cd1)), c, Inner, cd2) =>
      Join(Project(pl, Join(a, c, Inner, cd2)), b, Inner, cd1)
    case Join(Join(a, b, Inner, cd1), c, Inner, cd2) =>
      Join(Join(a, c, Inner, cd2), b, Inner, cd1)
    case p => p
  }

  def samplePlan(plan: LogicalPlan, invertFlag: Boolean): LogicalPlan = {
    // search for LogicalRelation or PhysicalOperation(_, _, LogicalRelation)
    plan.mapChildren(node => node match {
        case LocalRelation(output, data) => logInfo("Sampling a local relation")
        LocalRelationSample(output, data, invertFlag)
        case _ => node
      })
  }

  def checkMultiJoins(plan: LogicalPlan): Boolean = {
    /* Map will check each node for the MULTIJOIN and return a preorder Seq[Boolean]
     * Then, we check to see if any of the children were MULTIJOINS */
    val multiJoinSeq = plan.map({
        case Join(Join(a, b, Inner, cd1), c, Inner, cd2) => true
        case Project(pl1, Join(Project(pl2, Join(a, b, Inner, cd1)), c, Inner, cd2)) => true
        case _ => false
    })
    multiJoinSeq.exists(_ == true)
  }
  // def checkMultiJoins(plan: LogicalPlan): Boolean = plan match {
  //   case leaf: LeafNode => false
  //   case Join(Join(a, b, Inner, cd1) c, Inner, cd2) => true
  //   case Join(Project(pl1, Join(Project(pl2, Join(a, b, Inner, cd1)), c, Inner, cd2))) => true
  //   case node => val childMultiSeq = node.map(checkMultiJoins)
  //     childMultiSeq.exists(_ == true)
  // }
  def swapMultiJoins(plan: LogicalPlan): LogicalPlan = plan match {
    // leafnode => return
    // JOIN(JOIN(a,b), c) => JOIN(JOIN(a,c), b)
    // node => node.mapChildren(swapChildJoins(node))
    // case Join() => Join()
    case leaf: LeafNode => leaf
    case Join(Join(a, b, Inner, cd1), c, Inner, cd2) =>
      Join(Join(a, c, Inner, cd2), b, Inner, cd1)
    case Project(pl1, Join(Project(pl2, Join(a, b, Inner, cd1)), c, Inner, cd2)) =>
      Project(pl1, Join(Project(pl2, Join(a, c, Inner, cd2)), b, Inner, cd1))
    case node => node.mapChildren(swapMultiJoins)
  }

  /* At this point the data is already bound to the bottommost node as LocalRelation(output, data)
   * The data is in the form Seq[InternalRow] in LocalRelation
   * This gets casted to the leafExecNode LocalTableScanExec
   * Instead of this, I want to:
   *   1) swap the Join order
   *   2) replace LocalRelation with LocalRelationSample
   */
  lazy val swappedJoins = swapJoins(optimizedPlan)
  lazy val optimizedSubPlanOrig = samplePlan(optimizedPlan, false)
  lazy val optimizedSubPlanSwap = samplePlan(swappedJoins, false)

  lazy val sparkSampledPlanOrig: SparkPlan = {
    SQLContext.setActive(sparkSession.wrapped)
    planner.plan(ReturnAnswer(optimizedSubPlanOrig)).next()
  }
  lazy val sparkSampledPlanSwap: SparkPlan = {
    SQLContext.setActive(sparkSession.wrapped)
    planner.plan(ReturnAnswer(optimizedSubPlanSwap)).next()
  }

  lazy val executedSampledPlanOrig: SparkPlan = prepareForExecution(sparkSampledPlanOrig)
  lazy val executedSampledPlanSwap: SparkPlan = prepareForExecution(sparkSampledPlanSwap)

  lazy val toSampledRddOrig: RDD[InternalRow] = executedSampledPlanOrig.execute()
  lazy val toSampledRddSwap: RDD[InternalRow] = executedSampledPlanSwap.execute()

  lazy val (restOfOptimizedPlan, sampleRDD): (LogicalPlan, RDD[InternalRow]) =
  if (toSampledRddOrig.count() > toSampledRddSwap.count()) {
    /* Run the rest of the query,  */
    logInfo("@anjuwong Swapped JOIN is cheaper, executing the rest!")
    (samplePlan(swappedJoins, true), toSampledRddSwap)
  } else {
    logInfo("Original JOIN is cheaper, keeping it the same!")
    (samplePlan(optimizedPlan, true), toSampledRddOrig)
  }
  lazy val restOfSparkPlan: SparkPlan = {
    SQLContext.setActive(sparkSession.wrapped)
    planner.plan(ReturnAnswer(restOfOptimizedPlan)).next()
  }

  lazy val restOfExecutedPlan: SparkPlan = prepareForExecution(restOfSparkPlan)

  lazy val restOfToRDD: RDD[InternalRow] = restOfExecutedPlan.execute()

  lazy val sparkPlan: SparkPlan = {
    SQLContext.setActive(sparkSession.wrapped)
    planner.plan(ReturnAnswer(samplePlan(optimizedPlan, false))).next()
  }

  // executedPlan should not be used to initialize any SparkPlan. It should be
  // only used for execution.
  lazy val executedPlan: SparkPlan = prepareForExecution(sparkPlan)

  /** Internal version of the rdd. Avoids copies and has no schema */
  lazy val toRdd: RDD[InternalRow] = if (isMultiJoin(optimizedPlan)) {
    sampleRDD.union(restOfToRDD)
  } else {
    executedPlan.execute()
  }
// scalastyle: on

  /**
   * Prepares a planned [[SparkPlan]] for execution by inserting shuffle operations and internal
   * row format conversions as needed.
   */
  protected def prepareForExecution(plan: SparkPlan): SparkPlan = {
    preparations.foldLeft(plan) { case (sp, rule) => rule.apply(sp) }
  }

  /** A sequence of rules that will be applied in order to the physical plan before execution. */
  protected def preparations: Seq[Rule[SparkPlan]] = Seq(
    python.ExtractPythonUDFs,
    PlanSubqueries(sparkSession),
    EnsureRequirements(sparkSession.sessionState.conf),
    CollapseCodegenStages(sparkSession.sessionState.conf),
    ReuseExchange(sparkSession.sessionState.conf))

  protected def stringOrError[A](f: => A): String =
    try f.toString catch { case e: Throwable => e.toString }

  /**
   * Returns the result as a hive compatible sequence of strings.  For native commands, the
   * execution is simply passed back to Hive.
   */
  def hiveResultString(): Seq[String] = executedPlan match {
    case ExecutedCommandExec(desc: DescribeTableCommand) =>
      // If it is a describe command for a Hive table, we want to have the output format
      // be similar with Hive.
      desc.run(sparkSession).map {
        case Row(name: String, dataType: String, comment) =>
          Seq(name, dataType,
            Option(comment.asInstanceOf[String]).getOrElse(""))
            .map(s => String.format(s"%-20s", s))
            .mkString("\t")
      }
    case command: ExecutedCommandExec =>
      command.executeCollect().map(_.getString(0))

    case other =>
      val result: Seq[Seq[Any]] = other.executeCollectPublic().map(_.toSeq).toSeq
      // We need the types so we can output struct field names
      val types = analyzed.output.map(_.dataType)
      // Reformat to match hive tab delimited output.
      result.map(_.zip(types).map(toHiveString)).map(_.mkString("\t")).toSeq
  }

  /** Formats a datum (based on the given data type) and returns the string representation. */
  private def toHiveString(a: (Any, DataType)): String = {
    val primitiveTypes = Seq(StringType, IntegerType, LongType, DoubleType, FloatType,
      BooleanType, ByteType, ShortType, DateType, TimestampType, BinaryType)

    /** Implementation following Hive's TimestampWritable.toString */
    def formatTimestamp(timestamp: Timestamp): String = {
      val timestampString = timestamp.toString
      if (timestampString.length() > 19) {
        if (timestampString.length() == 21) {
          if (timestampString.substring(19).compareTo(".0") == 0) {
            return DateTimeUtils.threadLocalTimestampFormat.get().format(timestamp)
          }
        }
        return DateTimeUtils.threadLocalTimestampFormat.get().format(timestamp) +
          timestampString.substring(19)
      }

      return DateTimeUtils.threadLocalTimestampFormat.get().format(timestamp)
    }

    def formatDecimal(d: java.math.BigDecimal): String = {
      if (d.compareTo(java.math.BigDecimal.ZERO) == 0) {
        java.math.BigDecimal.ZERO.toPlainString
      } else {
        d.stripTrailingZeros().toPlainString
      }
    }

    /** Hive outputs fields of structs slightly differently than top level attributes. */
    def toHiveStructString(a: (Any, DataType)): String = a match {
      case (struct: Row, StructType(fields)) =>
        struct.toSeq.zip(fields).map {
          case (v, t) => s""""${t.name}":${toHiveStructString(v, t.dataType)}"""
        }.mkString("{", ",", "}")
      case (seq: Seq[_], ArrayType(typ, _)) =>
        seq.map(v => (v, typ)).map(toHiveStructString).mkString("[", ",", "]")
      case (map: Map[_, _], MapType(kType, vType, _)) =>
        map.map {
          case (key, value) =>
            toHiveStructString((key, kType)) + ":" + toHiveStructString((value, vType))
        }.toSeq.sorted.mkString("{", ",", "}")
      case (null, _) => "null"
      case (s: String, StringType) => "\"" + s + "\""
      case (decimal, DecimalType()) => decimal.toString
      case (other, tpe) if primitiveTypes contains tpe => other.toString
    }

    a match {
      case (struct: Row, StructType(fields)) =>
        struct.toSeq.zip(fields).map {
          case (v, t) => s""""${t.name}":${toHiveStructString(v, t.dataType)}"""
        }.mkString("{", ",", "}")
      case (seq: Seq[_], ArrayType(typ, _)) =>
        seq.map(v => (v, typ)).map(toHiveStructString).mkString("[", ",", "]")
      case (map: Map[_, _], MapType(kType, vType, _)) =>
        map.map {
          case (key, value) =>
            toHiveStructString((key, kType)) + ":" + toHiveStructString((value, vType))
        }.toSeq.sorted.mkString("{", ",", "}")
      case (null, _) => "NULL"
      case (d: Int, DateType) => new java.util.Date(DateTimeUtils.daysToMillis(d)).toString
      case (t: Timestamp, TimestampType) => formatTimestamp(t)
      case (bin: Array[Byte], BinaryType) => new String(bin, StandardCharsets.UTF_8)
      case (decimal: java.math.BigDecimal, DecimalType()) => formatDecimal(decimal)
      case (other, tpe) if primitiveTypes.contains(tpe) => other.toString
    }
  }

  def simpleString: String = {
    s"""== Physical Plan ==
       |${stringOrError(executedPlan)}
      """.stripMargin.trim
  }

  override def toString: String = {
    def output =
      analyzed.output.map(o => s"${o.name}: ${o.dataType.simpleString}").mkString(", ")

    s"""== Parsed Logical Plan ==
       |${stringOrError(logical)}
       |== Analyzed Logical Plan ==
       |${stringOrError(output)}
       |${stringOrError(analyzed)}
       |== Optimized Logical Plan ==
       |${stringOrError(optimizedPlan)}
       |== Physical Plan ==
       |${stringOrError(executedPlan)}
    """.stripMargin.trim
  }

  /** A special namespace for commands that can be used to debug query execution. */
  // scalastyle:off
  object debug {
  // scalastyle:on

    /**
     * Prints to stdout all the generated code found in this plan (i.e. the output of each
     * WholeStageCodegen subtree).
     */
    def codegen(): Unit = {
      // scalastyle:off println
      println(org.apache.spark.sql.execution.debug.codegenString(executedPlan))
      // scalastyle:on println
    }
  }
}
