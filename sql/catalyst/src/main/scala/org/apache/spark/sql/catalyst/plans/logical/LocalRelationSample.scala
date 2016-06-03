/*
 * @author: Andrew Wong (anjuwong)
 * UCLA CS188, Spring 2016
 * Professor Paul Eggert
 */

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.{analysis, CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.types.{StructField, StructType}

object LocalRelationSample {
  def apply(output: Attribute*): LocalRelation = new LocalRelation(output)

  def apply(output1: StructField, output: StructField*): LocalRelation = {
    new LocalRelation(StructType(output1 +: output).toAttributes)
  }

  def fromExternalRows(output: Seq[Attribute], data: Seq[Row]): LocalRelation = {
    val schema = StructType.fromAttributes(output)
    val converter = CatalystTypeConverters.createToCatalystConverter(schema)
    LocalRelation(output, data.map(converter(_).asInstanceOf[InternalRow]))
  }

  def fromProduct(output: Seq[Attribute], data: Seq[Product]): LocalRelation = {
    val schema = StructType.fromAttributes(output)
    val converter = CatalystTypeConverters.createToCatalystConverter(schema)
    LocalRelation(output, data.map(converter(_).asInstanceOf[InternalRow]))
  }
}

case class LocalRelationSample(output: Seq[Attribute], data: Seq[InternalRow] = Nil, invertFlag: Boolean = false)
  extends LeafNode with analysis.MultiInstanceRelation {

  // A local relation must have resolved output.
  require(output.forall(_.resolved), "Unresolved attributes found when constructing LocalRelation.")

  /**
   * Returns an identical copy of this relation with new exprIds for all attributes.  Different
   * attributes are required when a relation is going to be included multiple times in the same
   * query.
   */
  override final def newInstance(): this.type = {
    LocalRelation(output.map(_.newInstance()), data).asInstanceOf[this.type]
  }

  override protected def stringArgs = Iterator(output)

  override def sameResult(plan: LogicalPlan): Boolean = plan match {
    case LocalRelation(otherOutput, otherData) =>
      otherOutput.map(_.dataType) == output.map(_.dataType) && otherData == data
    case _ => false
  }

  override lazy val statistics =
    Statistics(sizeInBytes = output.map(_.dataType.defaultSize).sum * data.length)
}
