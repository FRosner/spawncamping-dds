package de.frosner.dds.analytics

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import de.frosner.dds.util.DataFrameUtils.{getDateFields, getNumericFields, getNominalFields, numericAsDouble, dateOrTimeAsTimestamp, anyAsAny}
import scala.collection.mutable

case class ColumnsStatisticsAggregator(schema: StructType) extends Serializable {

  private var numericAggregators: mutable.Map[Int, (NumericColumnStatisticsAggregator, StructField)] = {
    val numericFields = getNumericFields(schema)
    val numericAggregators = numericFields.map{ case (index, field) => (index, (new NumericColumnStatisticsAggregator(), field))}
    new mutable.HashMap() ++ numericAggregators
  }

  private var dateAggregators: mutable.Map[Int, (DateColumnStatisticsAggregator, StructField)] = {
    val dateFields = getDateFields(schema)
    val dateAggregators = dateFields.map{ case (index, field) => (index, (new DateColumnStatisticsAggregator(), field))}
    new mutable.HashMap() ++ dateAggregators
  }

  private var nominalAggregators: mutable.Map[Int, (NominalColumnStatisticsAggregator, StructField)] = {
    val nominalFields = getNominalFields(schema)
    val nominalAggregators = nominalFields.map{ case (index, field) => (index, (new NominalColumnStatisticsAggregator(), field))}
    new mutable.HashMap() ++ nominalAggregators
  }

  def iterate(row: Row): ColumnsStatisticsAggregator = {
    for ((idx, (agg, field)) <- numericAggregators) {
      agg.iterate(numericAsDouble(row, idx, field))
    }
    for ((idx, (agg, field)) <- dateAggregators) {
      agg.iterate(dateOrTimeAsTimestamp(row, idx, field))
    }
    for ((idx, (agg, field)) <- nominalAggregators) {
      agg.iterate(anyAsAny(row, idx, field))
    }
    this
  }

  def merge(intermediateAggregator: ColumnsStatisticsAggregator): ColumnsStatisticsAggregator = {
    require(schema == intermediateAggregator.schema, "The schemas of two aggregators to be merged must match")

    val intermediateNumericAggregators = intermediateAggregator.numericAggregators
    numericAggregators = numericAggregators.map{ case (idx, (agg, field)) => {
      val (intermediateAgg, _) = intermediateNumericAggregators(idx)
      (idx, (agg.merge(intermediateAgg), field))
    }}

    val intermediateDateAggregators = intermediateAggregator.dateAggregators
    dateAggregators = dateAggregators.map{ case (idx, (agg, field)) => {
      val (intermediateAgg, _) = intermediateDateAggregators(idx)
      (idx, (agg.merge(intermediateAgg), field))
    }}

    val intermediateNominalAggregators = intermediateAggregator.nominalAggregators
    nominalAggregators = nominalAggregators.map{ case (idx, (agg, field)) => {
      val (intermediateAgg, _) = intermediateNominalAggregators(idx)
      (idx, (agg.merge(intermediateAgg), field))
    }}

    this
  }

  def numericColumns = numericAggregators.toMap

  def dateColumns = dateAggregators.toMap

  def nominalColumns = nominalAggregators.toMap

}
