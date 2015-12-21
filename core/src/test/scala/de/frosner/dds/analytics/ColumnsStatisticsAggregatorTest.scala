package de.frosner.dds.analytics

import java.sql.Date

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.{FlatSpec, Matchers}

class ColumnsStatisticsAggregatorTest extends FlatSpec with Matchers {

  "A column statistics aggregator" should "be initialized properly" in {
    val dateColumn = StructField("1", DateType, false)
    val intColumn = StructField("2", IntegerType, false)
    val stringColumn = StructField("3", StringType, false)
    val schema = StructType(List(dateColumn, intColumn, stringColumn))
    val agg = ColumnsStatisticsAggregator(schema)

    val actualNumericColumns = agg.numericColumns
    actualNumericColumns.size shouldBe 1
    val (actualNumericAggregator, actualNumericColumn) = actualNumericColumns(1)
    actualNumericAggregator.totalCount shouldBe 0l
    actualNumericColumn shouldBe intColumn

    val actualDateColumns = agg.dateColumns
    actualDateColumns.size shouldBe 1
    val (actualDateAggregator, actualDateColumn) = actualDateColumns(0)
    actualDateAggregator.totalCount shouldBe 0l
    actualDateColumn shouldBe dateColumn

    val actualNominalColumns = agg.nominalColumns
    actualNominalColumns.size shouldBe 1
    val (actualNominalAggregator, actualNominalColumn) = actualNominalColumns(2)
    actualNominalAggregator.totalCount shouldBe 0l
    actualNominalColumn shouldBe stringColumn
  }

  it should "update the aggregators when iterating" in {
    val dateColumn = StructField("1", DateType, false)
    val intColumn = StructField("2", IntegerType, false)
    val stringColumn = StructField("3", StringType, false)
    val schema = StructType(List(dateColumn, intColumn, stringColumn))
    val agg = ColumnsStatisticsAggregator(schema)

    for (expectedCount <- List(1l,2l)) {
      agg.iterate(Row(new Date(500), 10, "b"))

      val (actualNumericAggregator, _) = agg.numericColumns(1)
      actualNumericAggregator.totalCount shouldBe expectedCount

      val (actualDateAggregator, _) = agg.dateColumns(0)
      actualDateAggregator.totalCount shouldBe expectedCount

      val (actualNominalAggregator, _) = agg.nominalColumns(2)
      actualNominalAggregator.totalCount shouldBe expectedCount
    }
  }

  it should "update the aggregators when merging" in {
    val dateColumn = StructField("1", DateType, false)
    val intColumn = StructField("2", IntegerType, false)
    val stringColumn = StructField("3", StringType, false)
    val schema = StructType(List(dateColumn, intColumn, stringColumn))
    val agg1 = ColumnsStatisticsAggregator(schema)
    val agg2 = ColumnsStatisticsAggregator(schema)

    agg1.iterate(Row(new Date(500), 10, "b"))
    agg1.iterate(Row(new Date(500), 10, "b"))
    agg2.iterate(Row(new Date(500), 10, "b"))

    val merged = agg1.merge(agg2)

    val (actualNumericAggregator, _) = merged.numericColumns(1)
    actualNumericAggregator.totalCount shouldBe 3l

    val (actualDateAggregator, _) = merged.dateColumns(0)
    actualDateAggregator.totalCount shouldBe 3l

    val (actualNominalAggregator, _) = merged.nominalColumns(2)
    actualNominalAggregator.totalCount shouldBe 3l
  }

  it should "only merge two aggregators having the same schema" in {
    val dateColumn = StructField("1", DateType, false)
    val intColumn = StructField("2", IntegerType, false)
    val stringColumn = StructField("3", StringType, false)
    val schema1 = StructType(List(intColumn, stringColumn))
    val schema2 = StructType(List(dateColumn, stringColumn))
    val agg1 = ColumnsStatisticsAggregator(schema1)
    val agg2 = ColumnsStatisticsAggregator(schema2)
    intercept[IllegalArgumentException] {
      agg1.merge(agg2)
    }
  }

}
