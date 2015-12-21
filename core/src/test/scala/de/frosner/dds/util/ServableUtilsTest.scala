package de.frosner.dds.util

import de.frosner.dds.servables.{KeyValueSequence, Table}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.util.StatCounter
import org.scalatest.{FlatSpec, Matchers}

// TODO check all test names (not only here), whether they still make sense (e.g. there is no JSON in core anymore)
class ServableUtilsTest extends FlatSpec with Matchers {

  "An optimal number of bins" should "be computed correctly" in {
    val testValues = List(0,1,3,5,8,12,16)
    val expectedValues = List(1,1,3,4,4,5,5)
    testValues.map(ServableUtils.optimalNumberOfBins(_)) shouldBe expectedValues
  }

  "A stat table" should "be constructed correctly from a stat counter" in {
    val statCounter = StatCounter(1D, 2D, 3D)
    val tableTitle = "Stats Table"
    val statsTable = ServableUtils.statCounterToTable(statCounter, tableTitle)

    statsTable.title shouldBe tableTitle
    statsTable.schema shouldBe StructType(List(
      StructField("count", LongType, false),
      StructField("sum", DoubleType, false),
      StructField("min", DoubleType, false),
      StructField("max", DoubleType, false),
      StructField("mean", DoubleType, false),
      StructField("stdev", DoubleType, false),
      StructField("variance", DoubleType, false)
    ))
    statsTable.content shouldBe Seq(Row(
      statCounter.count,
      statCounter.sum,
      statCounter.min,
      statCounter.max,
      statCounter.mean,
      statCounter.stdev,
      statCounter.variance
    ))
  }

  it should "be constructed correctly from multiple stat counters" in {
    val statCounter1 = StatCounter(1D, 2D, 3D)
    val statCounter2 = StatCounter(0D, 5D)
    val tableTitle = "table"
    val statsTable = ServableUtils.statCountersToTable(List("label1", "label2"), List(statCounter1, statCounter2), tableTitle)

    statsTable.title shouldBe tableTitle
    statsTable.schema shouldBe StructType(List(
      StructField("label", StringType, false),
      StructField("count", LongType, false),
      StructField("sum", DoubleType, false),
      StructField("min", DoubleType, false),
      StructField("max", DoubleType, false),
      StructField("mean", DoubleType, false),
      StructField("stdev", DoubleType, false),
      StructField("variance", DoubleType, false)
    ))
    statsTable.content shouldBe Seq(
      Row(
        "label1",
        statCounter1.count,
        statCounter1.sum,
        statCounter1.min,
        statCounter1.max,
        statCounter1.mean,
        statCounter1.stdev,
        statCounter1.variance
      ), Row(
        "label2",
        statCounter2.count,
        statCounter2.sum,
        statCounter2.min,
        statCounter2.max,
        statCounter2.mean,
        statCounter2.stdev,
        statCounter2.variance
      )
    )
  }

  "A key value sequence" should "have the correct format when constructed from single stat counter" in {
    val statCounter = StatCounter(1D, 2D, 3D)
    val sequenceTitle = "sequence"
    val statsSequence = ServableUtils.statCounterToKeyValueSequence(statCounter, sequenceTitle)
    
    statsSequence.title shouldBe sequenceTitle
    statsSequence.keyValuePairs.toList shouldBe List(
      ("Count", statCounter.count.toString),
      ("Sum", statCounter.sum.toString),
      ("Min", statCounter.min.toString),
      ("Max", statCounter.max.toString),
      ("Mean", statCounter.mean.toString),
      ("Stdev", statCounter.stdev.toString),
      ("Variance", statCounter.variance.toString)
    )
  }

}
