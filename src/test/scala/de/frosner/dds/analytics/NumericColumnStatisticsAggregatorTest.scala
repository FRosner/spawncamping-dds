package de.frosner.dds.analytics

import org.scalatest.{FlatSpec, Matchers}

class NumericColumnStatisticsAggregatorTest extends FlatSpec with Matchers {

  val epsilon = 0.000001

  "A numeric column statistics aggregator" should "be initialized properly for Integer" in {
    val agg = new NumericColumnStatisticsAggregator[Int]
    agg.totalCount shouldBe 0
    agg.missingCount shouldBe 0
    agg.sum shouldBe 0
    agg.sumOfSquares shouldBe 0
  }

  it should "be initialized properly for BigDecimal" in {
    val agg = new NumericColumnStatisticsAggregator[BigDecimal]
    agg.totalCount shouldBe BigDecimal(0)
    agg.missingCount shouldBe BigDecimal(0)
    agg.sum shouldBe BigDecimal(0)
    agg.sumOfSquares shouldBe BigDecimal(0)
  }

  it should "compute the correct sums" in {
    val agg = new NumericColumnStatisticsAggregator[Int]
    agg.iterate(Option(5))
    agg.sum shouldBe 5
    agg.iterate(Option.empty)
    agg.sum shouldBe 5
    agg.iterate(Option(1))
    agg.sum shouldBe 6
  }

  it should "compute the correct sums of squares" in {
    val agg = new NumericColumnStatisticsAggregator[Double]
    agg.iterate(Option(5d))
    agg.sumOfSquares shouldBe 25d +- epsilon
    agg.iterate(Option(10d))
    agg.sumOfSquares shouldBe 125d +- epsilon
    agg.iterate(Option.empty)
    agg.sumOfSquares shouldBe 125d +- epsilon
  }

  it should "compute the correct total count" in {
    val agg = new NumericColumnStatisticsAggregator[BigInt]
    agg.iterate(Option(BigInt(5)))
    agg.totalCount shouldBe 1l
    agg.iterate(Option(BigInt(1)))
    agg.totalCount shouldBe 2l
    agg.iterate(Option.empty)
    agg.totalCount shouldBe 3l
  }

  it should "compute the missing value count" in {
    val agg = new NumericColumnStatisticsAggregator[Float]
    agg.iterate(Option(5f))
    agg.missingCount shouldBe 0l
    agg.iterate(Option.empty)
    agg.missingCount shouldBe 1l
    agg.iterate(Option(2l))
    agg.missingCount shouldBe 1l
  }

  it should "merge two sums correctly" in {
    val agg1 = new NumericColumnStatisticsAggregator[Byte]
    agg1.iterate(Option(1.toByte))
    val agg2 = new NumericColumnStatisticsAggregator[Byte]
    agg2.iterate(Option(2.toByte))
    agg1.merge(agg2).sum shouldBe 3.toByte
  }

  it should "merge two sums of squares correctly" in {
    val agg1 = new NumericColumnStatisticsAggregator[Int]
    agg1.iterate(Option(1))
    val agg2 = new NumericColumnStatisticsAggregator[Int]
    agg2.iterate(Option(2))
    agg1.merge(agg2).sumOfSquares shouldBe 5
  }

  it should "merge two total counts correctly" in {
    val agg1 = new NumericColumnStatisticsAggregator[Int]
    agg1.iterate(Option(1))
    val agg2 = new NumericColumnStatisticsAggregator[Int]
    agg2.iterate(Option(2))
    agg1.merge(agg2).totalCount shouldBe 2
  }

  it should "merge two missing value counts correctly" in {
    val agg1 = new NumericColumnStatisticsAggregator[Int]
    agg1.iterate(Option(1))
    val agg2 = new NumericColumnStatisticsAggregator[Int]
    agg2.iterate(Option.empty)
    agg1.merge(agg2).missingCount shouldBe 1
  }

}
