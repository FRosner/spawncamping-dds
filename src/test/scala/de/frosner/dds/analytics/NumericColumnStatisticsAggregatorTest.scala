package de.frosner.dds.analytics

import org.scalatest.{FlatSpec, Matchers}

class NumericColumnStatisticsAggregatorTest extends FlatSpec with Matchers {

  val epsilon = 0.000001

  "A numeric column statistics aggregator" should "be initialized properly" in {
    val agg = new NumericColumnStatisticsAggregator
    agg.totalCount shouldBe 0l
    agg.missingCount shouldBe 0l
    agg.sum shouldBe 0d
    agg.min shouldBe Double.PositiveInfinity
    agg.max shouldBe Double.NegativeInfinity
    agg.mean.isNaN shouldBe true
    agg.variance.isNaN shouldBe true
    agg.stdev.isNaN shouldBe true
  }

  it should "compute the correct total count" in {
    val agg = new NumericColumnStatisticsAggregator
    agg.iterate(Option(5d))
    agg.totalCount shouldBe 1l
    agg.iterate(Option(2d))
    agg.totalCount shouldBe 2l
    agg.iterate(Option.empty)
    agg.totalCount shouldBe 3l
  }

  it should "compute the missing value count correctly" in {
    val agg = new NumericColumnStatisticsAggregator
    agg.iterate(Option(5d))
    agg.missingCount shouldBe 0l
    agg.iterate(Option.empty)
    agg.missingCount shouldBe 1l
    agg.iterate(Option(2d))
    agg.missingCount shouldBe 1l
  }

  it should "compute the correct sums" in {
    val agg = new NumericColumnStatisticsAggregator
    agg.iterate(Option(5d))
    agg.iterate(Option.empty)
    agg.sum shouldBe 5d +- epsilon
    agg.iterate(Option(1d))
    agg.sum shouldBe 6d +- epsilon
  }

  it should "compute the min correctly" in {
    val agg = new NumericColumnStatisticsAggregator
    agg.iterate(Option.empty)
    agg.min shouldBe Double.PositiveInfinity
    agg.iterate(Option(5d))
    agg.min shouldBe 5d
    agg.iterate(Option(2d))
    agg.min shouldBe 2d
    agg.iterate(Option(10d))
    agg.min shouldBe 2d
  }

  it should "compute the max correctly" in {
    val agg = new NumericColumnStatisticsAggregator
    agg.iterate(Option.empty)
    agg.max shouldBe Double.NegativeInfinity
    agg.iterate(Option(5d))
    agg.max shouldBe 5d
    agg.iterate(Option(2d))
    agg.max shouldBe 5d
    agg.iterate(Option(10d))
    agg.max shouldBe 10d
  }

  it should "compute the mean correctly" in {
    val agg = new NumericColumnStatisticsAggregator
    agg.iterate(Option.empty)
    agg.mean.isNaN shouldBe true
    agg.iterate(Option(5d))
    agg.mean shouldBe 5d +- epsilon
    agg.iterate(Option(0d))
    agg.mean shouldBe 2.5 +- epsilon
    agg.iterate(Option(10))
    agg.mean shouldBe 5d +- epsilon
    agg.iterate(Option.empty)
    agg.mean shouldBe 5d +- epsilon
  }

  it should "compute the variance correctly" in {
    val agg = new NumericColumnStatisticsAggregator
    agg.iterate(Option.empty)
    agg.variance.isNaN shouldBe true
    agg.iterate(Option(5d))
    agg.variance.isNaN shouldBe true
    agg.iterate(Option(0d))
    agg.variance shouldBe 6.25 +- epsilon
    agg.iterate(Option(10))
    agg.variance shouldBe 16.6666667 +- epsilon
    agg.iterate(Option.empty)
    agg.variance shouldBe 16.6666667 +- epsilon
  }

  it should "compute the stdev correctly" in {
    val agg = new NumericColumnStatisticsAggregator
    agg.iterate(Option.empty)
    agg.stdev.isNaN shouldBe true
    agg.iterate(Option(5d))
    agg.stdev.isNaN shouldBe true
    agg.iterate(Option(0d))
    agg.stdev shouldBe 2.5 +- epsilon
    agg.iterate(Option.empty)
    agg.stdev shouldBe 2.5 +- epsilon
  }

  it should "merge two total counts correctly" in {
    val agg1 = new NumericColumnStatisticsAggregator
    agg1.iterate(Option(1))
    val agg2 = new NumericColumnStatisticsAggregator
    agg2.iterate(Option(2))
    agg1.merge(agg2).totalCount shouldBe 2l
  }

  it should "merge two missing value counts correctly" in {
    val agg1 = new NumericColumnStatisticsAggregator
    agg1.iterate(Option(1))
    agg1.iterate(Option.empty)
    val agg2 = new NumericColumnStatisticsAggregator
    agg2.iterate(Option.empty)
    agg1.merge(agg2).missingCount shouldBe 2l
  }

  it should "merge two sums correctly" in {
    val agg1 = new NumericColumnStatisticsAggregator
    agg1.iterate(Option(1))
    val agg2 = new NumericColumnStatisticsAggregator
    agg2.iterate(Option(2))
    agg1.merge(agg2).sum shouldBe 3d +- epsilon
  }

  it should "merge two mins correctly" in {
    val agg1 = new NumericColumnStatisticsAggregator
    agg1.iterate(Option(1))
    val agg2 = new NumericColumnStatisticsAggregator
    agg2.iterate(Option(2))
    agg1.merge(agg2).min shouldBe 1d
  }

  it should "merge two maxs correctly" in {
    val agg1 = new NumericColumnStatisticsAggregator
    agg1.iterate(Option(1))
    val agg2 = new NumericColumnStatisticsAggregator
    agg2.iterate(Option(2))
    agg1.merge(agg2).max shouldBe 2d
  }

  it should "merge two means correctly" in {
    val agg1 = new NumericColumnStatisticsAggregator
    agg1.iterate(Option(1))
    val agg2 = new NumericColumnStatisticsAggregator
    agg2.iterate(Option(2))
    agg1.merge(agg2).mean shouldBe 1.5d +- epsilon
  }

  it should "merge two variances correctly" in {
    val agg1 = new NumericColumnStatisticsAggregator
    agg1.iterate(Option(5))
    agg1.iterate(Option(0))
    val agg2 = new NumericColumnStatisticsAggregator
    agg2.iterate(Option(10))
    agg1.merge(agg2).variance shouldBe 16.6666667 +- epsilon
  }

  it should "merge two standard deviations correctly" in {
    val agg1 = new NumericColumnStatisticsAggregator
    agg1.iterate(Option(5))
    val agg2 = new NumericColumnStatisticsAggregator
    agg2.iterate(Option(0))
    agg1.merge(agg2).stdev shouldBe 2.5 +- epsilon
  }

}
