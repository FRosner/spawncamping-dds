package de.frosner.dds.analytics

import org.scalatest.{Matchers, FlatSpec}

class CorrelationAggregatorTest extends FlatSpec with Matchers {

  val epsilon = 0.000001

  "A correlation aggregator" should "be initialized properly" in {
    val agg = new CorrelationAggregator(5)
    agg.aggregators.mapValues{ case (agg1, agg2) => (agg1.totalCount, agg2.totalCount) }.toMap shouldBe Map(
      (0, 1) -> (0l, 0l),
      (0, 2) -> (0l, 0l),
      (0, 3) -> (0l, 0l),
      (0, 4) -> (0l, 0l),
      (1, 2) -> (0l, 0l),
      (1, 3) -> (0l, 0l),
      (1, 4) -> (0l, 0l),
      (2, 3) -> (0l, 0l),
      (2, 4) -> (0l, 0l),
      (3, 4) -> (0l, 0l)
    )
    agg.runningCov.toMap shouldBe Map(
      (0, 1) -> 0d,
      (0, 2) -> 0d,
      (0, 3) -> 0d,
      (0, 4) -> 0d,
      (1, 2) -> 0d,
      (1, 3) -> 0d,
      (1, 4) -> 0d,
      (2, 3) -> 0d,
      (2, 4) -> 0d,
      (3, 4) -> 0d
    )
  }

  it should "iterate the numeric aggregators accordingly" in {
    val agg = new CorrelationAggregator(3)
    agg.iterateWithoutNulls(List(1d,2d,3d))
    agg.aggregators.mapValues{ case (agg1, agg2) => (agg1.totalCount, agg2.totalCount) }.toMap shouldBe Map(
      (0, 1) -> (1l, 1l),
      (0, 2) -> (1l, 1l),
      (1, 2) -> (1l, 1l)
    )
    agg.iterate(List(Option(1d),Option(2d),Option.empty))
    agg.aggregators.mapValues{ case (agg1, agg2) => (agg1.totalCount, agg2.totalCount) }.toMap shouldBe Map(
      (0, 1) -> (2l, 2l),
      (0, 2) -> (1l, 1l),
      (1, 2) -> (1l, 1l)
    )
  }

  it should "compute the running covariance accordingly" in {
    val agg = new CorrelationAggregator(3)
    agg.iterateWithoutNulls(List(1d,2d,3d))
    agg.runningCov.toMap shouldBe Map(
      (0, 1) -> 0d,
      (0, 2) -> 0d,
      (1, 2) -> 0d
    )
    agg.iterate(List(Option(2d),Option.empty,Option(4d)))
    agg.runningCov.toMap shouldBe Map(
      (0, 1) -> 0d,
      (0, 2) -> 0.25,
      (1, 2) -> 0d
    )
  }

  it should "compute the running covariance for negative values" in {
    val agg = new CorrelationAggregator(2)
    agg.iterateWithoutNulls(List(10d,1d))
    agg.iterateWithoutNulls(List(-5d,2d))
    agg.iterateWithoutNulls(List(1d,-10d))
    agg.iterateWithoutNulls(List(80d,20d))
    agg.iterateWithoutNulls(List(3d,5d))
    val result = agg.runningCov.toMap
    result((0,1)) should be (256.91999999999996 +- epsilon)
  }

  it should "merge two numerical aggregators correctly" in {
    val agg1 = new CorrelationAggregator(3)
    agg1.iterateWithoutNulls(List(1d,2d,3d))
    val agg2 = new CorrelationAggregator(3)
    agg2.iterate(List(Option(1d),Option(2d),Option.empty))
    agg1.merge(agg2).aggregators.mapValues{ case (agg1, agg2) => (agg1.totalCount, agg2.totalCount) }.toMap shouldBe Map(
      (0, 1) -> (2l, 2l),
      (0, 2) -> (1l, 1l),
      (1, 2) -> (1l, 1l)
    )
  }

  it should "merge two covariances correctly" in {
    val agg1 = new CorrelationAggregator(3)
    agg1.iterateWithoutNulls(List(1d,2d,3d))
    val agg2 = new CorrelationAggregator(3)
    agg2.iterate(List(Option(2d),Option.empty,Option(4d)))
    agg1.merge(agg2).runningCov.toMap shouldBe Map(
      (0, 1) -> 0d,
      (0, 2) -> 0.25,
      (1, 2) -> 0d
    )
  }

  it should "merge more than two covariances correctly" in {
    val agg1 = new CorrelationAggregator(3)
    agg1.iterateWithoutNulls(List(5d, 7d, 1d))
    agg1.iterateWithoutNulls(List(3d, 2d, 8d))
    val agg2 = new CorrelationAggregator(3)
    agg2.iterateWithoutNulls(List(2d, 6d, -5d))
    agg2.iterate(List(Option.empty, Option.empty, Option.empty))
    val result = agg1.merge(agg2).runningCov
    result((0, 1)) should be (1d +- epsilon)
    result((0, 2)) should be (1.8888888888 +- epsilon)
    result((1, 2)) should be (-9d +- epsilon)
  }

  it should "merge more than two covariances correctly when the left one is empty" in {
    val agg1 = new CorrelationAggregator(3)
    val agg2 = new CorrelationAggregator(3)
    agg2.iterateWithoutNulls(List(5d, 7d, 1d))
    agg2.iterateWithoutNulls(List(3d, 2d, 8d))
    agg2.iterateWithoutNulls(List(2d, 6d, -5d))
    agg2.iterate(List(Option.empty, Option.empty, Option.empty))
    val result = agg1.merge(agg2).runningCov
    result((0, 1)) should be (1d +- epsilon)
    result((0, 2)) should be (1.8888888888 +- epsilon)
    result((1, 2)) should be (-9d +- epsilon)
  }

  it should "merge more than two covariances correctly when the right one is empty" in {
    val agg1 = new CorrelationAggregator(3)
    agg1.iterateWithoutNulls(List(5d, 7d, 1d))
    agg1.iterateWithoutNulls(List(3d, 2d, 8d))
    agg1.iterateWithoutNulls(List(2d, 6d, -5d))
    agg1.iterate(List(Option.empty, Option.empty, Option.empty))
    val agg2 = new CorrelationAggregator(3)
    val result = agg1.merge(agg2).runningCov
    result((0, 1)) should be (1d +- epsilon)
    result((0, 2)) should be (1.8888888888 +- epsilon)
    result((1, 2)) should be (-9d +- epsilon)
  }

  it should "require the number of columns in an iteration its initial size" in {
    val agg = new CorrelationAggregator(2)
    intercept[IllegalArgumentException] {
      agg.iterateWithoutNulls(List(1d))
    }
  }

  it should "require the intermediate aggregator to have the same size when merging" in {
    val agg = new CorrelationAggregator(2)
    val intermediateAgg = new CorrelationAggregator(3)
    intercept[IllegalArgumentException] {
      agg.merge(intermediateAgg)
    }
  }

  "A correlation matrix built from a correlation aggregator" should "have 1 in the diagonal" in {
    val agg = new CorrelationAggregator(5)
    agg.iterateWithoutNulls(List(1d, 2d, 3d, 4d, 5d))
    agg.iterateWithoutNulls(List(5d, 3d, 2d, 1d, 4d))
    val result = agg.pearsonCorrelations
    result((0,0)) shouldBe 1d
    result((1,1)) shouldBe 1d
    result((2,2)) shouldBe 1d
    result((3,3)) shouldBe 1d
    result((4,4)) shouldBe 1d
  }

  it should "be symmetric" in {
    val agg = new CorrelationAggregator(5)
    agg.iterateWithoutNulls(List(1d, 2d, 3d, 4d, 5d))
    agg.iterateWithoutNulls(List(5d, 3d, 2d, 1d, 4d))
    val result = agg.pearsonCorrelations
    for (i <- 0 to 4; j <- 0 to 4) {
      result((i, j)) shouldBe result((j, i))
    }
  }

  it should "have correlation of 1 for a perfect correlated set of columns" in {
    val agg = new CorrelationAggregator(3)
    agg.iterateWithoutNulls(List(1d, 2d, 3d))
    agg.iterateWithoutNulls(List(4d, 5d, 6d))
    agg.iterateWithoutNulls(List(7d, 8d, 9d))
    val result = agg.pearsonCorrelations
    result((0, 1)) should be (1d +- epsilon)
    result((0, 2)) should be (1d +- epsilon)
    result((1, 2)) should be (1d +- epsilon)
  }

  it should "have correlation of -1 for a perfect negatively correlated set of columns" in {
    val agg = new CorrelationAggregator(3)
    agg.iterateWithoutNulls(List(1d, 8d, 3d))
    agg.iterateWithoutNulls(List(4d, 5d, 6d))
    agg.iterateWithoutNulls(List(7d, 2d, 9d))
    val result = agg.pearsonCorrelations
    result((0, 1)) should be (-1d +- epsilon)
    result((0, 2)) should be (1d +- epsilon)
    result((1, 2)) should be (-1d +- epsilon)
  }

  it should "give the same correlation values as R for a normal data set" in {
    val agg = new CorrelationAggregator(3)
    agg.iterateWithoutNulls(List(5d, 7d, 1d))
    agg.iterateWithoutNulls(List(3d, 2d, 8d))
    agg.iterateWithoutNulls(List(2d, 6d, -5d))
    val result = agg.pearsonCorrelations
    result((0, 1)) should be (0.3711537 +- epsilon)
    result((0, 2)) should be (0.2850809 +- epsilon)
    result((1, 2)) should be (-0.7842301 +- epsilon)
  }

  it should "give the same correlation values as R when covariances have been merged" in {
    val agg1 = new CorrelationAggregator(3)
    agg1.iterateWithoutNulls(List(5d, 7d, 1d))
    agg1.iterateWithoutNulls(List(3d, 2d, 8d))
    val agg2 = new CorrelationAggregator(3)
    agg2.iterateWithoutNulls(List(2d, 6d, -5d))
    agg2.iterate(List(Option.empty, Option.empty, Option.empty))
    val result = agg1.merge(agg2).pearsonCorrelations
    result((0, 1)) should be (0.3711537 +- epsilon)
    result((0, 2)) should be (0.2850809 +- epsilon)
    result((1, 2)) should be (-0.7842301 +- epsilon)
  }

  it should "give the same correlation values as R for a data set with null values" in {
    val agg = new CorrelationAggregator(3)
    agg.iterateWithoutNulls(List(5d, 7d, 1d))
    agg.iterate(List(Option(3d), Option.empty, Option(8d)))
    agg.iterateWithoutNulls(List(2d, 6d, -5d))
    val result = agg.pearsonCorrelations
    result((0, 1)) should be (1d +- epsilon)
    result((0, 2)) should be (0.2850809 +- epsilon)
    result((1, 2)) should be (1d +- epsilon)
  }

}
