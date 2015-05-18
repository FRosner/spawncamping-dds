package de.frosner.dds.analytics

import org.scalatest.{Matchers, FlatSpec}

class CorrelationAggregatorTest extends FlatSpec with Matchers {

  val epsilon = 0.000001

  "A correlation aggregator" should "be initialized properly" in {
    val agg = new CorrelationAggregator(5)
    val expectedCounts = Map(
      (0, 1) -> 0,
      (0, 2) -> 0,
      (0, 3) -> 0,
      (0, 4) -> 0,
      (1, 2) -> 0,
      (1, 3) -> 0,
      (1, 4) -> 0,
      (2, 3) -> 0,
      (2, 4) -> 0,
      (3, 4) -> 0
    )
    val expectedSumsAndSumsOfSquares = Map(
      (0, 1) -> (0d, 0d),
      (0, 2) -> (0d, 0d),
      (0, 3) -> (0d, 0d),
      (0, 4) -> (0d, 0d),
      (1, 2) -> (0d, 0d),
      (1, 3) -> (0d, 0d),
      (1, 4) -> (0d, 0d),
      (2, 3) -> (0d, 0d),
      (2, 4) -> (0d, 0d),
      (3, 4) -> (0d, 0d)
    )
    val expectedSumsOfPairs = Map(
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
    agg.counts.toMap shouldBe expectedCounts
    agg.sums.toMap shouldBe expectedSumsAndSumsOfSquares
    agg.sumsOfSquares.toMap shouldBe expectedSumsAndSumsOfSquares
    agg.sumsOfPairs.toMap shouldBe expectedSumsOfPairs
  }

  it should "compute the correct sums" in {
    val agg = new CorrelationAggregator(3)
    agg.iterateWithoutNulls(List(1d,2d,3d))
    agg.sums shouldBe Map(
      (0, 1) -> (1d, 2d),
      (0, 2) -> (1d, 3d),
      (1, 2) -> (2d, 3d)
    )
    agg.iterate(List(Option(1d),Option(2d),Option.empty))
    agg.sums shouldBe Map(
      (0, 1) -> (2d, 4d),
      (0, 2) -> (1d, 3d),
      (1, 2) -> (2d, 3d)
    )
  }

  it should "compute the correct sums of squares" in {
    val agg = new CorrelationAggregator(3)
    agg.iterateWithoutNulls(List(1d,2d,3d))
    agg.sumsOfSquares.toMap shouldBe Map(
      (0, 1) -> (1d, 4d),
      (0, 2) -> (1d, 9d),
      (1, 2) -> (4d, 9d)
    )
    agg.iterate(List(Option(1d),Option.empty,Option(3d)))
    agg.sumsOfSquares.toMap shouldBe Map(
      (0, 1) -> (1d, 4d),
      (0, 2) -> (2d, 18d),
      (1, 2) -> (4d, 9d)
    )
  }

  it should "compute the correct count" in {
    val agg = new CorrelationAggregator(3)
    agg.iterateWithoutNulls(List(1d,2d,3d))
    agg.counts shouldBe Map(
      (0, 1) -> 1,
      (0, 2) -> 1,
      (1, 2) -> 1
    )
    agg.iterate(List(Option.empty,Option(2d),Option(3d)))
    agg.counts shouldBe Map(
      (0, 1) -> 1,
      (0, 2) -> 1,
      (1, 2) -> 2
    )
  }

  it should "compute the correct sum of pair for two columns" in {
    val agg = new CorrelationAggregator(2)
    agg.iterateWithoutNulls(List(2d, 3d))
    agg.sumsOfPairs.toMap shouldBe Map((0, 1) -> 6d)
    agg.iterateWithoutNulls(List(1d, 1d))
    agg.sumsOfPairs.toMap shouldBe Map((0, 1) -> 7d)
    agg.iterate(List(Option(1d), Option.empty))
    agg.sumsOfPairs.toMap shouldBe Map((0, 1) -> 7d)
  }

  it should "compute the correct sum of pair for more than two columns" in {
    val agg = new CorrelationAggregator(3)
    agg.iterateWithoutNulls(List(2d, 3d, 4d))
    agg.sumsOfPairs.toMap shouldBe Map((0, 1) -> 6d, (0, 2) -> 8d, (1, 2) -> 12d)
    agg.iterate(List(Option(1d), Option(1d), Option.empty))
    agg.sumsOfPairs.toMap shouldBe Map((0, 1) -> 7d, (0, 2) -> 8d, (1, 2) -> 12d)
  }

  it should "merge two sums correctly" in {
    val agg1 = new CorrelationAggregator(3)
    agg1.iterateWithoutNulls(List(1d,2d,3d))
    val agg2 = new CorrelationAggregator(3)
    agg2.iterateWithoutNulls(List(1d,2d,3d))
    agg1.merge(agg2).sums.toMap shouldBe Map(
      (0, 1) -> (2d, 4d),
      (0, 2) -> (2d, 6d),
      (1, 2) -> (4d, 6d)
    )
  }

  it should "merge two sums of squares correctly" in {
    val agg1 = new CorrelationAggregator(3)
    agg1.iterateWithoutNulls(List(1d,2d,3d))
    val agg2 = new CorrelationAggregator(3)
    agg2.iterateWithoutNulls(List(1d,2d,3d))
    agg1.merge(agg2).sumsOfSquares.toMap shouldBe Map(
      (0, 1) -> (2d, 8d),
      (0, 2) -> (2d, 18d),
      (1, 2) -> (8d, 18d)
    )
  }

  it should "merge two counts correctly" in {
    val agg1 = new CorrelationAggregator(3)
    agg1.iterateWithoutNulls(List(1d,2d,3d))
    val agg2 = new CorrelationAggregator(3)
    agg2.iterateWithoutNulls(List(1d,2d,3d))
    agg1.merge(agg2).counts shouldBe Map(
      (0, 1) -> 2,
      (0, 2) -> 2,
      (1, 2) -> 2
    )
  }

  it should "merge two sums of pairs correctly for two columns" in {
    val agg1 = new CorrelationAggregator(2)
    agg1.iterateWithoutNulls(List(2d, 3d))
    val agg2 = new CorrelationAggregator(2)
    agg2.iterateWithoutNulls(List(1d, 1d))
    agg1.merge(agg2).sumsOfPairs.toMap shouldBe Map((0, 1) -> 7d)
  }

  it should "merge two sums of pairs correctly for more than two columns" in {
    val agg1 = new CorrelationAggregator(3)
    agg1.iterateWithoutNulls(List(2d, 3d, 4d))
    val agg2 = new CorrelationAggregator(3)
    agg2.iterateWithoutNulls(List(1d, 1d, 1d))
    agg1.merge(agg2).sumsOfPairs.toMap shouldBe Map(
      (0, 1) -> 7d,
      (0, 2) -> 9d,
      (1, 2) -> 13d
    )
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

//    [,1] [,2]      [,3]
//  [1,] 1.0000000    1 0.2850809
//    [2,] 1.0000000    1 1.0000000
//    [3,] 0.2850809    1 1.0000000
//  > x
//  [,1] [,2] [,3]
//  [1,]    5    7    1
//    [2,]    3   NA    8
//    [3,]    2    6   -5

}
