package de.frosner.dds.analytics

import org.scalatest.{FlatSpec, Matchers}

class MutualInformationAggregatorTest extends FlatSpec with Matchers {

  val epsilon = 0.000001

  "A mutual information aggregator" should "be initialized properly" in {
    val agg = new MutualInformationAggregator(5)
    val expectedColumnCounts = List(
      Map.empty[Any, Int],
      Map.empty[Any, Int],
      Map.empty[Any, Int],
      Map.empty[Any, Int],
      Map.empty[Any, Int]
    )
    val expectedCrossColumnCounts = Map(
      (0, 0) -> Map.empty[(Any, Any), Int],
      (0, 1) -> Map.empty[(Any, Any), Int],
      (0, 2) -> Map.empty[(Any, Any), Int],
      (0, 3) -> Map.empty[(Any, Any), Int],
      (0, 4) -> Map.empty[(Any, Any), Int],
      (1, 1) -> Map.empty[(Any, Any), Int],
      (1, 2) -> Map.empty[(Any, Any), Int],
      (1, 3) -> Map.empty[(Any, Any), Int],
      (1, 4) -> Map.empty[(Any, Any), Int],
      (2, 2) -> Map.empty[(Any, Any), Int],
      (2, 3) -> Map.empty[(Any, Any), Int],
      (2, 4) -> Map.empty[(Any, Any), Int],
      (3, 3) -> Map.empty[(Any, Any), Int],
      (3, 4) -> Map.empty[(Any, Any), Int],
      (4, 4) -> Map.empty[(Any, Any), Int]
    )
    agg.columnCounts.map(_.toMap).toList shouldBe expectedColumnCounts
    agg.crossColumnCounts.map{ case ((key1, key2), value) => ((key1, key2), value.toMap) }.toMap shouldBe
      expectedCrossColumnCounts
  }

  it should "compute the correct column counts" in {
    val agg = new MutualInformationAggregator(3)
    agg.iterate(List(1d,2d,3d))
    agg.columnCounts.map(_.toMap).toList shouldBe List(
      Map[Any, Int](1d -> 1),
      Map[Any, Int](2d -> 1),
      Map[Any, Int](3d -> 1)
    )
    agg.iterate(List(1d, 3d, null))
    agg.columnCounts.map(_.toMap).toList shouldBe List(
      Map[Any, Int](1d -> 2),
      Map[Any, Int](2d -> 1, 3d -> 1),
      Map[Any, Int](3d -> 1, (null, 1))
    )
    agg.iterate(List(1d, 3d, null))
    agg.columnCounts.map(_.toMap).toList shouldBe List(
      Map[Any, Int](1d -> 3),
      Map[Any, Int](2d -> 1, 3d -> 2),
      Map[Any, Int](3d -> 1, (null, 2))
    )
  }

  it should "compute the cross column counts" in {
    val agg = new MutualInformationAggregator(3)
    agg.iterate(List("a","b","c"))
    agg.crossColumnCounts.map{ case ((key1, key2), value) => ((key1, key2), value.toMap) }.toMap shouldBe Map(
      (0, 0) -> Map[(Any, Any), Int](("a", "a") -> 1),
      (0, 1) -> Map[(Any, Any), Int](("a", "b") -> 1),
      (0, 2) -> Map[(Any, Any), Int](("a", "c") -> 1),
      (1, 1) -> Map[(Any, Any), Int](("b", "b") -> 1),
      (1, 2) -> Map[(Any, Any), Int](("b", "c") -> 1),
      (2, 2) -> Map[(Any, Any), Int](("c", "c") -> 1)
    )
    agg.iterate(List("a", "c", null))
    agg.crossColumnCounts.map{ case ((key1, key2), value) => ((key1, key2), value.toMap) }.toMap shouldBe Map(
      (0, 0) -> Map[(Any, Any), Int](("a", "a") -> 2),
      (0, 1) -> Map[(Any, Any), Int](("a", "b") -> 1, ("a", "c") -> 1),
      (0, 2) -> Map[(Any, Any), Int](("a", "c") -> 1, ("a", null) -> 1),
      (1, 1) -> Map[(Any, Any), Int](("b", "b") -> 1, ("c", "c") -> 1),
      (1, 2) -> Map[(Any, Any), Int](("b", "c") -> 1, ("c", null) -> 1),
      (2, 2) -> Map[(Any, Any), Int](("c", "c") -> 1, (null, null) -> 1)
    )
    agg.iterate(List("a","b","c"))
    agg.crossColumnCounts.map{ case ((key1, key2), value) => ((key1, key2), value.toMap) }.toMap shouldBe Map(
      (0, 0) -> Map[(Any, Any), Int](("a", "a") -> 3),
      (0, 1) -> Map[(Any, Any), Int](("a", "b") -> 2, ("a", "c") -> 1),
      (0, 2) -> Map[(Any, Any), Int](("a", "c") -> 2, ("a", null) -> 1),
      (1, 1) -> Map[(Any, Any), Int](("b", "b") -> 2, ("c", "c") -> 1),
      (1, 2) -> Map[(Any, Any), Int](("b", "c") -> 2, ("c", null) -> 1),
      (2, 2) -> Map[(Any, Any), Int](("c", "c") -> 2, (null, null) -> 1)
    )
  }

  it should "merge two column counts correctly" in {
    val agg1 = new MutualInformationAggregator(3)
    agg1.iterate(List("a","b","c"))
    val agg2 = new MutualInformationAggregator(3)
    agg2.iterate(List("a","b","c"))
    agg1.merge(agg2).columnCounts.map(_.toMap).toList shouldBe List(
      Map[Any, Int]("a" -> 2),
      Map[Any, Int]("b" -> 2),
      Map[Any, Int]("c" -> 2)
    )
  }

  it should "merge two cross column counts correctly" in {
    val agg1 = new MutualInformationAggregator(3)
    agg1.iterate(List("a","b","c"))
    val agg2 = new MutualInformationAggregator(3)
    agg2.iterate(List("a","b","c"))
    agg1.merge(agg2).crossColumnCounts.map{ case ((key1, key2), value) => ((key1, key2), value.toMap) }.toMap shouldBe Map(
      (0, 0) -> Map[(Any, Any), Int](("a", "a") -> 2),
      (0, 1) -> Map[(Any, Any), Int](("a", "b") -> 2),
      (0, 2) -> Map[(Any, Any), Int](("a", "c") -> 2),
      (1, 1) -> Map[(Any, Any), Int](("b", "b") -> 2),
      (1, 2) -> Map[(Any, Any), Int](("b", "c") -> 2),
      (2, 2) -> Map[(Any, Any), Int](("c", "c") -> 2)
    )
  }

  it should "require the number of columns in an iteration its initial size" in {
    val agg = new MutualInformationAggregator(2)
    intercept[IllegalArgumentException] {
      agg.iterate(List(1d))
    }
  }

  it should "require the intermediate aggregator to have the same size when merging" in {
    val agg = new MutualInformationAggregator(2)
    val intermediateAgg = new MutualInformationAggregator(3)
    intercept[IllegalArgumentException] {
      agg.merge(intermediateAgg)
    }
  }

  "A mutual information matrix built from a mutual information aggregator" should
    "have the entropy of each column in the diagonal" in {
    val agg = new MutualInformationAggregator(3)
    agg.iterate(List(1d, 2d, 3d))
    agg.iterate(List(1d, 2d, 4d))
    agg.iterate(List(2d, 3d, 5d))
    val result = agg.mutualInformation
    // mi.plugin(rbind(c(0, 2/3, 0), c(0, 0, 1/3), c(0, 0, 0)), unit="log")
    result((0,0)) shouldBe 0.6365141682948128 +- epsilon
    result((1,1)) shouldBe 0.6365141682948128 +- epsilon
    result((2,2)) shouldBe 1.0986122886681098 +- epsilon
  }

  it should "be symmetric" in {
    val agg = new MutualInformationAggregator(5)
    agg.iterate(List(1d, 2d, 3d, 4d, 5d))
    agg.iterate(List(5d, 3d, 2d, 1d, 4d))
    agg.iterate(List(1d, 2d, 3d, 4d, 5d))
    agg.iterate(List(5d, 3d, 2d, 1d, 4d))
    val result = agg.mutualInformation
    for (i <- 0 to 4; j <- 0 to 4) {
      result((i, j)) shouldBe result((j, i))
    }
  }

  it should "give the same mutual information values as my calculator for a normal data set" in {
    val agg = new MutualInformationAggregator(3)
    agg.iterate(List("a", "a", "a"))
    agg.iterate(List("a", "b", "b"))
    agg.iterate(List("b", "b", "c"))
    val result = agg.mutualInformation
    result((0, 0)) should be (0.6365142 +- epsilon)
    result((0, 1)) should be (0.174416 +- epsilon)
    result((0, 2)) should be (0.6365142 +- epsilon)
    result((1, 1)) should be (0.6365142 +- epsilon)
    result((1, 2)) should be (0.6365142 +- epsilon)
    result((2, 2)) should be (1.098612 +- epsilon)
  }

  it should "give the same mutual information values as my calculator for a normal data set after merging" in {
    val agg1 = new MutualInformationAggregator(3)
    agg1.iterate(List("a", "a", "a"))
    agg1.iterate(List("a", "b", "b"))
    val agg2 = new MutualInformationAggregator(3)
    agg2.iterate(List("b", "b", "c"))
    val result = agg1.merge(agg2).mutualInformation
    result((0, 0)) should be (0.6365142 +- epsilon)
    result((0, 1)) should be (0.174416 +- epsilon)
    result((0, 2)) should be (0.6365142 +- epsilon)
    result((1, 1)) should be (0.6365142 +- epsilon)
    result((1, 2)) should be (0.6365142 +- epsilon)
    result((2, 2)) should be (1.098612 +- epsilon)
  }

  it should "give the same mutual information values as my calculator for a data set with null values" in {
    val agg = new MutualInformationAggregator(3)
    agg.iterate(List("a", "a", "a"))
    agg.iterate(List("a", null, null))
    agg.iterate(List(null, null, "c"))
    val result = agg.mutualInformation
    result((0, 0)) should be (0.6365142 +- epsilon)
    result((0, 1)) should be (0.174416 +- epsilon)
    result((0, 2)) should be (0.6365142 +- epsilon)
    result((1, 1)) should be (0.6365142 +- epsilon)
    result((1, 2)) should be (0.6365142 +- epsilon)
    result((2, 2)) should be (1.098612 +- epsilon)
  }

  it should "have a correct metric variant" in {
    val agg = new MutualInformationAggregator(3)
    agg.iterate(List("a", "a", "a"))
    agg.iterate(List("a", null, null))
    agg.iterate(List(null, null, "c"))
    val result = agg.mutualInformationMetric
    result((0, 0)) should be (1d +- epsilon)
    result((0, 1)) should be (0.2740174 +- epsilon)
    result((0, 2)) should be (0.5793803 +- epsilon)
    result((1, 1)) should be (1d +- epsilon)
    result((1, 2)) should be (0.5793803 +- epsilon)
    result((2, 2)) should be (1d +- epsilon)
  }

  it should "contain only 0 if all values are the same" in {
    val agg = new MutualInformationAggregator(2)
    agg.iterate(List("a", "a"))
    agg.iterate(List("a", "a"))
    val result = agg.mutualInformation
    result((0, 0)) should be (0d +- epsilon)
    result((0, 1)) should be (0d +- epsilon)
    result((1, 1)) should be (0d +- epsilon)
  }

}
