package de.frosner.dds.analytics

import org.scalatest.{Matchers, FlatSpec}

class NominalColumnStatisticsAggregatorTest extends FlatSpec with Matchers {

  "A nominal column statistics aggregator" should "be initialized correctly" in {
    val agg = new NominalColumnStatisticsAggregator
    agg.totalCount shouldBe 0l
    agg.missingCount shouldBe 0l
    agg.nonMissingCount shouldBe 0l
  }

  it should "compute the total count correctly" in {
    val agg = new NominalColumnStatisticsAggregator
    agg.iterate(Option.empty)
    agg.totalCount shouldBe 1l
    agg.iterate(Option(5))
    agg.totalCount shouldBe 2l
  }

  it should "compute the missing count correctly" in {
    val agg = new NominalColumnStatisticsAggregator
    agg.iterate(Option.empty)
    agg.missingCount shouldBe 1l
    agg.iterate(Option(5))
    agg.missingCount shouldBe 1l
  }

  it should "compute the non-missing count correctly" in {
    val agg = new NominalColumnStatisticsAggregator
    agg.iterate(Option.empty)
    agg.nonMissingCount shouldBe 0l
    agg.iterate(Option(5))
    agg.nonMissingCount shouldBe 1l
  }

  it should "merge two total counts correctly" in {
    val agg1 = new NominalColumnStatisticsAggregator
    agg1.iterate(Option("a"))
    val agg2 = new NominalColumnStatisticsAggregator
    agg2.iterate(Option.empty)
    agg1.merge(agg2).totalCount shouldBe 2l
  }

  it should "merge two missing counts correctly" in {
    val agg1 = new NominalColumnStatisticsAggregator
    agg1.iterate(Option("a"))
    agg1.iterate(Option.empty)
    val agg2 = new NominalColumnStatisticsAggregator
    agg2.iterate(Option.empty)
    agg1.merge(agg2).missingCount shouldBe 2l
  }

  it should "merge two non-missing counts correctly" in {
    val agg1 = new NominalColumnStatisticsAggregator
    agg1.iterate(Option("a"))
    val agg2 = new NominalColumnStatisticsAggregator
    agg2.iterate(Option.empty)
    agg2.iterate(Option(5))
    agg1.merge(agg2).nonMissingCount shouldBe 2l
  }

}
