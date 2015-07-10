package de.frosner.dds.analytics

import java.sql.Timestamp
import java.util.Calendar

import org.scalatest.{FlatSpec, Matchers}

class DateColumnStatisticsAggregatorTest extends FlatSpec with Matchers {

  "A date column statistics aggregator" should "be initialed properly" in {
    val agg = new DateColumnStatisticsAggregator
    agg.totalCount shouldBe 0l
    agg.missingCount shouldBe 0l
    agg.nonMissingCount shouldBe 0l
    agg.yearFrequencies shouldBe Map.empty
    agg.monthFrequencies shouldBe Map.empty
    agg.dayOfWeekFrequencies shouldBe Map.empty
  }

  it should "compute the correct total count" in {
    val agg = new DateColumnStatisticsAggregator
    agg.iterate(Option(new Timestamp(5)))
    agg.totalCount shouldBe 1l
    agg.iterate(Option(new Timestamp(10)))
    agg.totalCount shouldBe 2l
    agg.iterate(Option.empty)
    agg.totalCount shouldBe 3l
  }

  it should "compute the correct missing count" in {
    val agg = new DateColumnStatisticsAggregator
    agg.iterate(Option(new Timestamp(5)))
    agg.missingCount shouldBe 0l
    agg.iterate(Option(new Timestamp(10)))
    agg.missingCount shouldBe 0l
    agg.iterate(Option.empty)
    agg.missingCount shouldBe 1l
  }

  it should "compute the correct non missing count" in {
    val agg = new DateColumnStatisticsAggregator
    agg.iterate(Option(new Timestamp(5)))
    agg.nonMissingCount shouldBe 1l
    agg.iterate(Option(new Timestamp(10)))
    agg.nonMissingCount shouldBe 2l
    agg.iterate(Option.empty)
    agg.nonMissingCount shouldBe 2l
  }

  it should "compute the correct year frequencies" in {
    val agg = new DateColumnStatisticsAggregator
    val calendar = Calendar.getInstance

    calendar.set(2000, Calendar.JUNE, 10)
    agg.iterate(Option(new Timestamp(calendar.getTimeInMillis)))
    agg.yearFrequencies shouldBe Map(2000 -> 1l)

    calendar.set(1950, Calendar.NOVEMBER, 10)
    agg.iterate(Option(new Timestamp(calendar.getTimeInMillis)))
    agg.yearFrequencies shouldBe Map(2000 -> 1l, 1950 -> 1l)

    calendar.set(2000, Calendar.JANUARY, 1)
    agg.iterate(Option(new Timestamp(calendar.getTimeInMillis)))
    agg.yearFrequencies shouldBe Map(2000 -> 2l, 1950 -> 1l)

    agg.iterate(Option.empty)
    agg.yearFrequencies shouldBe Map(2000 -> 2l, 1950 -> 1l, DateColumnStatisticsAggregator.NULL_YEAR -> 1l)
  }

  it should "compute the correct month frequencies" in {
    val agg = new DateColumnStatisticsAggregator
    val calendar = Calendar.getInstance

    calendar.set(2000, Calendar.JUNE, 10)
    agg.iterate(Option(new Timestamp(calendar.getTimeInMillis)))
    agg.monthFrequencies shouldBe Map(Calendar.JUNE -> 1l)

    calendar.set(1950, Calendar.NOVEMBER, 10)
    agg.iterate(Option(new Timestamp(calendar.getTimeInMillis)))
    agg.monthFrequencies shouldBe Map(Calendar.NOVEMBER -> 1l, Calendar.JUNE -> 1l)

    calendar.set(2000, Calendar.NOVEMBER, 1)
    agg.iterate(Option(new Timestamp(calendar.getTimeInMillis)))
    agg.monthFrequencies shouldBe Map(Calendar.NOVEMBER -> 2l, Calendar.JUNE -> 1l)

    agg.iterate(Option.empty)
    agg.monthFrequencies shouldBe Map(Calendar.NOVEMBER -> 2l, Calendar.JUNE -> 1l, DateColumnStatisticsAggregator.NULL_MONTH -> 1l)

  }

  it should "compute the correct day of week frequencies" in {
    val agg = new DateColumnStatisticsAggregator
    val calendar = Calendar.getInstance

    calendar.set(2015, Calendar.JULY, 4)
    agg.iterate(Option(new Timestamp(calendar.getTimeInMillis)))
    agg.dayOfWeekFrequencies shouldBe Map(Calendar.SATURDAY -> 1l)

    calendar.set(2015, Calendar.JULY, 5)
    agg.iterate(Option(new Timestamp(calendar.getTimeInMillis)))
    agg.dayOfWeekFrequencies shouldBe Map(Calendar.SATURDAY -> 1l, Calendar.SUNDAY -> 1l)

    calendar.set(2015, Calendar.JULY, 12)
    agg.iterate(Option(new Timestamp(calendar.getTimeInMillis)))
    agg.dayOfWeekFrequencies shouldBe Map(Calendar.SATURDAY -> 1l, Calendar.SUNDAY -> 2l)

    agg.iterate(Option.empty)
    agg.dayOfWeekFrequencies shouldBe Map(Calendar.SATURDAY -> 1l, Calendar.SUNDAY -> 2l, DateColumnStatisticsAggregator.NULL_DAY -> 1l)
  }

  it should "compute the correct top year" in {
    val agg = new DateColumnStatisticsAggregator
    val calendar = Calendar.getInstance

    calendar.set(2000, Calendar.JUNE, 10)
    agg.iterate(Option(new Timestamp(calendar.getTimeInMillis)))
    agg.topYear shouldBe (2000, 1l)

    calendar.set(1950, Calendar.NOVEMBER, 10)
    agg.iterate(Option(new Timestamp(calendar.getTimeInMillis)))

    calendar.set(2000, Calendar.JANUARY, 1)
    agg.iterate(Option(new Timestamp(calendar.getTimeInMillis)))
    agg.topYear shouldBe (2000, 2l)

    agg.iterate(Option.empty)
    agg.topYear shouldBe (2000, 2l)
  }

  it should "compute the correct top month" in {
    val agg = new DateColumnStatisticsAggregator
    val calendar = Calendar.getInstance

    calendar.set(2000, Calendar.JUNE, 10)
    agg.iterate(Option(new Timestamp(calendar.getTimeInMillis)))
    agg.topMonth shouldBe (Calendar.JUNE, 1l)

    calendar.set(1950, Calendar.NOVEMBER, 10)
    agg.iterate(Option(new Timestamp(calendar.getTimeInMillis)))

    calendar.set(2000, Calendar.NOVEMBER, 1)
    agg.iterate(Option(new Timestamp(calendar.getTimeInMillis)))
    agg.topMonth shouldBe (Calendar.NOVEMBER, 2l)

    agg.iterate(Option.empty)
    agg.topMonth shouldBe (Calendar.NOVEMBER, 2l)
  }

  it should "compute the correct top day of week" in {
    val agg = new DateColumnStatisticsAggregator
    val calendar = Calendar.getInstance

    calendar.set(2015, Calendar.JULY, 4)
    agg.iterate(Option(new Timestamp(calendar.getTimeInMillis)))
    agg.topDayOfWeek shouldBe (Calendar.SATURDAY, 1l)

    calendar.set(2015, Calendar.JULY, 5)
    agg.iterate(Option(new Timestamp(calendar.getTimeInMillis)))

    calendar.set(2015, Calendar.JULY, 12)
    agg.iterate(Option(new Timestamp(calendar.getTimeInMillis)))
    agg.topDayOfWeek shouldBe (Calendar.SUNDAY, 2l)

    agg.iterate(Option.empty)
    agg.topDayOfWeek shouldBe (Calendar.SUNDAY, 2l)
  }

  it should "merge the correct total count correctly" in {
    val agg1 = new DateColumnStatisticsAggregator
    agg1.iterate(Option(new Timestamp(5)))
    agg1.iterate(Option(new Timestamp(10)))
    val agg2 = new DateColumnStatisticsAggregator
    agg2.iterate(Option.empty)
    agg1.merge(agg2).totalCount shouldBe 3l
  }

  it should "merge the correct missing count correctly" in {
    val agg1 = new DateColumnStatisticsAggregator
    agg1.iterate(Option(new Timestamp(5)))
    agg1.iterate(Option(new Timestamp(10)))
    val agg2 = new DateColumnStatisticsAggregator
    agg2.iterate(Option.empty)
    agg1.merge(agg2).missingCount shouldBe 1l
  }

  it should "merge the correct non missing count correctly" in {
    val agg1 = new DateColumnStatisticsAggregator
    agg1.iterate(Option(new Timestamp(5)))
    agg1.iterate(Option(new Timestamp(10)))
    val agg2 = new DateColumnStatisticsAggregator
    agg2.iterate(Option.empty)
    agg1.merge(agg2).nonMissingCount shouldBe 2l
  }

  it should "merge the correct year frequencies correctly" in {
    val calendar = Calendar.getInstance

    val agg1 = new DateColumnStatisticsAggregator
    calendar.set(2000, Calendar.JUNE, 10)
    agg1.iterate(Option(new Timestamp(calendar.getTimeInMillis)))
    calendar.set(1950, Calendar.NOVEMBER, 10)
    agg1.iterate(Option(new Timestamp(calendar.getTimeInMillis)))

    val agg2 = new DateColumnStatisticsAggregator
    calendar.set(2000, Calendar.JANUARY, 1)
    agg2.iterate(Option(new Timestamp(calendar.getTimeInMillis)))

    agg1.merge(agg2).yearFrequencies shouldBe Map(2000 -> 2l, 1950 -> 1l)
  }

  it should "merge the correct month frequencies correctly" in {
    val calendar = Calendar.getInstance

    val agg1 = new DateColumnStatisticsAggregator
    calendar.set(2000, Calendar.JUNE, 10)
    agg1.iterate(Option(new Timestamp(calendar.getTimeInMillis)))
    calendar.set(1950, Calendar.NOVEMBER, 10)
    agg1.iterate(Option(new Timestamp(calendar.getTimeInMillis)))

    val agg2 = new DateColumnStatisticsAggregator
    calendar.set(2000, Calendar.NOVEMBER, 1)
    agg2.iterate(Option(new Timestamp(calendar.getTimeInMillis)))

    agg1.merge(agg2).monthFrequencies shouldBe Map(Calendar.NOVEMBER -> 2l, Calendar.JUNE -> 1l)
  }

  it should "merge the correct day of week frequencies correctly" in {
    val calendar = Calendar.getInstance

    val agg1 = new DateColumnStatisticsAggregator
    calendar.set(2015, Calendar.JULY, 4)
    agg1.iterate(Option(new Timestamp(calendar.getTimeInMillis)))
    calendar.set(2015, Calendar.JULY, 5)
    agg1.iterate(Option(new Timestamp(calendar.getTimeInMillis)))

    val agg2 = new DateColumnStatisticsAggregator
    calendar.set(2015, Calendar.JULY, 12)
    agg2.iterate(Option(new Timestamp(calendar.getTimeInMillis)))

    agg1.merge(agg2).dayOfWeekFrequencies shouldBe Map(Calendar.SATURDAY -> 1l, Calendar.SUNDAY -> 2l)
  }

}
