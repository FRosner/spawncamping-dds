package de.frosner.dds.analytics

import java.sql.Timestamp
import java.util.Calendar

import scalaz._
import Scalaz._

import scala.collection.mutable

class DateColumnStatisticsAggregator extends Serializable {

  private var counts: NominalColumnStatisticsAggregator = new NominalColumnStatisticsAggregator()
  private var runningYearFrequencies: mutable.Map[Int, Long] = mutable.HashMap.empty
  private var runningMonthFrequencies: mutable.Map[Int, Long] = mutable.HashMap.empty
  private var runningDayOfWeekFrequencies: mutable.Map[Int, Long] = mutable.HashMap.empty

  def iterate(value: Option[Timestamp]): DateColumnStatisticsAggregator = {
    counts = counts.iterate(value)
    if (value.isDefined) {
      val calendar = Calendar.getInstance()
      calendar.setTime(value.get)
      val year = calendar.get(Calendar.YEAR)
      val month = calendar.get(Calendar.MONTH)
      val day = calendar.get(Calendar.DAY_OF_WEEK)
      runningYearFrequencies.update(year, runningYearFrequencies.getOrElse(year, 0l) + 1l)
      runningMonthFrequencies.update(month, runningMonthFrequencies.getOrElse(month, 0l) + 1l)
      runningDayOfWeekFrequencies.update(day, runningDayOfWeekFrequencies.getOrElse(day, 0l) + 1l)
    }
    this
  }

  def merge(that: DateColumnStatisticsAggregator): DateColumnStatisticsAggregator = {
    counts = counts.merge(that.counts)
    runningYearFrequencies = mutable.HashMap.empty ++
      (runningYearFrequencies.toMap |+| that.runningYearFrequencies.toMap)
    runningMonthFrequencies = mutable.HashMap.empty ++
      (runningMonthFrequencies.toMap |+| that.runningMonthFrequencies.toMap)
    runningDayOfWeekFrequencies = mutable.HashMap.empty ++
      (runningDayOfWeekFrequencies.toMap |+| that.runningDayOfWeekFrequencies.toMap)
    this
  }

  def totalCount = counts.totalCount

  def missingCount = counts.missingCount

  def nonMissingCount = counts.nonMissingCount

  def yearFrequencies = runningYearFrequencies.toMap

  def monthFrequencies = runningMonthFrequencies.toMap

  def dayOfWeekFrequencies = runningDayOfWeekFrequencies.toMap

  def topYear = yearFrequencies.maxBy{ case (year, freq) => freq }

  def topMonth = monthFrequencies.maxBy{ case (month, freq) => freq }

  def topDayOfWeek = dayOfWeekFrequencies.maxBy{ case (day, freq) => freq }

}
