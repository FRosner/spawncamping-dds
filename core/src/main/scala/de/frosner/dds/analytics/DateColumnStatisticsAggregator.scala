package de.frosner.dds.analytics

import java.sql.Timestamp
import java.util.Calendar

import scalaz._
import Scalaz._

import scala.collection.mutable

import DateColumnStatisticsAggregator._

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
    } else {
      runningYearFrequencies.update(NULL_YEAR, runningYearFrequencies.getOrElse(NULL_YEAR, 0l) + 1l)
      runningMonthFrequencies.update(NULL_MONTH, runningMonthFrequencies.getOrElse(NULL_MONTH, 0l) + 1l)
      runningDayOfWeekFrequencies.update(NULL_DAY, runningDayOfWeekFrequencies.getOrElse(NULL_DAY, 0l) + 1l)
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

object DateColumnStatisticsAggregator {

  val NULL_YEAR = Integer.MAX_VALUE

  val NULL_MONTH = Integer.MAX_VALUE

  val NULL_DAY = Integer.MAX_VALUE

  def calendarYearToString(year: Int) = year match {
    case DateColumnStatisticsAggregator.NULL_YEAR => "NULL"
    case normalYear => normalYear.toString
  }

  def calendarMonthToString(month: Int) = month match {
    case Calendar.JANUARY => "Jan"
    case Calendar.FEBRUARY => "Feb"
    case Calendar.MARCH => "Mar"
    case Calendar.APRIL => "Apr"
    case Calendar.MAY => "May"
    case Calendar.JUNE => "Jun"
    case Calendar.JULY => "Jul"
    case Calendar.AUGUST => "Aug"
    case Calendar.SEPTEMBER => "Sep"
    case Calendar.OCTOBER => "Oct"
    case Calendar.NOVEMBER => "Nov"
    case Calendar.DECEMBER => "Dec"
    case DateColumnStatisticsAggregator.NULL_MONTH => "NULL"
  }

  def calendarDayToString(day: Int) = day match {
    case Calendar.MONDAY => "Mon"
    case Calendar.TUESDAY => "Tue"
    case Calendar.WEDNESDAY => "Wed"
    case Calendar.THURSDAY => "Thu"
    case Calendar.FRIDAY => "Fri"
    case Calendar.SATURDAY => "Sat"
    case Calendar.SUNDAY => "Sun"
    case DateColumnStatisticsAggregator.NULL_DAY => "NULL"
  }

}
