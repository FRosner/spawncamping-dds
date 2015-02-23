package de.frosner.dds.core

import de.frosner.dds.chart.ChartTypeEnum.ChartType
import de.frosner.dds.chart._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.StatCounter

import scala.reflect.ClassTag

/**
 * Hacks applied here:
 *
 * - ClassTags are needed for conversion to PairRDD
 *   http://mail-archives.apache.org/mod_mbox/incubator-spark-user/201404.mbox/%3CCANGvG8o-EWeETtYb3VGpmSR9ZvJui8vPO-aLsKj7xTMYQgsPAg@mail.gmail.com%3E
 */
object DDS {

  private val helper = Helper(this.getClass)

  private var chart: Option[Servable] = Option.empty

  private var chartServer: Option[ChartServer] = Option.empty
  private var serverNumber = 1

  private[core] def start(server: ChartServer): Unit = {
    if (chartServer.isDefined) {
      println("Server already started! Type 'help()' to see a list of available commands.")
    } else {
      chartServer = Option(server)
      serverNumber += 1
      chartServer.map(_.start())
    }
  }

  @Help(
    shortDescription = "Starts the DDS Web UI",
    longDescription = "Starts the DDS Web UI bound to the default interface and port."
  )
  def start(): Unit = {
    start(SprayChartServer("dds-" + serverNumber))
  }

  private[core] def resetServer() = {
    chartServer = Option.empty
  }

  def stop() = {
    if (!chartServer.isDefined) {
      println("No server there to stop! Type 'start()' to start one.")
    } else {
      chartServer.map(_.stop())
      resetServer()
    }
  }

  def help() = {
    helper.printMethods(System.out)
  }

  private def seriesPlot[N](series: Iterable[Series[N]], chartTypes: ChartTypes)(implicit num: Numeric[N]): Unit = {
    require(series.size == chartTypes.types.size)
    val chart = Chart(SeriesData(series, chartTypes))
    chartServer.map(_.serve(chart))
  }

  private def seriesPlot[N](series: Iterable[Series[N]], chartType: ChartType)(implicit num: Numeric[N]): Unit = {
    seriesPlot(series, ChartTypes.multiple(chartType, series.size))
  }

  private def pieFromReducedGroups[K, N](reducedGroup: RDD[(K, N)])(implicit num: Numeric[N]): Unit = {
    val groupSeries = reducedGroup.map{
      case (key, summedValues) => Series(key.toString, List(summedValues))
    }.collect
    seriesPlot(groupSeries, ChartTypeEnum.Pie)
  }

  def pieGroups[K, N](groupValues: RDD[(K, Iterable[N])])(implicit num: Numeric[N]): Unit = {
    pieFromReducedGroups(groupValues.map{ case (key, values) => (key, values.sum) })
  }

  def groupAndPie[K: ClassTag, N: ClassTag](toBeGroupedValues: RDD[(K, N)])(implicit num: Numeric[N]): Unit = {
    pieFromReducedGroups(toBeGroupedValues.reduceByKey(num.plus(_, _)))
  }

  private def summarize(stats: Stats) = {
    chartServer.map(_.serve(stats))
  }

  def summarize[N](values: RDD[N])(implicit num: Numeric[N]): Unit = {
    summarize(Stats(values.stats()))
  }
  
  def summarizeGroups[K, N](groupValues: RDD[(K, Iterable[N])])(implicit num: Numeric[N]): Unit = {
    val statCounters = groupValues.map{ case (key, values) =>
      (key, StatCounter(values.map(num.toDouble(_))))
    }.map{ case (key, stat) =>
      (key.toString, stat)
    }.collect
    val (labels, stats) = statCounters.unzip
    summarize(Stats(labels, stats))
  }

  def groupAndSummarize[K: ClassTag, N: ClassTag](toBeGroupedValues: RDD[(K, N)])(implicit num: Numeric[N]): Unit = {
    summarizeGroups(toBeGroupedValues.groupByKey())
  }

  def main(args: Array[String]): Unit = {
    start()
  }

}


