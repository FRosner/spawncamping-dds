package de.frosner.dds.core

import de.frosner.dds.chart.ChartTypeEnum.ChartType
import de.frosner.dds.chart._
import org.apache.spark.rdd.RDD

object DDS {

  private var chart: Option[Chart] = Option.empty

  private var chartServer: Option[ChartServer] = Option.empty
  private var serverNumber = 1

  private[core] def start(server: ChartServer): Unit = {
    chartServer = Option(server)
    serverNumber += 1
    chartServer.map(_.start())
  }

  def start(): Unit = {
    start(SprayChartServer("dds-" + serverNumber))
  }

  def stop() = {
    chartServer.map(_.stop())
    chartServer = Option.empty
  }

  private def seriesPlot[T](series: Seq[Seq[T]], chartTypes: ChartTypes)(implicit num: Numeric[T]): Unit = {
    require(series.size == chartTypes.types.size)
    val chartSeries = series.zip(1 to series.size).map { case (values, idx) => {
      Series("data" + idx, values)
    }}
    val chart = Chart(SeriesData(chartSeries, chartTypes))
    chartServer.map(_.serve(chart))
  }

  private def seriesPlot[T](series: Seq[Seq[T]], chartType: ChartType)(implicit num: Numeric[T]): Unit = {
    seriesPlot(series, ChartTypes((1 to series.size).map(x => chartType).toList))
  }

  def linePlot[T](values: Seq[T], otherValues: Seq[T]*)(implicit num: Numeric[T]): Unit = {
    seriesPlot(values +: otherValues, ChartTypeEnum.Line)
  }
  
  def piePlot[T](values: Seq[T], otherValues: Seq[T]*)(implicit num: Numeric[T]): Unit = {
    seriesPlot(values +: otherValues, ChartTypeEnum.Pie)
  }

  def barPlot[T](values: Seq[T], otherValues: Seq[T]*)(implicit num: Numeric[T]): Unit = {
    seriesPlot(values +: otherValues, ChartTypeEnum.Bar)
  }

  def test(rdd: RDD[Double]) = rdd.count
  
}
