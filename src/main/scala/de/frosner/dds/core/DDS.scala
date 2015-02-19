package de.frosner.dds.core

import de.frosner.dds.chart.ChartTypeEnum.ChartType
import de.frosner.dds.chart.{Series, SeriesData, Chart, ChartTypeEnum}

object DDS {

  var chart: Option[Chart] = Option.empty
  
  def start() = {
    ChartServer.start();
  }

  private def seriesPlot[T](o: Iterable[T], chartType: ChartType)(implicit num: Numeric[T]) = {
    val series = Series("data",o)
    val chart = Chart(SeriesData(series, chartType))
    ChartServer.serve(chart)
  }

  def linePlot[T](o: Iterable[T])(implicit num: Numeric[T]) = {
    seriesPlot(o, ChartTypeEnum.Line)
  }

  def piePlot[T](o: Iterable[T])(implicit num: Numeric[T]) = {
    seriesPlot(o, ChartTypeEnum.Pie)
  }

  def barPlot[T](o: Iterable[T])(implicit num: Numeric[T]) = {
    seriesPlot(o, ChartTypeEnum.Bar)
  }

}
