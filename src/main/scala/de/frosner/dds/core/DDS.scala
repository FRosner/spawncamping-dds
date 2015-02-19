package de.frosner.dds.core

import de.frosner.dds.chart.{Series, SeriesData, Chart}

object DDS {

  var chart: Option[Chart] = Option.empty
  
  def start() = {
    ChartServer.start();
  }

  def linePlot[T](o: Iterable[T])(implicit num: Numeric[T]) = {
    val series = Series("data",o)
    val chart = Chart(SeriesData(series))
    ChartServer.serve(chart)
  }

}
