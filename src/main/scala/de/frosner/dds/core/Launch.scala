package de.frosner.dds.core

import de.frosner.dds.chart.{Series, SeriesData, Chart}

object Launch extends App {

  DDS.start()

  val values = List(1,2,3)
  val series = Series("data1", values)
  val data = SeriesData(series)
  DDS.plot(Chart(data))

}
