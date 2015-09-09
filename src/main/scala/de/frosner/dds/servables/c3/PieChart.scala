package de.frosner.dds.servables.c3

case class PieChart[K, V](keyValuePairs: Iterable[(K, V)], title: String)(implicit num: Numeric[V])
  extends IndexedChart(
    data = SeriesData(
      series = keyValuePairs.map{ case (key, value) => Series(key.toString, List(value))},
      types = ChartTypes.multiple(ChartTypeEnum.Pie, keyValuePairs.size)
    ),
    title = title
  ) {

  val servableType: String = "pie"

}
