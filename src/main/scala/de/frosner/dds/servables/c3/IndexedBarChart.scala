package de.frosner.dds.servables.c3

case class IndexedBarChart[N](labels: Seq[String],
                              values: Seq[Seq[N]],
                              title: String)
                             (implicit num: Numeric[N])
  extends IndexedChart(
    data = SeriesData(
      series = labels.zip(values).map{ case (label, values) => Series(label, values) },
      ChartTypes.multiple(ChartTypeEnum.Bar, labels.size)
    ),
    title = title
  ) {

  val servableType: String = "bar (indexed)"

}
