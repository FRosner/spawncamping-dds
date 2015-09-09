package de.frosner.dds.servables.c3

case class LineChart[N](labels: Seq[String],
                        values: Seq[Seq[N]],
                        title: String)
                       (implicit num: Numeric[N])
  extends IndexedChart(
    data = SeriesData(
      series = labels.zip(values).map{ case (label, values) => Series(label, values) },
      ChartTypes.multiple(ChartTypeEnum.Line, labels.size)
    ),
    title = title
  ) {

  val servableType: String = "line"

}
