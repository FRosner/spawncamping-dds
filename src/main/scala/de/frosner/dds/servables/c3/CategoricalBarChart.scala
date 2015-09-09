package de.frosner.dds.servables.c3

case class CategoricalBarChart[N](labels: Seq[String],
                        values: Seq[Seq[N]],
                        categories: Seq[String],
                        title: String)
                       (implicit num: Numeric[N])
  extends CategoricalChart(
    data = SeriesData(
      series = labels.zip(values).map{ case (label, values) => Series(label, values) },
      ChartTypes.multiple(ChartTypeEnum.Bar, labels.size)
    ),
    categories = categories,
    title = title
  ) {

  val servableType: String = "bar (categorical)"

}
