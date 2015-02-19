package de.frosner.dds.chart

object ChartTypeEnum extends Enumeration {

  type ChartType = Value

  val Line = Value("line")
  val Spline = Value("spline")
  val Step = Value("step")
  val Area = Value("area")
  val AreaSpline = Value("area-spline")
  val AreaStep = Value("area-step")
  val Bar = Value("bar")
  val Scatter = Value("scatter")
  val Pie = Value("pie")
  val Donut = Value("donut")
  val Gauge = Value("gauge")

}