package de.frosner.dds.chart

import de.frosner.dds.chart.ChartTypeEnum.ChartType
import spray.json.{JsNumber, JsString, JsArray, JsObject}

case class SeriesData[T](series: Iterable[Series[T]], types: ChartTypes) extends Data {

  override def toJson: JsObject = {
    JsObject(
      ("columns", JsArray(series.map(_.toJson).toVector)),
      ("types", types.toJsonWithLabels(series.map(_.label)))
    )
  }

}

object SeriesData {
  def apply[T](series: Series[T], chartType: ChartType) = new SeriesData(List(series), ChartTypes(List(chartType)))
}

case class Series[T](label: String, values: Iterable[T])(implicit num: Numeric[T]) {

  def toJson: JsArray = {
    val jsValues = values.map{
      case v: Double => JsNumber(v)
      case v: Int => JsNumber(v)
      case v: Long => JsNumber(v)
      case v: BigDecimal => JsNumber(v)
      case v: BigInt => JsNumber(v)
      case default => throw new IllegalArgumentException(s"Unsupported value type ${default.getClass}")
    }.toVector
    JsArray((Vector.empty :+ JsString(label)) ++ jsValues)
  }

}
