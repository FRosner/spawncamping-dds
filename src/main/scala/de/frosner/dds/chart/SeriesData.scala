package de.frosner.dds.chart

import spray.json.{JsNumber, JsString, JsArray, JsObject}

case class SeriesData[T](series: Iterable[Series[T]]) extends Data {

  override def toJson: JsObject = {
    JsObject(("columns", JsArray(series.map(_.toJson).toVector)))
  }

}

object SeriesData {
  def apply[T](series: Series[T]) = new SeriesData(List(series))
}

case class Series[T](name: String, values: Iterable[T])(implicit num: Numeric[T]) {

  def toJson: JsArray = {
    val jsValues = values.map{
      case v: Double => JsNumber(v)
      case v: Int => JsNumber(v)
      case v: Long => JsNumber(v)
      case v: BigDecimal => JsNumber(v)
      case v: BigInt => JsNumber(v)
      case default => throw new IllegalArgumentException(s"Unsupported value type ${default.getClass}")
    }.toVector
    JsArray((Vector.empty :+ JsString(name)) ++ jsValues)
  }

}
