package de.frosner.dds.chart

import spray.json.{JsString, JsNumber, JsArray}

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
