package de.frosner.dds.servables.c3

import spray.json.{JsArray, JsNumber, JsString}

/**
 * Class representing a single series of numeric data points and a corresponding label.
 *
 * @param label of the series
 * @param values of the series (numeric)
 * @param num implicit conversion from the values to numeric values
 * @tparam N type of the values
 */
case class Series[N](label: String, values: Iterable[N])(implicit num: Numeric[N]) {

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
