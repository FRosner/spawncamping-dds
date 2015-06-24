package de.frosner.dds.servables.scatter

import de.frosner.dds.core.Servable
import de.frosner.dds.servables.tabular.{Table, OrderedMap}
import spray.json.{JsString, JsNumber, JsObject, JsArray}

case class Points2D[XT, YT](points: Seq[(XT, YT)], title: String = Servable.DEFAULT_TITLE)
                           (implicit numX: Numeric[XT] = null, numY: Numeric[YT] = null) extends Servable {

  override val servableType: String = "points-2d"

  def contentAsJson = JsObject(
    ("points", JsArray(
        points.map{ case (x, y) => JsObject(OrderedMap(List(
          ("x", if (numX != null) JsNumber(numX.toDouble(x)) else JsString(x.toString)),
          ("y", if (numY != null) JsNumber(numY.toDouble(y)) else JsString(y.toString))
        )))}.toVector
    )),
    ("types", JsObject(
      ("x", JsString(if (numX != null) Table.NUMERIC_TYPE else Table.DISCRETE_TYPE)),
      ("y", JsString(if (numY != null) Table.NUMERIC_TYPE else Table.DISCRETE_TYPE))
    ))
  )

}
