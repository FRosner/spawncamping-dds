package de.frosner.dds.servables.scatter

import de.frosner.dds.core.Servable
import de.frosner.dds.servables.tabular.OrderedMap
import spray.json.{JsNumber, JsObject, JsArray}

case class Points2D(points: Seq[(Double, Double)]) extends Servable {

  override val servableType: String = "points-2d"

  def contentAsJson = JsArray(
    points.map{ case (x, y) => JsObject(OrderedMap(List(
      ("x", JsNumber(x)),
      ("y", JsNumber(y))
    )))}.toVector
  )

}
