package de.frosner.dds.servables.c3

import de.frosner.dds.core.Servable
import spray.json._

/**
 * Class representing a <a href="http://c3js.org/reference.html">C3 chart</a>. It contains the id of the DOM object
 * to be drawn into as well as the data to draw.
 *
 * @param data to plot in the chart
 */
case class Chart(data: Data, xAxis: XAxis = XAxis.indexed, title: String = Servable.DEFAULT_TITLE) extends Servable {

  val servableType = "chart"

  def contentAsJson: JsValue = JsObject(
    ("data", data.toJson),
    ("axis", JsObject(("x", xAxis.toJson)))
  )

}
