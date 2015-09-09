package de.frosner.dds.servables.c3

import de.frosner.dds.core.Servable
import spray.json._

/**
 * Class representing a <a href="http://c3js.org/reference.html">C3 chart</a>. It contains the id of the DOM object
 * to be drawn into as well as the data to draw.
 *
 * @param data to plot in the chart
 */
abstract class Chart(data: Data, xAxis: XAxis, title: String) extends Servable {

  def contentAsJson: JsValue = JsObject(
    ("data", data.toJson),
    ("axis", JsObject(("x", xAxis.toJson)))
  )

}
