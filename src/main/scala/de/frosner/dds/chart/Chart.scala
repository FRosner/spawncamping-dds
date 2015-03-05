package de.frosner.dds.chart

import de.frosner.dds.core.Servable
import spray.json._

/**
 * Class representing a <a href="http://c3js.org/reference.html">C3 chart</a>. It contains the id of the DOM object
 * to be drawn into as well as the data to draw.
 *
 * @param data to plot in the chart
 */
case class Chart(data: Data) extends Servable {

  val servableType = "chart"

  val bindTo = "#" + Chart.id

  def contentAsJson: JsValue = JsObject(
    ("bindto", JsString(bindTo)),
    ("data", data.toJson)
  )

}

object Chart{

  val id = "chart"

}
