package de.frosner.dds.chart

import de.frosner.dds.core.Servable
import spray.json._

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
