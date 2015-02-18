package de.frosner.dds.chart

import spray.json._

case class Chart(data: Data) {

  val bindTo = "#" + Chart.id

  def toJsonString: String = this.toJson.prettyPrint

  def toJson: JsValue = JsObject(
    ("bindto", JsString(bindTo)),
    ("data", data.toJson)
  )

}

object Chart{

  val id = "chart"

}
