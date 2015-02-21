package de.frosner.dds.chart

import spray.json.{JsString, JsObject}

case class DummyData(key: String, value: String) extends Data {
  override def toJson: JsObject = JsObject(("dummy", JsString("value")))
}
