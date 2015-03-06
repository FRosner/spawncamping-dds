package de.frosner.dds.servables.c3

import spray.json.{JsObject, JsString}

case class DummyData(key: String, value: String) extends Data {
  override def toJson: JsObject = JsObject(("dummy", JsString("value")))
}
