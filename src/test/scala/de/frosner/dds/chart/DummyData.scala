package de.frosner.dds.chart

import spray.json.{JsString, JsObject}

class DummyData extends Data {
  override def toJson: JsObject = JsObject(("dummy", JsString("value")))
}
