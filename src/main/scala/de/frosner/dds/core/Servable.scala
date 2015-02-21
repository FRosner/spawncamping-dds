package de.frosner.dds.core

import spray.json.{JsString, JsValue, JsObject}

trait Servable {

  val servableType: String

  def toJson: JsObject = JsObject(
    ("type", JsString(servableType)),
    ("content", contentAsJson)
  )

  protected def contentAsJson: JsValue

  def toJsonString: String = toJson.prettyPrint

}
