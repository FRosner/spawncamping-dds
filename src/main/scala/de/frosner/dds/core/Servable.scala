package de.frosner.dds.core

import spray.json.{JsObject, JsString, JsValue}

/**
 * Object that can be served to a [[Server]]. It needs to have a content that can be transformed to JSON and a type
 * that can be understood by the front-end JavaScript code to select what to do with the content.
 */
trait Servable {

  /**
   * Type of the [[Servable]]. This is used by the front-end to decide how to display the [[Servable]].
   */
  val servableType: String

  /**
   * Title of the [[Servable]]. This is shown in the front-end.
   */
  val title: String

  /**
   * @return JSON representation of this [[Servable]]. It contains the type as well as the content.
   */
  def toJson: JsObject = JsObject(
    ("type", JsString(servableType)),
    ("title", JsString(title)),
    ("content", contentAsJson)
  )

  /**
   * @return Content of the [[Servable]] that will be used by the front-end to display it.
   *         This contains the actual data.
   */
  protected def contentAsJson: JsValue

  /**
   * @return Stringified JSON representation to put into the HTTP response.
   */
  def toJsonString: String = toJson.prettyPrint

}

object Servable {

  val DEFAULT_TITLE = ""

}
