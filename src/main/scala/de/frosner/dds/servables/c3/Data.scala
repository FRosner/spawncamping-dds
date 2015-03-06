package de.frosner.dds.servables.c3

import spray.json.JsObject

/**
 * Trait representing data objects for a [[Chart]].
 */
trait Data {

  def toJson: JsObject

}
