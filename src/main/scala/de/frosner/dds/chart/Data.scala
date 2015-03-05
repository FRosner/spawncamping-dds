package de.frosner.dds.chart

import spray.json.JsObject

/**
 * Trait representing data objects for a [[Chart]].
 */
trait Data {

  def toJson: JsObject

}
