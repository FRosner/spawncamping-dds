package de.frosner.dds.chart

import spray.json.JsObject

trait Data {

  def toJson: JsObject

}
