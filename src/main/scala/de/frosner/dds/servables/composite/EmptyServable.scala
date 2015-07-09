package de.frosner.dds.servables.composite

import de.frosner.dds.core.Servable
import spray.json.{JsObject, JsValue}

object EmptyServable {

  lazy val instance: Servable = new Servable {
    val title = Servable.DEFAULT_TITLE
    val servableType = "empty"
    def contentAsJson: JsValue = JsObject()
  }

}
