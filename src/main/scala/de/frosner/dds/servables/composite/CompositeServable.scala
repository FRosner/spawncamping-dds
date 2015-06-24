package de.frosner.dds.servables.composite

import de.frosner.dds.core.Servable
import spray.json.{JsArray, JsValue}

case class CompositeServable(servables: Seq[Seq[Servable]], title: String = Servable.DEFAULT_TITLE) extends Servable {

  val servableType = "composite"

  def contentAsJson: JsValue = JsArray(
    servables.map(row => JsArray(
      row.map(_.toJson).toVector
    )).toVector
  )

}
