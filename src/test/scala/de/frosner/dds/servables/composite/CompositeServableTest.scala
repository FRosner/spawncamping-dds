package de.frosner.dds.servables.composite

import de.frosner.dds.core.Servable
import org.scalatest.{Matchers, FlatSpec}
import spray.json._

class CompositeServableTest extends FlatSpec with Matchers {

  "A composite servable" should "have the correct JSON format" in {
    CompositeServable(List(
      List(DummyServable(1), DummyServable(2), DummyServable(3)),
      List(DummyServable(4))
    )).toJson shouldBe JsObject(
      ("type", JsString("composite")),
      ("title", JsString("")),
      ("content", JsArray(Vector(
        JsArray(Vector(DummyServable(1).toJson, DummyServable(2).toJson, DummyServable(3).toJson)),
        JsArray(Vector(DummyServable(4).toJson))
      )))
    )
  }

}

case class DummyServable(number: Int) extends Servable {

  val servableType = "dummy"

  val title = "dummy"

  def contentAsJson: JsValue = JsNumber(number)

}
