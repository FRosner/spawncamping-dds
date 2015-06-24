package de.frosner.dds.core

import org.scalatest.{FlatSpec, Matchers}
import spray.json.{JsObject, JsString, JsValue}

class ServableTest extends FlatSpec with Matchers {

  "A servable" should "have the correct JSON format" in {
    val servable = new Servable {
      override protected def contentAsJson: JsValue = JsString("test-content")
      override val servableType: String = "test-type"
      override val title: String = "test-title"
    }
    servable.toJson shouldBe JsObject(
      ("type", JsString("test-type")),
      ("title", JsString("test-title")),
      ("content", JsString("test-content"))
    )
  }

}
