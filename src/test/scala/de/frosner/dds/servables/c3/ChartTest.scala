package de.frosner.dds.servables.c3

import org.scalatest.{FlatSpec, Matchers}
import spray.json.{JsObject, JsString}

class ChartTest extends FlatSpec with Matchers {

  "A chart" should "have the correct JSON format with default x-axis" in {
    val data = new DummyData("data", "1")
    Chart(data).contentAsJson shouldBe JsObject(
      ("data", data.toJson),
      ("axis", JsObject(
        ("x", XAxis.indexed.toJson)
      ))
    )
  }

  it should "include a custom axis type in the JSON format" in {
    val data = new DummyData("data", "1")
    val categoricalAxis = XAxis.categorical(List("a", "b"))
    Chart(data, categoricalAxis).contentAsJson.asJsObject.fields("axis") shouldBe
      JsObject(("x", categoricalAxis.toJson))
  }

}
