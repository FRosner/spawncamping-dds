package de.frosner.dds.servables.c3

import org.scalatest.{FlatSpec, Matchers}
import spray.json.{JsObject, JsString}

class ChartTest extends FlatSpec with Matchers {

  "A chart" should "have the correct JSON format with default x-axis" in {
    val data = new DummyData("data", "1")
    Chart(data).contentAsJson shouldBe JsObject(
      ("bindto", JsString("#" + Chart.id)),
      ("data", data.toJson),
      ("axis", JsObject(
        ("x", XAxis.indexed.toJson)
      ))
    )
  }

}
