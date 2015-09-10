package de.frosner.dds.servables.c3

import org.scalatest.{FlatSpec, Matchers}
import spray.json.{JsObject, JsString}

class ChartTest extends FlatSpec with Matchers {

  private case class DummyChart(data: Data, xAxis: XAxis, title: String) extends Chart(data, xAxis, title) {
    val servableType = "dummy-chart"
  }

  "A chart" should "have the correct JSON format with default x-axis" in {
    val data = new DummyData("data", "1")
    val axis = XAxis.indexed
    DummyChart(data, axis, "myTitle").contentAsJson shouldBe JsObject(
      ("data", data.toJson),
      ("axis", JsObject(
        ("x", axis.toJson)
      ))
    )
  }

}
