package de.frosner.dds.chart

import org.scalatest.{Matchers, FlatSpec}
import spray.json.{JsString, JsObject}

class ChartTest extends FlatSpec with Matchers {

  "A chart" should "have the correct JSON format" in {
    val data = new DummyData("data", "1")
    Chart(data).toJson shouldBe JsObject(
      ("bindto", JsString("#" + Chart.id)),
      ("data", data.toJson)
    )
  }

}
