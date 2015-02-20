package de.frosner.dds.chart

import org.scalatest.{Matchers, FlatSpec}
import spray.json.{JsNumber, JsString, JsArray}

class SeriesTest extends FlatSpec with Matchers {

  "A series" should "have the correct JSON format" in {
    val series = Series("label", List(1,2,3))
    series.toJson shouldBe JsArray(JsString("label"), JsNumber(1), JsNumber(2), JsNumber(3))
  }

}
