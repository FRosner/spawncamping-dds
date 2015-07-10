package de.frosner.dds.servables.histogram

import org.scalatest.{FlatSpec, Matchers}
import spray.json.{JsArray, JsNumber, JsObject, JsString}

class HistogramTest extends FlatSpec with Matchers {

  "The preconditions of a histogram" should "not allow more bins than frequencies + 1" in {
    intercept[IllegalArgumentException] {
      Histogram(List(0.0, 0.2, 0.3), List(0))
    }
  }

  it should "not allow less bins than frequencies + 1" in {
    intercept[IllegalArgumentException] {
      Histogram(List(0.3), List(0))
    }
  }

  "A histogram" should "have the correct JSON format" in {
    val histogram = Histogram(List(0.0, 1.0, 2.0), List(2, 5)).toJson
    histogram.fields("type") shouldBe JsString("histogram")
    histogram.fields("content") shouldBe JsArray(
      JsObject(
        ("start", JsNumber(0.0)),
        ("end", JsNumber(1.0)),
        ("y", JsNumber(2.0))
      ), JsObject(
        ("start", JsNumber(1.0)),
        ("end", JsNumber(2.0)),
        ("y", JsNumber(5.0))
      )
    )
  }

  "An optimal number of bins" should "be computed correctly" in {
    val testValues = List(0,1,3,5,8,12,16)
    val expectedValues = List(1,1,3,4,4,5,5)
    testValues.map(Histogram.optimalNumberOfBins(_)) shouldBe expectedValues
  }

}
