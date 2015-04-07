package de.frosner.dds.servables.scatter

import org.scalatest.{Matchers, FlatSpec}
import spray.json.{JsObject, JsNumber, JsArray}

class Points2DTest extends FlatSpec with Matchers {

  "A points object" should "have the correct JSON format" in {
    Points2D(List((1.0, 2.0), (5.0, 3.0))).contentAsJson shouldBe JsArray(
      JsObject(("x", JsNumber(1)), ("y", JsNumber(2))),
      JsObject(("x", JsNumber(5)), ("y", JsNumber(3)))
    )
  }

}
