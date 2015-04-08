package de.frosner.dds.servables.matrix

import org.scalatest.{Matchers, FlatSpec}
import spray.json.{JsString, JsNumber, JsArray, JsObject}

class Matrix2DTest extends FlatSpec with Matchers {

  "A 2D matrix" should "have the correct JSON format" in {
    Matrix2D(List(List(1,2), List(3,4)), List("a", "b"), List("c", "d")).contentAsJson shouldBe JsObject(
      ("entries", JsArray(JsArray(JsNumber(1), JsNumber(2)), JsArray(JsNumber(3), JsNumber(4)))),
      ("rowNames", JsArray(JsString("a"), JsString("b"))),
      ("colNames", JsArray(JsString("c"), JsString("d")))
    )
  }

}
