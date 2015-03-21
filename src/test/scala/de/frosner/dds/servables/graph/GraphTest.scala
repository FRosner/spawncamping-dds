package de.frosner.dds.servables.graph

import org.scalatest.{Matchers, FlatSpec}
import spray.json.{JsNumber, JsString, JsArray, JsObject}

class GraphTest extends FlatSpec with Matchers {

  "A graph" should "have the correct JSON format" in {
    Graph(List("a", "b"), List((0, 0), (0, 1))).toJson shouldBe JsObject(
      ("type", JsString("graph")),
      ("content", JsObject(
        ("vertices", JsArray(JsString("a"), JsString("b"))),
        ("edges", JsArray(
          JsObject(("source", JsNumber(0)), ("target", JsNumber(0))),
          JsObject(("source", JsNumber(0)), ("target", JsNumber(1)))
        ))
      ))
    )
  }

}
