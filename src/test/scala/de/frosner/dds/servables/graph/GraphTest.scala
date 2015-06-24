package de.frosner.dds.servables.graph

import org.scalatest.{Matchers, FlatSpec}
import spray.json.{JsNumber, JsString, JsArray, JsObject}

class GraphTest extends FlatSpec with Matchers {

  "A graph" should "have the correct JSON format" in {
    Graph(List("a", "b"), List((0, 0, "a-a"), (0, 1, "a-b")), "graph").toJson shouldBe JsObject(
      ("type", JsString("graph")),
      ("title", JsString("graph")),
      ("content", JsObject(
        ("vertices", JsArray(JsObject(("label", JsString("a"))), JsObject(("label", JsString("b"))))),
        ("edges", JsArray(
          JsObject(("source", JsNumber(0)), ("target", JsNumber(0)), ("label", JsString("a-a"))),
          JsObject(("source", JsNumber(0)), ("target", JsNumber(1)), ("label", JsString("a-b")))
        ))
      ))
    )
  }

}
