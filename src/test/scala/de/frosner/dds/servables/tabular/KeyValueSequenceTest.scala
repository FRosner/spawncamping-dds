package de.frosner.dds.servables.tabular

import org.apache.spark.util.StatCounter
import org.scalatest.{FlatSpec, Matchers}
import spray.json._

class KeyValueSequenceTest extends FlatSpec with Matchers {

  "A key value sequence" should "have the correct JSON format when constructed from single stat counter" in {
    val statCounter = StatCounter(1D, 2D, 3D)
    val stats = KeyValueSequence.fromStatCounter(statCounter)

    stats.toJson shouldBe JsObject(
      ("type", JsString("keyValue")),
      ("title", JsString("Summary Statistics")),
      ("content", JsObject(
        ("Count", JsString(statCounter.count.toString)),
        ("Sum", JsString(statCounter.sum.toString)),
        ("Min", JsString(statCounter.min.toString)),
        ("Max", JsString(statCounter.max.toString)),
        ("Mean", JsString(statCounter.mean.toString)),
        ("Stdev", JsString(statCounter.stdev.toString)),
        ("Variance", JsString(statCounter.variance.toString))
      ))
    )
  }

  it should "have the correct JSON format when constructed directly" in {
    val keyValueSequence = KeyValueSequence(List(("a", 1), ("b", 2)), "title")

    keyValueSequence.toJson shouldBe JsObject(
      ("type", JsString("keyValue")),
      ("title", JsString("title")),
      ("content", JsObject(
        ("a", JsString("1")),
        ("b", JsString("2"))
      ))
    )
  }

}
