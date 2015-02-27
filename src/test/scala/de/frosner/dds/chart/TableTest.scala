package de.frosner.dds.chart

import org.apache.spark.util.StatCounter
import org.scalatest.{Matchers, FlatSpec}
import spray.json._

class TableTest extends FlatSpec with Matchers {

  "A table" should "have the correct JSON format when constructed from single stat counter" in {
    val statCounter = StatCounter(1D, 2D, 3D)
    val stats = Table.fromStatCounter(statCounter)
    stats.contentAsJson shouldBe JsArray(JsObject(OrderedMap[String, JsValue](List(
      ("label", JsString("data")),
      ("count", JsNumber(statCounter.count)),
      ("sum", JsNumber(statCounter.sum)),
      ("min", JsNumber(statCounter.min)),
      ("max", JsNumber(statCounter.max)),
      ("mean", JsNumber(statCounter.mean)),
      ("stdev", JsNumber(statCounter.stdev)),
      ("variance", JsNumber(statCounter.variance))
    ))))
  }

  it should "have the correct JSON format when constructed from multiple stat counters" in {
    val statCounter1 = StatCounter(1D, 2D, 3D)
    val statCounter2 = StatCounter(0D, 5D)
    val stats = Table.fromStatCounters(List("label1", "label2"), List(statCounter1, statCounter2))
    stats.contentAsJson shouldBe JsArray(
      JsObject(
        ("label", JsString("label1")),
        ("count", JsNumber(statCounter1.count)),
        ("sum", JsNumber(statCounter1.sum)),
        ("min", JsNumber(statCounter1.min)),
        ("max", JsNumber(statCounter1.max)),
        ("mean", JsNumber(statCounter1.mean)),
        ("stdev", JsNumber(statCounter1.stdev)),
        ("variance", JsNumber(statCounter1.variance))
      ),
      JsObject(
        ("label", JsString("label2")),
        ("count", JsNumber(statCounter2.count)),
        ("sum", JsNumber(statCounter2.sum)),
        ("min", JsNumber(statCounter2.min)),
        ("max", JsNumber(statCounter2.max)),
        ("mean", JsNumber(statCounter2.mean)),
        ("stdev", JsNumber(statCounter2.stdev)),
        ("variance", JsNumber(statCounter2.variance))
      )
    )
  }

  it should "have the correct JSON format when constructed directly" in {
    Table(List("a", "b"), List(List("va1", "vb1"), List("va2", "vb2"))).contentAsJson shouldBe(
      JsArray(
        JsObject(
          ("a", JsString("va1")),
          ("b", JsString("vb1"))
        ),
        JsObject(
          ("a", JsString("va2")),
          ("b", JsString("vb2"))
        )
      )
      )
  }

}
