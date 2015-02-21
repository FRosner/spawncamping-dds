package de.frosner.dds.chart

import org.apache.spark.util.StatCounter
import org.scalatest.{Matchers, FlatSpec}
import spray.json.{JsNumber, JsObject, JsArray}

class StatsTest extends FlatSpec with Matchers {

  "A stats object" should "have the correct JSON format when constructed from single stat counter" in {
    val statCounter = StatCounter(1D, 2D, 3D)
    val stats = Stats(statCounter)
    stats.contentAsJson shouldBe JsArray(JsObject(
      ("count", JsNumber(statCounter.count)),
      ("sum", JsNumber(statCounter.sum)),
      ("min", JsNumber(statCounter.sum)),
      ("max", JsNumber(statCounter.sum)),
      ("mean", JsNumber(statCounter.sum)),
      ("stdev", JsNumber(statCounter.sum)),
      ("variance", JsNumber(statCounter.sum))
    ))
  }

  it should "have the correct JSON format when constructed from multiple stat counters" in {
    val statCounter1 = StatCounter(1D, 2D, 3D)
    val statCounter2 = StatCounter(0D, 5D)
    val stats = Stats(List(statCounter1, statCounter2))
    stats.contentAsJson shouldBe JsArray(
      JsObject(
        ("count", JsNumber(statCounter1.count)),
        ("sum", JsNumber(statCounter1.sum)),
        ("min", JsNumber(statCounter1.sum)),
        ("max", JsNumber(statCounter1.sum)),
        ("mean", JsNumber(statCounter1.sum)),
        ("stdev", JsNumber(statCounter1.sum)),
        ("variance", JsNumber(statCounter1.sum))
      ),
      JsObject(
        ("count", JsNumber(statCounter2.count)),
        ("sum", JsNumber(statCounter2.sum)),
        ("min", JsNumber(statCounter2.sum)),
        ("max", JsNumber(statCounter2.sum)),
        ("mean", JsNumber(statCounter2.sum)),
        ("stdev", JsNumber(statCounter2.sum)),
        ("variance", JsNumber(statCounter2.sum))
      )
    )
  }

}
