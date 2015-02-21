package de.frosner.dds.chart

import de.frosner.dds.core.Servable
import org.apache.spark.util.StatCounter
import spray.json.{JsArray, JsNumber, JsObject, JsValue}

case class Stats(stats: Seq[StatCounter]) extends Servable {

  val servableType = "stats"

  def contentAsJson: JsArray = JsArray(stats.map(stat => {
    JsObject(
      ("count", JsNumber(stat.count)),
      ("sum", JsNumber(stat.sum)),
      ("min", JsNumber(stat.sum)),
      ("max", JsNumber(stat.sum)),
      ("mean", JsNumber(stat.sum)),
      ("stdev", JsNumber(stat.sum)),
      ("variance", JsNumber(stat.sum))
    )
  }).toVector)

}

object Stats {

  def apply(stat: StatCounter): Stats = Stats(List(stat))

}
