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
      ("min", JsNumber(stat.min)),
      ("max", JsNumber(stat.max)),
      ("mean", JsNumber(stat.mean)),
      ("stdev", JsNumber(stat.stdev)),
      ("variance", JsNumber(stat.variance))
    )
  }).toVector)

}

object Stats {

  def apply(stat: StatCounter): Stats = Stats(List(stat))

}
