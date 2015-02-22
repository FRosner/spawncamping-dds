package de.frosner.dds.chart

import de.frosner.dds.core.Servable
import de.frosner.dds.util.StringResource
import org.apache.spark.util.StatCounter
import spray.json._

case class Stats(labels: Seq[String], stats: Seq[StatCounter]) extends Servable {

  val servableType = "stats"

  def contentAsJson: JsArray = {
    val statsAndLabels = labels.zip(stats)
    JsArray(statsAndLabels.map{ case (label, stat) => {
      JsObject(
        ("label", JsString(label)),
        ("count", JsNumber(stat.count)),
        ("sum", JsNumber(stat.sum)),
        ("min", JsNumber(stat.min)),
        ("max", JsNumber(stat.max)),
        ("mean", JsNumber(stat.mean)),
        ("stdev", JsNumber(stat.stdev)),
        ("variance", JsNumber(stat.variance))
      )
    }}.toVector)
  }

}

object Stats {

  def apply(stat: StatCounter): Stats = Stats(List("data"), List(stat))

  lazy val css = StringResource.read("/css/stats.css")

}
