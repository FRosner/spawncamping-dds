package de.frosner.dds.servables.tabular

import de.frosner.dds.core.Servable
import org.apache.spark.util.StatCounter
import spray.json._

case class KeyValueSequence(keyValueSequence: Seq[(Any, Any)], title: String = Servable.DEFAULT_TITLE) extends Servable {

  val servableType = "keyValue"

  override protected def contentAsJson: JsValue = JsObject(OrderedMap(keyValueSequence.map{
    case (key, value) => (key.toString, JsString(value.toString))
  }))

}

object KeyValueSequence {

  def fromStatCounter(stats: StatCounter, title: String = "Summary Statistics") = {
    KeyValueSequence(
      List(
        ("Count", stats.count),
        ("Sum", stats.sum),
        ("Min", stats.min),
        ("Max", stats.max),
        ("Mean", stats.mean),
        ("Stdev", stats.stdev),
        ("Variance", stats.variance)
      ), title
    )
  }

}
