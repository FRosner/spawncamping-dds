package de.frosner.dds.servables.histogram

import de.frosner.dds.core.Servable
import spray.json.{JsArray, JsNumber, JsObject, JsValue}

case class Histogram(bins: Seq[Double], frequencies: Seq[Long], title: String = "") extends Servable {

  require(bins.size == frequencies.size + 1)

  val servableType = "histogram"

  override protected def contentAsJson: JsValue = {
    val jsReadyBins = bins.sliding(2).filter(_.size == 2).toList.zip(frequencies)
    val jsBins = jsReadyBins.map{ case (Seq(start, end), frequency) =>
       JsObject(
         ("start", JsNumber(start)),
         ("end", JsNumber(end)),
         ("y", JsNumber(frequency))
       )
     }
    JsArray(jsBins.toVector)
  }

}
