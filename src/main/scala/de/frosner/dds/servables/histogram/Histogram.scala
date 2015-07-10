package de.frosner.dds.servables.histogram

import de.frosner.dds.core.Servable
import spray.json.{JsArray, JsNumber, JsObject, JsValue}

case class Histogram(bins: Seq[Double], frequencies: Seq[Long], title: String = Servable.DEFAULT_TITLE) extends Servable {

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

object Histogram {

  private def lg2IntCeil(number: Long): Int = {
    //integer lg_2 can be calculated by bitshifting
    var log = 0
    var countDown = number
    while (countDown > 1) {
      countDown = countDown >> 1
      log += 1
    }
    //ceiling by checking whether each LSB was zero, adding 1 if not.
    if (number == (1 << log) || number == 0)
      log
    else
      log + 1
  }
  
  def optimalNumberOfBins(count: Long): Int = lg2IntCeil(count) + 1

}
