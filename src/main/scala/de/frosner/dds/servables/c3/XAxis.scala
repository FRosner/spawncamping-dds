package de.frosner.dds.servables.c3

import de.frosner.dds.servables.c3.AxisTypeEnum._
import spray.json.{JsArray, JsString, JsObject, JsValue}

trait XAxis {

  val axisType: AxisType
  def toJson: JsValue = JsObject(
    configJson.+(("type", JsString(axisType.toString)))
  )
  private[servables] val configJson: Map[String, JsValue]

}

object XAxis {

  private class IndexedXAxis extends XAxis {
    override val axisType: AxisType = AxisTypeEnum.Indexed
    override private[servables] val configJson: Map[String, JsValue] = Map.empty
  }

  val indexed: XAxis = new IndexedXAxis()

  private case class CategoricalXAxis(categories: Seq[String]) extends XAxis {
    override val axisType: AxisType = AxisTypeEnum.Categorical
    override private[servables] val configJson: Map[String, JsValue] =
      Map(("categories", JsArray(categories.map(c => JsString(c)).toVector)))
  }

  def categorical(categories: Seq[String]): XAxis = CategoricalXAxis(categories)

}
