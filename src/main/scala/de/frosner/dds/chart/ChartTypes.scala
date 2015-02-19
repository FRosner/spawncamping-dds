package de.frosner.dds.chart

import de.frosner.dds.chart.ChartTypeEnum.ChartType
import spray.json.{JsString, JsObject, JsValue}

case class ChartTypes(types: Iterable[ChartType]) {
  
  def toJsonWithLabels(labels: Iterable[String]): JsObject = {
    require(labels.size == types.size)
    val labelTypePairs = labels.zip(types.map(t => JsString(t.toString)))
    JsObject(labelTypePairs.toMap)
  }
  
}

object ChartTypes {

  def apply[T](singleType: ChartType) = new ChartTypes(List(singleType))

}
