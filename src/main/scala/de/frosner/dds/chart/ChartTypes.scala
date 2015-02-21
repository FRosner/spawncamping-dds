package de.frosner.dds.chart

import de.frosner.dds.chart.ChartTypeEnum.ChartType
import spray.json.{JsObject, JsString}

case class ChartTypes(types: Iterable[ChartType]) {
  
  def toJsonWithLabels(labels: Iterable[String]): JsObject = {
    require(labels.size == types.size)
    val labelTypePairs = labels.zip(types.map(t => JsString(t.toString)))
    JsObject(labelTypePairs.toMap)
  }
  
}

object ChartTypes {

  def apply(singleType: ChartType): ChartTypes = ChartTypes(List(singleType))

  def multiple(multipleType: ChartType, amount: Int) = ChartTypes((1 to amount).map(x => multipleType).toList)

}
