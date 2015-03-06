package de.frosner.dds.servables.c3

import de.frosner.dds.servables.c3.ChartTypeEnum.ChartType
import spray.json.{JsObject, JsString}

/**
 * Collection of [[ChartType]]s with a corresponding JSON representation.
 *
 * @param types
 */
case class ChartTypes(types: Iterable[ChartType]) {

  /**
   * Convert the chart types to a JSON object where each type is associated to the given label. Both iterables are
   * zipped and then the labels are treated as keys for the corresponding [[ChartType]].
   *
   * @param labels of the [[ChartType]]s
   * @return JSON object containing a mapping between labels and [[ChartType]]s
   */
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
