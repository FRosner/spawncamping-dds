package de.frosner.dds.servables.c3

import de.frosner.dds.servables.c3.ChartTypeEnum.ChartType
import spray.json.{JsArray, JsObject}

/**
 * Representation of sequential data points. A [[SeriesData]] object can contain several sequences / series. The type
 * at position x corresponds to the sequence at the same position.
 *
 * @param series of data
 * @param types of charts (how to plot the corresponding series)
 * @tparam N type of the data points
 */
case class SeriesData[N](series: Iterable[Series[N]], types: ChartTypes) extends Data {

  require(series.size == types.types.size)

  override def toJson: JsObject = {
    JsObject(
      ("columns", JsArray(series.map(_.toJson).toVector)),
      ("types", types.toJsonWithLabels(series.map(_.label)))
    )
  }

}

object SeriesData {
  def apply[N](series: Series[N], chartType: ChartType) = new SeriesData(List(series), ChartTypes(List(chartType)))
}

