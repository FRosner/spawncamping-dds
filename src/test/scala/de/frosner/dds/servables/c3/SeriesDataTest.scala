package de.frosner.dds.servables.c3

import org.scalatest._
import spray.json.{JsArray, JsObject}

class SeriesDataTest extends FlatSpec with Matchers {

  "A series data object" should "have the correct JSON format when constructed from a single series" in {
    val series = Series("series1", List(1,2,3))
    val chartType = ChartTypeEnum.Line
    val seriesData = SeriesData(series, chartType)
    val expected = Map(
      ("columns", JsArray(series.toJson)),
      ("types", ChartTypes(chartType).toJsonWithLabels(List("series1")))
    )
    seriesData.toJson shouldBe JsObject(expected)
  }

  it should "have the correct JSON format when constructed from multiple series" in {
    val series1 = Series("series1", List(1,2,3))
    val series2 = Series("series2", List(5,4,3))
    val chartTypes = ChartTypes(List(ChartTypeEnum.Line, ChartTypeEnum.Bar))
    val seriesData = SeriesData(List(series1, series2), chartTypes)
    val expected = Map(
      ("columns", JsArray(series1.toJson, series2.toJson)),
      ("types", chartTypes.toJsonWithLabels(List("series1", "series2")))
    )
    seriesData.toJson shouldBe JsObject(expected)
  }

}
