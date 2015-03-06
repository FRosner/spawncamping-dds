package de.frosner.dds.servables.c3

import org.scalatest.{FlatSpec, Matchers}
import spray.json.{JsObject, JsString}

class ChartTypesTest extends FlatSpec with Matchers {

  "Chart types" should "have correct JSON format when constructed from a single chart type" in {
    val chartType = ChartTypeEnum.Line
    ChartTypes(chartType).toJsonWithLabels(List("label1")) shouldBe JsObject(("label1", JsString(chartType.toString)))
  }

  it should "have correct JSON format when constructed from multiple chart types" in {
    val chartType1 = ChartTypeEnum.Bar
    val chartType2 = ChartTypeEnum.Area
    ChartTypes(List(chartType1, chartType2)).toJsonWithLabels(List("label1", "label2")) shouldBe
      JsObject(("label1", JsString(chartType1.toString)), ("label2", JsString(chartType2.toString)))
  }

  it should "throw an exception when more labels are passed than there are types" in {
    val chartType = ChartTypeEnum.Bar
    intercept[IllegalArgumentException] {
      ChartTypes(chartType).toJsonWithLabels(List("label1", "label2"))
    }
  }

  it should "throw an exception when less labels are passed than there are types" in {
    val chartType = ChartTypeEnum.Bar
    intercept[IllegalArgumentException] {
      ChartTypes(chartType).toJsonWithLabels(List[String]())
    }
  }

}
