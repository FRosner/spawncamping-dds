package de.frosner.dds.servables.c3

import de.frosner.dds.servables.c3.AxisTypeEnum.AxisType
import org.scalatest.{FlatSpec, Matchers}
import spray.json.{JsArray, JsObject, JsString, JsValue}

class XAxisTest extends FlatSpec with Matchers {

  "An x-axis" should "serialize its type correctly" in {
    class CustomXAxis extends XAxis {
      override val axisType: AxisType = AxisTypeEnum.Categorical
      override private[servables] val configJson: Map[String, JsValue] = Map.empty
    }
    new CustomXAxis().toJson shouldBe JsObject(("type", JsString("category")))
  }

  it should "serialize other properties correctly" in {
    class CustomXAxis extends XAxis {
      override val axisType: AxisType = AxisTypeEnum.Categorical
      override private[servables] val configJson: Map[String, JsValue] = Map(("key", JsString("value")))
    }
    new CustomXAxis().toJson shouldBe JsObject(
      ("type", JsString("category")),
      ("key", JsString("value"))
    )
  }

  "An indexed x-axis" should "have the correct axis type" in {
    XAxis.indexed.axisType shouldBe AxisTypeEnum.Indexed
  }

  it should "have no additional properties" in {
    XAxis.indexed.configJson shouldBe Map.empty
  }


  "A categorical x-axis" should "have the correct axis type" in {
    XAxis.categorical(Seq.empty).axisType shouldBe AxisTypeEnum.Categorical
  }

  it should "have the categories as additional properties" in {
    XAxis.categorical(List("a", "b")).configJson shouldBe
      Map(("categories", JsArray(JsString("a"), JsString("b"))))
  }

}
