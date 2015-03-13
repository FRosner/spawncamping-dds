package de.frosner.dds.servables.c3

import de.frosner.dds.servables.c3.AxisTypeEnum.AxisType
import org.scalatest.{Matchers, FlatSpec}
import spray.json.{JsString, JsObject, JsValue}

class XAxisTest extends FlatSpec with Matchers {

  "An X-Axis" should "serialize its type correctly" in {
    class CustomXAxis extends XAxis {
      override val axisType: AxisType = AxisTypeEnum.Categorical
      override private[servables] val configJson: Map[String, JsValue] = Map.empty
    }
    new CustomXAxis().toJson shouldBe JsObject(("type", JsString("category")))
  }

}
