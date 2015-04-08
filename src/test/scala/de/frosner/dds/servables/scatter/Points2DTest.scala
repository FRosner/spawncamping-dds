package de.frosner.dds.servables.scatter

import de.frosner.dds.servables.tabular.Table
import org.scalatest.{Matchers, FlatSpec}
import spray.json.{JsString, JsObject, JsNumber, JsArray}

class Points2DTest extends FlatSpec with Matchers {

  "A 2D points object" should "have the correct JSON format when both axis are numeric" in {
    Points2D(List((1.0, 2.0), (5.0, 3.0))).contentAsJson shouldBe JsObject(
      ("points", JsArray(
        JsObject(("x", JsNumber(1)), ("y", JsNumber(2))),
        JsObject(("x", JsNumber(5)), ("y", JsNumber(3)))
      )),
      ("types", JsObject(
        ("x", JsString(Table.NUMERIC_TYPE)),
        ("y", JsString(Table.NUMERIC_TYPE))
      ))
    )
  }

  it should "have the correct JSON format when x axis is not numeric" in {
    Points2D(List(("a", 2.0), ("b", 3.0))).contentAsJson shouldBe JsObject(
      ("points", JsArray(
        JsObject(("x", JsString("a")), ("y", JsNumber(2))),
        JsObject(("x", JsString("b")), ("y", JsNumber(3)))
      )),
      ("types", JsObject(
        ("x", JsString(Table.DISCRETE_TYPE)),
        ("y", JsString(Table.NUMERIC_TYPE))
      ))
    )
  }

  it should "have the correct JSON format when y axis is not numeric" in {
    Points2D(List((2.0, "a"), (3.0, "b"))).contentAsJson shouldBe JsObject(
      ("points", JsArray(
        JsObject(("x", JsNumber(2)), ("y", JsString("a"))),
        JsObject(("x", JsNumber(3)), ("y", JsString("b")))
      )),
      ("types", JsObject(
        ("x", JsString(Table.NUMERIC_TYPE)),
        ("y", JsString(Table.DISCRETE_TYPE))
      ))
    )
  }

  it should "have the correct JSON format when both axes are not numeric" in {
    Points2D(List(("a", "c"), ("b", "d"))).contentAsJson shouldBe JsObject(
      ("points", JsArray(
        JsObject(("x", JsString("a")), ("y", JsString("c"))),
        JsObject(("x", JsString("b")), ("y", JsString("d")))
      )),
      ("types", JsObject(
        ("x", JsString(Table.DISCRETE_TYPE)),
        ("y", JsString(Table.DISCRETE_TYPE))
      ))
    )
  }

  it should "use toString on to build the categorical JS values" in {
    case class Test(value: String)
    Points2D(List((Test("a"), 2))).contentAsJson shouldBe JsObject(
      ("points", JsArray(
        JsObject(("x", JsString("Test(a)")), ("y", JsNumber(2)))
      )),
      ("types", JsObject(
        ("x", JsString(Table.DISCRETE_TYPE)),
        ("y", JsString(Table.NUMERIC_TYPE))
      ))
    )
  }

}
