package de.frosner.dds.servables.tabular

import org.apache.spark.util.StatCounter
import org.scalatest.{FlatSpec, Matchers}
import spray.json._

class TableTest extends FlatSpec with Matchers {

  "A stat table" should "have the correct JSON format when constructed from single stat counter" in {
    val statCounter = StatCounter(1D, 2D, 3D)
    val stats = Table.fromStatCounter(statCounter)

    val actualTableJson = stats.contentAsJson.asJsObject
    val actualTableTypes = actualTableJson.fields("types")
    val actualTableRows = actualTableJson.fields("rows")
    actualTableTypes shouldBe JsObject(
      ("count", JsString(Table.NUMERIC_TYPE)),
      ("sum", JsString(Table.NUMERIC_TYPE)),
      ("min", JsString(Table.NUMERIC_TYPE)),
      ("max", JsString(Table.NUMERIC_TYPE)),
      ("mean", JsString(Table.NUMERIC_TYPE)),
      ("stdev", JsString(Table.NUMERIC_TYPE)),
      ("variance", JsString(Table.NUMERIC_TYPE))
    )
    actualTableRows shouldBe JsArray(JsObject(OrderedMap[String, JsValue](List(
      ("count", JsNumber(statCounter.count)),
      ("sum", JsNumber(statCounter.sum)),
      ("min", JsNumber(statCounter.min)),
      ("max", JsNumber(statCounter.max)),
      ("mean", JsNumber(statCounter.mean)),
      ("stdev", JsNumber(statCounter.stdev)),
      ("variance", JsNumber(statCounter.variance))
    ))))
  }

  it should "have the correct JSON format when constructed from multiple stat counters" in {
    val statCounter1 = StatCounter(1D, 2D, 3D)
    val statCounter2 = StatCounter(0D, 5D)
    val stats = Table.fromStatCounters(List("label1", "label2"), List(statCounter1, statCounter2))

    val actualTableJson = stats.contentAsJson.asJsObject
    val actualTableTypes = actualTableJson.fields("types")
    val actualTableRows = actualTableJson.fields("rows")
    actualTableTypes shouldBe JsObject(
      ("label", JsString(Table.DISCRETE_TYPE)),
      ("count", JsString(Table.NUMERIC_TYPE)),
      ("sum", JsString(Table.NUMERIC_TYPE)),
      ("min", JsString(Table.NUMERIC_TYPE)),
      ("max", JsString(Table.NUMERIC_TYPE)),
      ("mean", JsString(Table.NUMERIC_TYPE)),
      ("stdev", JsString(Table.NUMERIC_TYPE)),
      ("variance", JsString(Table.NUMERIC_TYPE))
    )
    actualTableRows shouldBe JsArray(
      JsObject(
        ("label", JsString("label1")),
        ("count", JsNumber(statCounter1.count)),
        ("sum", JsNumber(statCounter1.sum)),
        ("min", JsNumber(statCounter1.min)),
        ("max", JsNumber(statCounter1.max)),
        ("mean", JsNumber(statCounter1.mean)),
        ("stdev", JsNumber(statCounter1.stdev)),
        ("variance", JsNumber(statCounter1.variance))
      ),
      JsObject(
        ("label", JsString("label2")),
        ("count", JsNumber(statCounter2.count)),
        ("sum", JsNumber(statCounter2.sum)),
        ("min", JsNumber(statCounter2.min)),
        ("max", JsNumber(statCounter2.max)),
        ("mean", JsNumber(statCounter2.mean)),
        ("stdev", JsNumber(statCounter2.stdev)),
        ("variance", JsNumber(statCounter2.variance))
      )
    )
  }

  "A regular table" should "have the correct JSON format when constructed directly" in {
    val table = Table(List("a", "b"), List(List("va1", "vb1"), List("va2", "vb2")))
    val actualTableJson = table.contentAsJson.asJsObject
    val actualTableTypes = actualTableJson.fields("types")
    val actualTableRows = actualTableJson.fields("rows")
    actualTableTypes shouldBe JsObject(
      ("a", JsString(Table.DISCRETE_TYPE)),
      ("b", JsString(Table.DISCRETE_TYPE))
    )
    actualTableRows shouldBe
      JsArray(
        JsObject(
          ("a", JsString("va1")),
          ("b", JsString("vb1"))
        ),
        JsObject(
          ("a", JsString("va2")),
          ("b", JsString("vb2"))
        )
      )
  }

  it should "be able to handle null fields" in {
    val table = Table(List("a", "b"), List(List("va1", null)))
    val actualTableJson = table.contentAsJson.asJsObject
    val actualTableTypes = actualTableJson.fields("types")
    val actualTableRows = actualTableJson.fields("rows")
    actualTableTypes shouldBe JsObject(
      ("a", JsString(Table.DISCRETE_TYPE)),
      ("b", JsString(Table.DISCRETE_TYPE))
    )
    actualTableRows shouldBe
      JsArray(
        JsObject(
          ("a", JsString("va1")),
          ("b", JsNull)
        )
      )
  }

  it should "work well with optional String values" in {
    val table = Table(List("data"), List(List(Option("a")), List(Option.empty[String])))
    table.contentAsJson.asJsObject.fields("rows") shouldBe
      JsArray(
        JsObject(
          ("data", JsString("a"))
        ),
        JsObject(
          ("data", JsNull)
        )
      )
  }

  it should "work well with optional Int values" in {
    val table = Table(List("data"), List(List(Option(1)), List(Option.empty[Int])))
    table.contentAsJson.asJsObject.fields("rows") shouldBe
      JsArray(
        JsObject(
          ("data", JsNumber(1))
        ),
        JsObject(
          ("data", JsNull)
        )
      )
  }

  it should "work well with optional custom class values" in {
    case class Custom(value: String)
    val table = Table(List("data"), List(List(Option(Custom("a"))), List(Option.empty[Custom])))
    table.contentAsJson.asJsObject.fields("rows") shouldBe
      JsArray(
        JsObject(
          ("data", JsString("Custom(a)"))
        ),
        JsObject(
          ("data", JsNull)
        )
      )
  }

  "A table with correct value types" should "be present when all rows are numeric" in {
    val table = Table(List("nums"), List(List(1), List(2), List(3)))
    table.contentAsJson.asJsObject.fields("types").asJsObject.fields shouldBe Map("nums" -> JsString("number"))
  }

  it should "be present when all rows are categorical" in {
    val table = Table(List("letters"), List(List("a"), List("b"), List("c")))
    table.contentAsJson.asJsObject.fields("types").asJsObject.fields shouldBe Map("letters" -> JsString("string"))
  }

  it should "be present when all rows are none" in {
    val table = Table(List("nones"), List(List(Option.empty), List(Option.empty), List(Option.empty)))
    table.contentAsJson.asJsObject.fields("types").asJsObject.fields shouldBe Map("nones" -> JsString("string"))
  }

  it should "be present when all rows are nulls" in {
    val table = Table(List("nulls"), List(List(null), List(null)))
    table.contentAsJson.asJsObject.fields("types").asJsObject.fields shouldBe Map("nulls" -> JsString("string"))
  }

  it should "be present when some rows are categorical, others are null" in {
    val table = Table(List("letters"), List(List(Option("a")), List(Option("b")), List(Option.empty[String])))
    table.contentAsJson.asJsObject.fields("types").asJsObject.fields shouldBe Map("letters" -> JsString("string"))
  }

  it should "be present when some rows are numeric, others are null" in {
    val table = Table(List("nums"), List(List(Option(1)), List(Option(2)), List(Option.empty[Int])))
    table.contentAsJson.asJsObject.fields("types").asJsObject.fields shouldBe Map("nums" -> JsString("number"))
  }

  it should "be present when there are numeric and categorical rows" in {
    val table = Table(List("numsAndLetters"), List(List(1), List("b")))
    table.contentAsJson.asJsObject.fields("types").asJsObject.fields shouldBe Map("numsAndLetters" -> JsString("string"))
  }

  it should "be present when there are numeric, categorical, and null rows" in {
    val table = Table(List("numsAndLetters"), List(List(Option(1)), List(Option("b")), List(Option.empty)))
    table.contentAsJson.asJsObject.fields("types").asJsObject.fields shouldBe Map("numsAndLetters" -> JsString("string"))
  }

  it should "be present when there only present optional numeric values" in {
    val table = Table(List("nums"), List(List(Option(1)), List(Option(2))))
    table.contentAsJson.asJsObject.fields("types").asJsObject.fields shouldBe Map("nums" -> JsString("number"))
  }

  it should "be present for all columns" in {
    val table = Table(List("nums", "letters", "nulls"), List(List(1, "a", Option.empty), List(2, "b", Option.empty)))
    table.contentAsJson.asJsObject.fields("types").asJsObject.fields shouldBe Map(
      "nums" -> JsString("number"),
      "letters" -> JsString("string"),
      "nulls" -> JsString("string")
    )
  }

  "Column types of a table" should "have the same order that the rows have" in {
    val table = Table(
      (0 to 20).map("t-" + _),
      List(
        0 to 20,
        20 to 40,
        40 to 60
      )
    )
    val tableJson = table.contentAsJson
    tableJson.asJsObject.fields("types").asJsObject.fields.iterator.toList shouldBe
      (0 to 20).map("t-" + _).zip(List.fill(21)(JsString("number"))).toList
  }

}
