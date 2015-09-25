package de.frosner.dds.webui.servables

import de.frosner.dds.servables._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.{Matchers, FlatSpec}
import spray.json._

class ServableJsonProtocolTest extends FlatSpec with Matchers {

  private def checkSerDeEquality(servable: Servable) = {
    val servableJs = servable.toJson(ServableJsonProtocol.ServableJsonFormat).asJsObject
    servableJs.convertTo[Servable](ServableJsonProtocol.ServableJsonFormat) shouldBe servable
  }

  // TODO check serialization of null values for all servables (put nulls wherever possible)

  "A bar chart" should "be serialized and deserialized correctly" in {
    checkSerDeEquality {
      BarChart(
        title = "bar",
        xDomain = Seq("a", "b", "c"),
        heights = Seq(Seq(1, 2, 3), Seq(4, 5, 6)),
        series = Seq("1", "2")
      )
    }
  }

  "A pie chart" should "be serialized and deserialized correctly" in {
    checkSerDeEquality {
      PieChart(
        title = "pie",
        categoryCountPairs = List(("a", 1d), ("b", 2d))
      )
    }
  }

  "A histogram" should "be serialized and deserialized correctly" in {
    checkSerDeEquality {
      Histogram(
        title = "hist",
        bins = List(1d, 2d, 3d),
        frequencies = List(2, 5)
      )
    }
  }

  "A table" should "be serialized and deserialized correctly (no bytearray)" in {
    checkSerDeEquality {
      Table(
        title = "table",
        schema = StructType(List(
          StructField("0", ByteType, false),
          StructField("1", ByteType, true),
          StructField("2", ShortType, false),
          StructField("3", ShortType, true),
          StructField("4", IntegerType, false),
          StructField("5", IntegerType, true),
          StructField("6", LongType, false),
          StructField("7", LongType, true),
          StructField("8", FloatType, false),
          StructField("9", FloatType, true),
          StructField("10", DoubleType, false),
          StructField("11", DoubleType, true),
          StructField("12", DecimalType.Unlimited, true),
          StructField("13", StringType, true),
          StructField("15", BooleanType, false),
          StructField("16", BooleanType, true),
          StructField("17", TimestampType, true),
          StructField("18", DateType, true),
          StructField("19", ArrayType(StringType), true),
          StructField("20", MapType(StringType, IntegerType, valueContainsNull = false), true),
          StructField("21", StructType(List(
            StructField("a", IntegerType, false),
            StructField("b", IntegerType, false),
            StructField("c", IntegerType, false)
          )), true)
        )),
        content = Seq(
          Row(
            0.toByte, new java.lang.Byte(0.toByte),
            1.toShort, new java.lang.Short(1.toShort),
            2, new java.lang.Integer(2),
            3l, new java.lang.Long(3l),
            4f, new java.lang.Float(4f),
            5d, new java.lang.Double(5d),
            new java.math.BigDecimal(6d),
            "abc",
            true, new java.lang.Boolean(true),
            new java.sql.Timestamp(10000),
            new java.sql.Date(10000),
            Seq("a", "b", "c"),
            Map("a" -> 1, "b" -> 2, "c" -> 3),
            Row(1, 2, 3)
          ),
          Row(
            0.toByte, null,
            1.toShort, null,
            2, null,
            3l, null,
            4f, null,
            5d, null,
            null,
            null,
            true, null,
            null,
            null,
            null,
            null,
            null
          )
        )
      )
    }
  }

  it should "be serialized and deserialized correctly (bytearray)" in {
    // this needs to be tested in a special way because Array[Byte] does not implement equals
    val table: Servable = Table(
      title = "table",
      schema = StructType(List(StructField("14", BinaryType, true))),
      content = Seq(Row(Array(8.toByte, 8.toByte, 8.toByte)), Row(null))
    )
    val serialized = table.toJson(ServableJsonProtocol.ServableJsonFormat).asJsObject
    val deserialized = serialized.convertTo[Servable](ServableJsonProtocol.ServableJsonFormat).asInstanceOf[Table]
    deserialized.content should have length 2
    deserialized.content(0).toSeq should have length 1
    deserialized.content(0).getAs[Array[Byte]](0).toSeq shouldBe Seq(8.toByte, 8.toByte, 8.toByte)
    deserialized.content(1).toSeq should have length 1
    deserialized.content(1).isNullAt(0) shouldBe true
  }

  "A heatmap" should "be serialized and deserialized correctly" in {
    checkSerDeEquality {
      Heatmap(
        title = "heatmap",
        content = Seq(
          Seq(1d, 2d, 3d),
          Seq(4d, 5d, 6d),
          Seq(7d, 8d, 9d)
        ),
        rowNames = Seq("1", "2", "3"),
        colNames = Seq("a", "b", "c"),
        zColorZeroes = Seq(0d, 9d)
      )
    }
  }

  "A graph" should "be serialized and deserialized correctly" in {
    checkSerDeEquality {
      Graph(
        title = "graph",
        vertices = Seq("v1", "v2", "v3"),
        edges = Seq(
          (0, 1, "v1-v2"),
          (1, 2, "v2-v3")
        )
      )
    }
  }

  "A scatter plot" should "be serialized and deserialized correctly" in {
    checkSerDeEquality {
      ScatterPlot(
        title = "scatter",
        points = Seq(
          (5d, "a"),
          (2d, "b"),
          (3d, "c")
        ),
        xIsNumeric = true,
        yIsNumeric = false
      )
    }
  }

  "A key value sequence" should "be serialized and deserialized correctly" in {
    checkSerDeEquality {
      KeyValueSequence(
        title = "Key Value Sequence",
        keyValuePairs = Seq(
          ("a", "5"),
          ("b", "3")
        )
      )
    }
  }

  "A composite servable" should "be serialized and deserialized correctly" in {
    checkSerDeEquality {
      Composite(
        title = "Composite",
        servables = Seq(
          Seq(Blank, KeyValueSequence("kvs", Seq(("a", "b")))),
          Seq(Histogram("hist", List(1d, 2d, 3d), List(2, 5)))
        )
      )
    }
  }

  "A blank servable" should "be serialized and deserialized correctly" in {
    checkSerDeEquality {
      Blank
    }
  }

}
