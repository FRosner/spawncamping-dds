package de.frosner.dds.util

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._
import org.scalatest.{Matchers, FlatSpec}

import DataFrameUtils._

class DataFrameUtilsTest extends FlatSpec with Matchers {

  // unused but useful to have a complete listing of all possible types
  private val allTypes = List(ByteType, StringType, BinaryType, BooleanType, TimestampType, DateType,
    ArrayType(StringType), MapType(StringType, StringType), DecimalType(5, 2),
    FloatType, DoubleType, IntegerType, LongType, ShortType)

  "isNumeric" should "be true for supported numeric types" in {
    val numericTypes = List(FloatType, DoubleType, IntegerType, LongType, ShortType)
    numericTypes.foreach(isNumeric(_) shouldBe true)
  }

  it should "be false for non-supported numeric types" in {
    val nonNumericTypes: Seq[DataType] = List(ByteType, StringType, BinaryType, BooleanType, TimestampType, DateType,
      ArrayType(StringType), MapType(StringType, StringType), DecimalType(5, 2))
    nonNumericTypes.foreach(isNumeric(_) shouldBe false)
  }

  "isDate" should "be true for date and timestamp types" in {
    val dateTypes = List(DateType, TimestampType)
    dateTypes.foreach(isDateOrTime(_) shouldBe true)
  }

  it should "be false for all other types" in {
    val nonDateTypes = List(ByteType, StringType, BinaryType, BooleanType, ArrayType(StringType),
      MapType(StringType, StringType), DecimalType(5, 2), FloatType, DoubleType, IntegerType, LongType, ShortType)
    nonDateTypes.foreach(isDateOrTime(_) shouldBe false)
  }

  "isNominal" should "be false for all numeric and date types" in {
    val dateTypes = List(DateType, TimestampType)
    val numericTypes = List(FloatType, DoubleType, IntegerType, LongType, ShortType)
    (dateTypes ++ numericTypes).foreach(isNominal(_) shouldBe false)
  }

  it should "be true for all other types" in {
    val nominalTypes = List(ByteType, StringType, BinaryType, BooleanType, ArrayType(StringType),
      MapType(StringType, StringType), DecimalType(5, 2))
    nominalTypes.foreach(isNominal(_) shouldBe true)
  }

  "asDouble" should "return a double value for non-nullable numeric columns" in {
    val columns = List(
      (Row(1d), StructField("", DoubleType, false)),
      (Row(1f), StructField("", FloatType, false)),
      (Row(1), StructField("", IntegerType, false)),
      (Row(1l), StructField("", LongType, false)),
      (Row(1.toShort), StructField("", ShortType, false))
    )
    columns.foreach{ case (row, field) => numericAsDouble(row, 0, field) shouldBe Option(1d) }
  }

  it should "return a double value for nullable, non-null numeric columns" in {
    val columns = List(
      (Row(1d), StructField("", DoubleType, true)),
      (Row(1f), StructField("", FloatType, true)),
      (Row(1), StructField("", IntegerType, true)),
      (Row(1l), StructField("", LongType, true)),
      (Row(1.toShort), StructField("", ShortType, true))
    )
    columns.foreach{ case (row, field) => numericAsDouble(row, 0, field) shouldBe Option(1d) }
  }

  it should "return an empty value for null numeric columns" in {
    val columns = List(
      (Row(null), StructField("", DoubleType, true)),
      (Row(null), StructField("", FloatType, true)),
      (Row(null), StructField("", IntegerType, true)),
      (Row(null), StructField("", LongType, true)),
      (Row(null), StructField("", ShortType, true))
    )
    columns.foreach{ case (row, field) => numericAsDouble(row, 0, field) shouldBe Option.empty }
  }

  it should "throw an exception for non-numeric columns" in {
    val columns = List(
      (Row(5.toByte), StructField("", ByteType, true)),
      (Row("hallo"), StructField("", StringType, false)),
      (Row(null), StructField("", BinaryType, true)),
      (Row(true), StructField("", BooleanType, true)),
      (Row(new Timestamp(5)), StructField("", TimestampType, false)),
      (Row(new Date(100)), StructField("", DateType, false)),
      (Row(List("")), StructField("", ArrayType(StringType), false)),
      (Row(Map("5" -> "test")), StructField("", MapType(StringType, StringType), false)),
      (Row(java.math.BigDecimal.valueOf(5d)), StructField("", DecimalType(5, 2), false))
    )
    columns.foreach{ case (row, field) => intercept[IllegalArgumentException](numericAsDouble(row, 0, field)) }
  }

  "asTimestamp" should "return a timestamp value for non-nullable date and timestamp columns" in {
    val columns = List(
      (Row(new Timestamp(5)), StructField("", TimestampType, false)),
      (Row(new Date(5)), StructField("", DateType, false))
    )
    columns.foreach{ case (row, field) => dateOrTimeAsTimestamp(row, 0, field) shouldBe Option(new Timestamp(5)) }
  }

  it should "return a timestamp value for nullable, non-null date and timestamp columns" in {
    val columns = List(
      (Row(new Timestamp(5)), StructField("", TimestampType, true)),
      (Row(new Date(5)), StructField("", DateType, true))
    )
    columns.foreach{ case (row, field) => dateOrTimeAsTimestamp(row, 0, field) shouldBe Option(new Timestamp(5)) }
  }

  it should "return an empty value for null date and timestamp columns" in {
    val columns = List(
      (Row(null), StructField("", TimestampType, true)),
      (Row(null), StructField("", DateType, true))
    )
    columns.foreach{ case (row, field) => dateOrTimeAsTimestamp(row, 0, field) shouldBe Option.empty }
  }

  it should "return an empty value for non-date and non-timestamp columns" in {
    val columns = List(
      (Row(5.toByte), StructField("", ByteType, true)),
      (Row("hallo"), StructField("", StringType, false)),
      (Row(null), StructField("", BinaryType, true)),
      (Row(true), StructField("", BooleanType, true)),
      (Row(List("")), StructField("", ArrayType(StringType), false)),
      (Row(Map("5" -> "test")), StructField("", MapType(StringType, StringType), false)),
      (Row(java.math.BigDecimal.valueOf(5d)), StructField("", DecimalType(5, 2), false)),
      (Row(1d), StructField("", DoubleType, true)),
      (Row(1f), StructField("", FloatType, true)),
      (Row(1), StructField("", IntegerType, true)),
      (Row(1l), StructField("", LongType, true)),
      (Row(1.toShort), StructField("", ShortType, true))
    )
    columns.foreach{ case (row, field) => intercept[IllegalArgumentException](dateOrTimeAsTimestamp(row, 0, field)) }
  }

  "asAny" should "return a value for non-nullable columns" in {
    val columns = List(
      (Row(5.toByte), StructField("", ByteType, false)),
      (Row("hallo"), StructField("", StringType, false)),
      (Row("bla".toCharArray.map(_.toByte)), StructField("", BinaryType, false)),
      (Row(true), StructField("", BooleanType, false)),
      (Row(List("")), StructField("", ArrayType(StringType), false)),
      (Row(Map("5" -> "test")), StructField("", MapType(StringType, StringType), false)),
      (Row(java.math.BigDecimal.valueOf(5d)), StructField("", DecimalType(5, 2), false)),
      (Row(1d), StructField("", DoubleType, false)),
      (Row(1f), StructField("", FloatType, false)),
      (Row(1), StructField("", IntegerType, false)),
      (Row(1l), StructField("", LongType, false)),
      (Row(1.toShort), StructField("", ShortType, false)),
      (Row(new Timestamp(5)), StructField("", TimestampType, false)),
      (Row(new Date(5)), StructField("", DateType, false))
    )
    val values = columns.map(_._1.get(0))
    columns.zip(values).foreach{ case ((row, field), expected) =>
      anyAsAny(row, 0, field) shouldBe Option(expected) }
  }

  it should "return a value for nullable, non-null columns" in {
    val columns = List(
      (Row(5.toByte), StructField("", ByteType, true)),
      (Row("hallo"), StructField("", StringType, true)),
      (Row("bla".toCharArray.map(_.toByte)), StructField("", BinaryType, true)),
      (Row(true), StructField("", BooleanType, true)),
      (Row(List("")), StructField("", ArrayType(StringType), true)),
      (Row(Map("5" -> "test")), StructField("", MapType(StringType, StringType), true)),
      (Row(java.math.BigDecimal.valueOf(5d)), StructField("", DecimalType(5, 2), true)),
      (Row(1d), StructField("", DoubleType, true)),
      (Row(1f), StructField("", FloatType, true)),
      (Row(1), StructField("", IntegerType, true)),
      (Row(1l), StructField("", LongType, true)),
      (Row(1.toShort), StructField("", ShortType, true)),
      (Row(new Timestamp(5)), StructField("", TimestampType, true)),
      (Row(new Date(5)), StructField("", DateType, true))
    )
    val values = columns.map(_._1.get(0))
    columns.zip(values).foreach{ case ((row, field), expected) =>
      anyAsAny(row, 0, field) shouldBe Option(expected) }
  }

  it should "return an empty value for null columns" in {
    val columns = List(
      (Row(null), StructField("", ByteType, true)),
      (Row(null), StructField("", StringType, true)),
      (Row(null), StructField("", BinaryType, true)),
      (Row(null), StructField("", BooleanType, true)),
      (Row(null), StructField("", ArrayType(StringType), true)),
      (Row(null), StructField("", MapType(StringType, StringType), true)),
      (Row(null), StructField("", DecimalType(5, 2), true)),
      (Row(null), StructField("", DoubleType, true)),
      (Row(null), StructField("", FloatType, true)),
      (Row(null), StructField("", IntegerType, true)),
      (Row(null), StructField("", LongType, true)),
      (Row(null), StructField("", ShortType, true)),
      (Row(null), StructField("", TimestampType, true)),
      (Row(null), StructField("", DateType, true))
    )
    columns.foreach{ case (row, field) => anyAsAny(row, 0, field) shouldBe Option.empty }
  }

  "getNumericFields" should "return all numeric fields" in {
    val schema = StructType(List(
      StructField("test1", StringType, true),
      StructField("test2", DoubleType, false),
      StructField("test3", IntegerType, true),
      StructField("test4", StringType, false)
    ))
    getNumericFields(schema).toList shouldBe List(
      (1, StructField("test2", DoubleType, false)),
      (2, StructField("test3", IntegerType, true))
    )
  }

  "getDateFields" should "return all date fields" in {
    val schema = StructType(List(
      StructField("test1", StringType, true),
      StructField("test2", DoubleType, false),
      StructField("test3", DateType, true),
      StructField("test4", DateType, false)
    ))
    getDateFields(schema).toList shouldBe List(
      (2, StructField("test3", DateType, true)),
      (3, StructField("test4", DateType, false))
    )
  }

  "getNominalFields" should "return all nominal fields" in {
    val schema = StructType(List(
      StructField("test1", StringType, true),
      StructField("test2", DoubleType, false),
      StructField("test3", IntegerType, true),
      StructField("test4", StringType, false)
    ))
    getNominalFields(schema).toList shouldBe List(
      (0, StructField("test1", StringType, true)),
      (3, StructField("test4", StringType, false))
    )
  }

  "requireSingleColumned" should "execute the toDo if the dataFrame is single columned" in {
    val schema = StructType(List(
      StructField("test1", StringType, true)
    ))
    requireSingleColumned(schema, "test")(Option(5)) shouldBe Option(5)
  }

  "requireSingleColumned" should "not execute the toDo if the dataFrame is single columned" in {
    val schema = StructType(List(
      StructField("test1", StringType, true),
      StructField("test1", StringType, true)
    ))
    requireSingleColumned(schema, "test")(Option(5)) shouldBe Option.empty
  }

  "A double binning udf" should "bin doubles correctly" in {
    val binDouble = binDoubleUdf(10, 0, 10)
    val binningFunction = binDouble.f.asInstanceOf[(Double) => String]
    binningFunction(0) shouldBe "0"
    binningFunction(0.5) shouldBe "0"
    binningFunction(1) shouldBe "1"
    binningFunction(9) shouldBe "9"
    binningFunction(10) shouldBe "9"
  }

  it should "assign no bin to NaN values" in {
    val binDouble = binDoubleUdf(10, 0, 10)
    val binningFunction = binDouble.f.asInstanceOf[(Double) => String]
    binningFunction(Double.NaN) shouldBe null
  }

  it should "assign no bin to values less than the minimum value" in {
    val binDouble = binDoubleUdf(10, 0, 10)
    val binningFunction = binDouble.f.asInstanceOf[(Double) => String]
    binningFunction(-0.5) shouldBe null
  }

  it should "assign no bin to values bigger than the maximum value" in {
    val binDouble = binDoubleUdf(10, 0, 10)
    val binningFunction = binDouble.f.asInstanceOf[(Double) => String]
    binningFunction(10.5) shouldBe null
  }

  it should "require a positive number of bins" in {
    intercept[IllegalArgumentException] {
      binDoubleUdf(0, 0, 10)
    }
  }

  it should "require a minimum value not to be greater than the maximum value" in {
    intercept[IllegalArgumentException] {
      binDoubleUdf(10, 12, 10)
    }
  }

}
