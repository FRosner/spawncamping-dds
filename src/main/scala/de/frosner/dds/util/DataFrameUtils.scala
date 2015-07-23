package de.frosner.dds.util

import java.sql.Timestamp
import java.sql.Date

import de.frosner.dds.core.DDS
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._

object DataFrameUtils {

  private def filterFields(schema: StructType)(filter: DataType => Boolean): Iterable[(Int, StructField)] =
    schema.fields.zipWithIndex.filter{ case (field, index) => filter(field.dataType) }.map(_.swap)

  def isNumeric(dataType: DataType) = dataType == DoubleType || dataType == FloatType || dataType == IntegerType ||
    dataType == LongType || dataType == ShortType

  def getNumericFields(dataFrame: DataFrame): Iterable[(Int, StructField)] = getNumericFields(dataFrame.schema)

  def getNumericFields(schema: StructType): Iterable[(Int, StructField)] = filterFields(schema)(isNumeric)

  def isDateOrTime(dataType: DataType) = dataType == DateType || dataType == TimestampType

  def getDateFields(dataFrame: DataFrame): Iterable[(Int, StructField)] = getDateFields(dataFrame.schema)

  def getDateFields(schema: StructType): Iterable[(Int, StructField)] = filterFields(schema)(isDateOrTime)

  def isNominal(dataType: DataType) = !isNumeric(dataType) && !isDateOrTime(dataType)

  def getNominalFields(dataFrame: DataFrame): Iterable[(Int, StructField)] = getNominalFields(dataFrame.schema)

  def getNominalFields(schema: StructType): Iterable[(Int, StructField)] = filterFields(schema)(isNominal)

  def numericAsDouble(row: Row, index: Int, field: StructField): Option[Double] = {
    val dataType = field.dataType
    val nullable = field.nullable
    (dataType, nullable) match {
      case (DoubleType, true) => if (row.isNullAt(index)) Option.empty[Double] else Option(row.getDouble(index))
      case (DoubleType, false) => Option(row.getDouble(index))
      case (IntegerType, true) => if (row.isNullAt(index)) Option.empty[Double] else Option(row.getInt(index).toDouble)
      case (IntegerType, false) => Option(row.getInt(index).toDouble)
      case (FloatType, true) => if (row.isNullAt(index)) Option.empty[Double] else Option(row.getFloat(index).toDouble)
      case (FloatType, false) => Option(row.getFloat(index).toDouble)
      case (LongType, true) => if (row.isNullAt(index)) Option.empty[Double] else Option(row.getLong(index).toDouble)
      case (LongType, false) => Option(row.getLong(index).toDouble)
      case (ShortType, true) => if (row.isNullAt(index)) Option.empty[Double] else Option(row.getShort(index).toDouble)
      case (ShortType, false) => Option(row.getShort(index).toDouble)
      case (_, _) => throw new IllegalArgumentException(s"Column cannot be converted to double: $index ($field)")
    }
  }

  def dateOrTimeAsTimestamp(row: Row, index: Int, field: StructField): Option[Timestamp] = {
    val dataType = field.dataType
    val nullable = field.nullable
    (dataType, nullable) match {
      case (DateType, true) => if (row.isNullAt(index)) Option.empty[Timestamp] else Option(new Timestamp(row.getAs[Date](index).getTime))
      case (DateType, false) => Option(new Timestamp(row.getAs[Date](index).getTime))
      case (TimestampType, true) => if (row.isNullAt(index)) Option.empty[Timestamp] else Option(row.getAs[Timestamp](index))
      case (TimestampType, false) => Option(row.getAs[Timestamp](index))
      case (_, _) => throw new IllegalArgumentException(s"Column cannot be converted to timestamp: $index ($field)")
    }
  }

  def anyAsAny(row: Row, index: Int, field: StructField): Option[Any] = {
    if (field.nullable) {
      if (row.isNullAt(index)) Option.empty else Option(row.get(index))
    } else {
      Option(row.get(index))
    }
  }

  def requireSingleColumned[R](dataFrame: DataFrame, function: String)(toDo: => Option[R]): Option[R] =
    requireSingleColumned(dataFrame.schema, function)(toDo)

  def requireSingleColumned[R](schema: StructType, function: String)(toDo: => Option[R]): Option[R] = {
    if (schema.fields.size != 1) {
      println(function + " function only supported on single columns.")
      println
      DDS.help(function)
      Option.empty[R]
    } else {
      toDo
    }
  }

  def binDoubleUdf(numBins: Int, min: Double, max: Double) = {
    require(numBins > 0, "The number of bins must be greater than 0")
    require(min <= max, "The minimum value must not be greater than the maximum")
    udf((value: Double) => {
      if (value.isNaN || value < min || value > max) {
        null.asInstanceOf[String]
      } else {
        val bin = (((value - min) / (max - min)) * numBins).toInt
        Math.min(bin, numBins - 1).toString
      }
    })
  }

}
