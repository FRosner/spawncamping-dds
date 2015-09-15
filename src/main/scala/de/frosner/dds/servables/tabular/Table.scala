package de.frosner.dds.servables.tabular

import de.frosner.dds.core.Servable
import de.frosner.dds.servables.tabular.Table._
import org.apache.spark.util.StatCounter
import spray.json._

case class Table(head: Seq[String], rows: Seq[Seq[Any]], title: String = Servable.DEFAULT_TITLE) extends Servable {

  val servableType = "table"

  private lazy val namesValuesTypes = rows.map(row =>
    head.zip(row).map{ case (columnName, value) => {
      val (jsValue, jsType) = value match {
        case None => (JsNull, NULL_TYPE)
        case null => (JsNull, NULL_TYPE)
        case Some(int: Int) => (JsNumber(int), NUMERIC_TYPE)
        case int: Int => (JsNumber(int), NUMERIC_TYPE)
        case Some(double: Double) => (JsNumber(double), NUMERIC_TYPE)
        case double: Double => (JsNumber(double), NUMERIC_TYPE)
        case Some(long: Long) => (JsNumber(long), NUMERIC_TYPE)
        case long: Long => (JsNumber(long), NUMERIC_TYPE)
        case Some(default) => (JsString(default.toString), DISCRETE_TYPE)
        case default => (JsString(default.toString), DISCRETE_TYPE)
      }
      (columnName, jsValue, jsType)
    }}
  )

  def contentAsJson: JsValue = {
    val jsRows = namesValuesTypes.map( row =>
      JsObject(OrderedMap[String, JsValue](
        row.map{ case (columnName, jsValue, jsType) => (columnName, jsValue) }
      ))
    )
    val jsTypes = namesValuesTypes.transpose.map( column => {
      val typeCounts = column.groupBy{ case (name, jsValue, jsType) => jsType }.mapValues(_.length)
      val (name, _, _) = column.head
      (name, JsString(
        if (typeCounts.contains(DISCRETE_TYPE))
          DISCRETE_TYPE
        else if (typeCounts.contains(NUMERIC_TYPE))
          NUMERIC_TYPE
        else
          DISCRETE_TYPE // treat all null values as discrete
      ))
    })
    JsObject(
      ("types", JsObject(OrderedMap(jsTypes))),
      ("rows", JsArray(jsRows.toVector))
    )
  }

}

object Table {

  val DISCRETE_TYPE = "string"

  val NUMERIC_TYPE = "number"

  val NULL_TYPE = "null"

  def fromStatCounter(stat: StatCounter, title: String = Servable.DEFAULT_TITLE): Table =
    fromStatCounters(List.empty, List(stat), title)

  def fromStatCounters(labels: Seq[String], stats: Seq[StatCounter], title: String = Servable.DEFAULT_TITLE) = {
    val optionalLabelHead = if (labels.size > 0) List("label") else List.empty
    val head = optionalLabelHead ++ List(
      "count", "sum", "min", "max", "mean", "stdev", "variance"
    )
    val rows = if (labels.size > 0) {
      labels.zip(stats).map{ case (label, stats) =>
        List(label, stats.count, stats.sum, stats.min, stats.max, stats.mean, stats.stdev, stats.variance)
      }
    } else {
      stats.map(stats => List(stats.count, stats.sum, stats.min, stats.max, stats.mean, stats.stdev, stats.variance))
    }
    Table(head, rows, title)
  }

}
