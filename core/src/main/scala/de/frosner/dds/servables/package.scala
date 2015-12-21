package de.frosner.dds

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

package object servables {

  sealed trait Servable {
    val title: String
  }

  // TODO serialize function that takes a serializer

  case class BarChart(title: String,
                      xDomain: Seq[String],
                      heights: Seq[Seq[Double]],
                      series: Seq[String]) extends Servable

  case class PieChart(title: String, categoryCountPairs: Iterable[(String, Double)]) extends Servable

  case class Histogram(title: String, bins: Seq[Double], frequencies: Seq[Long]) extends Servable

  case class Table(title: String, schema: StructType, content: Seq[Row]) extends Servable

  case class Heatmap(title: String,
                     content: Seq[Seq[Double]],
                     rowNames: Seq[String],
                     colNames: Seq[String],
                     zColorZeroes: Seq[Double]) extends Servable

  case class Graph(title: String, vertices: Seq[String], edges: Iterable[(Int, Int, String)]) extends Servable

  case class ScatterPlot(title: String,
                         points: Seq[(Any, Any)],
                         xIsNumeric: Boolean,
                         yIsNumeric: Boolean) extends Servable

  case class KeyValueSequence(title: String, keyValuePairs: Seq[(String, String)]) extends Servable

  // TODO document that the 2D layout is only a recommendation and does not have to be taken into account
  // TODO if it is not appropriate (e.g. on mobile devices)
  case class Composite(title: String, servables: Seq[Seq[Servable]]) extends Servable

  object Blank extends Servable {
    val title = ""
  }

}
