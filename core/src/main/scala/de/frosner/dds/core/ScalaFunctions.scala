package de.frosner.dds.core

import de.frosner.dds.servables._
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.ScalaReflection.Schema
import org.apache.spark.sql.catalyst.{CatalystTypeConvertersAdapter, CatalystTypeConverters, ScalaReflection}
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import scala.util.{Failure, Success}

object ScalaFunctions {

  private[core] def createBar(values: Seq[Double],
                              categories: Seq[String],
                              seriesName: String,
                              title: String): Option[Servable] = {
    createBars(List(seriesName), List(values), categories, title)
  }

  private[core] def createBar(values: Seq[Double],
                              seriesName: String,
                              title: String): Option[Servable] = {
    createBars(List(seriesName), List(values), title)
  }

  private[core] def createBars(labels: Seq[String],
                               values: Seq[Seq[Double]],
                               categories: Seq[String],
                               title: String): Option[Servable] = {
    Option(BarChart(title, categories, values, labels))
  }

  private[core] def createBars(labels: Seq[String], values: Seq[Seq[Double]], title: String): Option[Servable] = {
    val indexedCategories = (1 to values.map(_.size).max).map(_.toString).toSeq
    createBars(labels, values, indexedCategories, title)
  }

  private[core] def createPie(keyValuePairs: Iterable[(String, Double)], title: String): Option[Servable] = {
    Option(PieChart(title, keyValuePairs))
  }

  // TODO unit tests for different number and values of zColorZeroes
  private[core] def createHeatmap(values: Seq[Seq[Double]],
                                  rowNames: Seq[String] = null,
                                  colNames: Seq[String] = null,
                                  zColorZeroes: Seq[Double] = Seq.empty,
                                  title: String): Option[Servable] = {
    if (values.size == 0 || values.head.size == 0) {
      println("Can't show empty heatmap!")
      Option.empty
    } else {
      val actualRowNames: Seq[String] = if (rowNames != null) rowNames else (1 to values.size).map(_.toString)
      val actualColNames: Seq[String] = if (colNames != null) colNames else (1 to values.head.size).map(_.toString)
      val flattenedZ = values.flatten.filter(!_.isNaN)
      val actualZColorZeroes = if (flattenedZ.isEmpty) {
        Seq(Double.NaN, Double.NaN)
      } else if (zColorZeroes.isEmpty) {
        Seq(flattenedZ.min, flattenedZ.max)
      } else if (zColorZeroes.size == 1) {
        val zero = zColorZeroes(0)
        val min = flattenedZ.min
        val max = flattenedZ.max
        if (zero <= min) {
          Seq(zero, max)
        } else if (zero >= max) {
          Seq(min, zero)
        } else {
          Seq(min, zero, max)
        }
      } else {
        zColorZeroes
      }
      Option(Heatmap(title, values, actualRowNames, actualColNames, actualZColorZeroes))
    }
  }

  private[core] def createGraph[ID](vertices: Seq[(ID, String)],
                                    edges: Iterable[(ID, ID, String)],
                                    title: String): Option[Servable] = {
    val indexMap = vertices.map{ case (id, label) => id }.zip(0 to vertices.size).toMap
    val graph = Graph(
      title,
      vertices.map{ case (id, label) => label},
      edges.map{ case (sourceId, targetId, label) => (indexMap(sourceId), indexMap(targetId), label)}
    )
    Option(graph)
  }

  private[core] def createKeyValuePairs(pairs: List[(String, Any)], title: String): Option[Servable] = {
    if (pairs.isEmpty) {
      println("Cannot print empty key-value pairs.")
      Option.empty
    } else {
      Option(KeyValueSequence(
        title = title,
        keyValuePairs = pairs.map{ case (key, value) => (key, value.toString) }
      ))
    }
  }

  private[core] def createHistogram(bins: Seq[Double], frequencies: Seq[Long], title: String): Option[Servable] = {
    Option(Histogram(title, bins, frequencies))
  }

  private[core] def createScatter[X, Y](values: Seq[(X, Y)], title: String)
                                       (implicit numX: Numeric[X] = null, numY: Numeric[Y] = null): Option[Servable] = {
    def toStringOrDouble[T](t: T, num: Numeric[T]) = if (num != null) num.toDouble(t) else t.toString
    Option(ScatterPlot(
      title = title,
      points = values.map{ case (x, y) => (toStringOrDouble(x, numX), toStringOrDouble(y, numY)) },
      xIsNumeric = numX != null,
      yIsNumeric = numY != null
    ))
  }

  private[core] def createTable(schema: StructType,
                                rows: Seq[Row],
                                title: String): Option[Servable] =
    Option(Table(title, schema, rows))

  private[core] def createShow[V](sequence: Seq[V], title: String)(implicit tag: TypeTag[V]): Option[Servable] = {
    val vType = tag.tpe
    if (sequence.length == 0) {
      println("Sequence is empty!")
      Option.empty
    } else {
      // TODO check Option type (=> convert to nullable and what is inside?)
      val inferredSchema = scala.util.Try(ScalaReflection.schemaFor(tag))
      val result = inferredSchema match {
        case Success(Schema(struct: StructType, nullable)) => {
          // convert product to row like in RDDConversions.productToRowRdd
          val numColumns = struct.length
          val converters = struct.fields.map(_.dataType).map(CatalystTypeConvertersAdapter.createToCatalystConverter)
          val rows = sequence.asInstanceOf[Seq[Product]].map { r =>
            var i = 0
            val mutableRow = new GenericMutableRow(numColumns)
            while (i < numColumns) {
              mutableRow(i) = converters(i)(r.productElement(i))
              i += 1
            }
            mutableRow
          }
          Table(title, struct, rows)
        }
        case Success(Schema(other, nullable)) => {
          // RDD of a different type that fits into the catalyst type structure
          val structType = StructType(List(StructField("1", other, nullable)))
          Table(title, structType, sequence.map(c => Row(c)))
        }
        case Failure(exception) => {
          // RDD of a type that does not fit into the catalyst type structure so we convert it to String
          val structType = StructType(List(StructField("1", StringType, true)))
          Table(title, structType, sequence.map(c => Row(if (c != null) c.toString else null)))
        }
      }
      Option(result)
    }
  }

}
