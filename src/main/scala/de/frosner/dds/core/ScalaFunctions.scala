package de.frosner.dds.core

import de.frosner.dds.servables.c3.ChartTypeEnum._
import de.frosner.dds.servables.c3._
import de.frosner.dds.servables.graph.Graph
import de.frosner.dds.servables.histogram.Histogram
import de.frosner.dds.servables.matrix.Matrix2D
import de.frosner.dds.servables.scatter.Points2D
import de.frosner.dds.servables.tabular.{Table, KeyValueSequence}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

object ScalaFunctions {

  private[core] def createIndexedPlot[N](series: Iterable[Series[N]], chartTypes: ChartTypes)
                                        (implicit num: Numeric[N]): Option[Servable] = {
    Option(Chart(SeriesData(series, chartTypes)))
  }

  private[core] def createIndexedPlot[N](series: Iterable[Series[N]], chartType: ChartType)
                                        (implicit num: Numeric[N]): Option[Servable] = {
    createIndexedPlot(series, ChartTypes.multiple(chartType, series.size))
  }

  private[core] def createCategoricalPlot[N](series: Iterable[Series[N]],
                                             categories: Seq[String],
                                             chartType: ChartType)(implicit num: Numeric[N]): Option[Servable] = {
    Option(Chart(SeriesData(series, ChartTypes.multiple(chartType, series.size)), XAxis.categorical(categories)))
  }

  private[core] def createBar[N](values: Seq[N], categories: Seq[String], title: String)
                                (implicit num: Numeric[N]): Option[Servable] = {
    createBars(List(title), List(values), categories)
  }

  private[core] def createBar[N](values: Seq[N], title: String)(implicit num: Numeric[N]): Option[Servable] = {
    createBars(List(title), List(values))
  }

  private[core] def createBars[N](labels: Seq[String], values: Seq[Seq[N]], categories: Seq[String])
                                 (implicit num: Numeric[N]): Option[Servable] = {
    val series = labels.zip(values).map{ case (label, values) => Series(label, values) }
    createCategoricalPlot(series, categories, ChartTypeEnum.Bar)
  }

  private[core] def createBars[N](labels: Seq[String], values: Seq[Seq[N]])
                                 (implicit num: Numeric[N]): Option[Servable] = {
    val series = labels.zip(values).map{ case (label, values) => Series(label, values) }
    createIndexedPlot(series, ChartTypeEnum.Bar)
  }

  private[core] def createLine[N](values: Seq[N])(implicit num: Numeric[N]): Option[Servable] = {
    createLines(List("data"), List(values))
  }

  private[core] def createLines[N](labels: Seq[String], values: Seq[Seq[N]])(implicit num: Numeric[N]): Option[Servable] = {
    val series = labels.zip(values).map{ case (label, values) => Series(label, values) }
    createIndexedPlot(series, ChartTypeEnum.Line)
  }

  def createPie[K, V](keyValuePairs: Iterable[(K, V)])(implicit num: Numeric[V]): Option[Servable] = {
    createIndexedPlot(keyValuePairs.map{ case (key, value) => Series(key.toString, List(value))}, ChartTypeEnum.Pie)
  }

  private[core] def createHeatmap[N](values: Seq[Seq[N]], rowNames: Seq[String] = null,
                                     colNames: Seq[String] = null, title: String = Servable.DEFAULT_TITLE)
                                    (implicit num: Numeric[N]): Option[Servable] = {
    if (values.size == 0 || values.head.size == 0) {
      println("Can't show empty heatmap!")
      Option.empty
    } else {
      val actualRowNames: Seq[String] = if (rowNames != null) rowNames else (1 to values.size).map(_.toString)
      val actualColNames: Seq[String] = if (colNames != null) colNames else (1 to values.head.size).map(_.toString)
      Option(Matrix2D(values.map(_.map(entry => num.toDouble(entry))), actualRowNames, actualColNames, title))
    }
  }

  private[core] def createGraph[ID, VL, EL](vertices: Seq[(ID, VL)], edges: Iterable[(ID, ID, EL)]): Option[Servable] = {
    val indexMap = vertices.map{ case (id, label) => id }.zip(0 to vertices.size).toMap
    val graph = Graph(
      vertices.map{ case (id, label) => label.toString},
      edges.map{ case (sourceId, targetId, label) => (indexMap(sourceId), indexMap(targetId), label.toString)}
    )
    Option(graph)
  }

  private[core] def createKeyValuePairs(pairs: List[(Any, Any)], title: String): Option[Servable] = {
    if (pairs.isEmpty) {
      println("Cannot print empty key-value pairs.")
      Option.empty
    } else {
      Option(KeyValueSequence(pairs))
    }
  }

  private[core] def createHistogram[N1, N2](bins: Seq[N1], frequencies: Seq[N2])
                                           (implicit num1: Numeric[N1], num2: Numeric[N2]): Option[Servable] = {
    Option(Histogram(bins.map(b => num1.toDouble(b)), frequencies.map(f => num2.toLong(f))))
  }

  private[core] def createScatter[N1, N2](values: Seq[(N1, N2)])
                           (implicit num1: Numeric[N1] = null, num2: Numeric[N2] = null): Option[Servable] = {
    Option(Points2D(values)(num1, num2))
  }

  private[core] def createTable(head: Seq[String],
                                rows: Seq[Seq[Any]], 
                                title: String = Servable.DEFAULT_TITLE): Option[Servable] = {
    Option(Table(head, rows, title))
  }

  private[core] def createShow[V](sequence: Seq[V])(implicit tag: TypeTag[V]): Option[Servable] = {
    val vType = tag.tpe
    if (sequence.length == 0) {
      println("Sequence is empty!")
      Option.empty
    } else {
      val result = if (vType <:< typeOf[Product] && !(vType <:< typeOf[Option[_]]) && !(vType <:< typeOf[Iterable[_]])) {
        def getMembers[T: TypeTag] = typeOf[T].members.sorted.collect {
          case m: MethodSymbol if m.isCaseAccessor => m
        }
        val header = getMembers[V].map(_.name.toString.replace("_", ""))
        val rows = sequence.map(product => product.asInstanceOf[Product].productIterator.toSeq).toSeq
        Table(header, rows)
      } else {
        Table(List("sequence"), sequence.map(c => List(c)).toList)
      }
      Option(result)
    }
  }


}
