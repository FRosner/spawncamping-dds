package de.frosner.dds.core

import de.frosner.dds.analytics.{MutualInformationAggregator, CorrelationAggregator}
import de.frosner.dds.servables.c3.ChartTypeEnum.ChartType
import de.frosner.dds.servables.c3._
import de.frosner.dds.servables.composite.CompositeServable
import de.frosner.dds.servables.graph.Graph
import de.frosner.dds.servables.histogram.Histogram
import de.frosner.dds.servables.matrix.Matrix2D
import de.frosner.dds.servables.scatter.Points2D
import de.frosner.dds.servables.tabular.Table
import org.apache.log4j.Logger
import org.apache.spark.SparkContext._
import org.apache.spark.graphx
import org.apache.spark.graphx.{Edge, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.util.StatCounter

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
 * Main object containing the core commands that can be executed from the Spark shell. It holds a mutable reference
 * to a [[Server]] which is used to communicate the results to the web front-end.
 *
 * Hacks applied here:
 *
 * - ClassTags are needed for conversion to PairRDD
 *   http://mail-archives.apache.org/mod_mbox/incubator-spark-user/201404.mbox/%3CCANGvG8o-EWeETtYb3VGpmSR9ZvJui8vPO-aLsKj7xTMYQgsPAg@mail.gmail.com%3E
 */
object DDS {

  private val logger = Logger.getLogger("DDS")

  private val helper = Helper(this.getClass)

  private var servable: Option[Servable] = Option.empty

  private var server: Option[Server] = Option.empty
  private var serverNumber = 1

  private[core] def start(server: Server): Unit = {
    logger.debug(s"Attempting to start $server")
    if (this.server.isDefined) {
      println("Server already started! Type 'help()' to see a list of available commands.")
    } else {
      this.server = Option(server)
      serverNumber += 1
      this.server.map(_.start())
    }
  }

  @Help(
    category = "Web UI",
    shortDescription = "Starts the DDS Web UI",
    longDescription = "Starts the DDS Web UI bound to the default interface and port. You can stop it by calling stop()."
  )
  def start(): Unit = {
    start(SprayServer("dds-" + serverNumber))
  }

  @Help(
    category = "Web UI",
    shortDescription = "Starts the DDS Web UI bound to the given interface and port with an optional authentication mechanism",
    longDescription = "Starts the DDS Web UI bound to the given interface and port. You can also specify a password " +
      "which will be used for a simple HTTP authentication. Note however, that this is transmitting the password " +
      "unencrypted and you should not reuse this password somewhere else. You can stop it by calling stop().",
    parameters = "interface: String, port: Int, (optional) password: String"
  )
  def start(interface: String, port: Int, password: String = null): Unit = {
    start(SprayServer(
      "dds-" + serverNumber,
      interface = interface,
      port = port,
      launchBrowser = true,
      password = Option(password)
    ))
  }

  private[core] def resetServer() = {
    server = Option.empty
  }

  @Help(
    category = "Web UI",
    shortDescription = "Stops the DDS Web UI",
    longDescription = "Stops the DDS Web UI. You can restart it again by calling start()."
  )
  def stop() = {
    logger.debug(s"Attempting to stop $server")
    if (!server.isDefined) {
      println("No server there to stop! Type 'start()' to start one.")
    } else {
      server.map(_.stop())
      resetServer()
    }
  }

  @Help(
    category = "Help",
    shortDescription = "Shows available commands",
    longDescription = "Shows all commands available in DDS."
  )
  def help() = {
    helper.printAllMethods(System.out)
  }

  @Help(
    category = "Help",
    shortDescription = "Explains given command",
    longDescription = "Explains the given command.",
    parameters = "commandName: String"
  )
  def help(methodName: String) = {
    helper.printMethods(methodName, System.out)
  }

  private def serve(maybeServable: Option[Servable]): Unit =
    maybeServable.foreach(servable => serve(servable))

  private def serve(servable: Servable): Unit = {
    logger.debug(s"Attempting to serve $servable to $server")
    if (server.isDefined) {
      server.get.serve(servable)
    } else {
      println("Front-end not started. Type 'start()' to start the web-UI.")
    }
  }

  private def indexedPlot[N](series: Iterable[Series[N]], chartTypes: ChartTypes)(implicit num: Numeric[N]): Unit = {
    serve(Chart(SeriesData(series, chartTypes)))
  }

  private def indexedPlot[N](series: Iterable[Series[N]], chartType: ChartType)(implicit num: Numeric[N]): Unit = {
    indexedPlot(series, ChartTypes.multiple(chartType, series.size))
  }

  private def categoricalPlot[N](series: Iterable[Series[N]],
                                 categories: Seq[String],
                                 chartTypes: ChartTypes)(implicit num: Numeric[N]): Unit = {
    serve(Chart(SeriesData(series, chartTypes), XAxis.categorical(categories)))
  }

  private def categoricalPlot[N](series: Iterable[Series[N]],
                                 categories: Seq[String],
                                 chartType: ChartType)(implicit num: Numeric[N]): Unit = {
    categoricalPlot(series, categories, ChartTypes.multiple(chartType, series.size))
  }

  @Help(
    category = "Scala",
    shortDescription = "Plots a graph",
    longDescription = "Plots a graph layouted by the D3 force layout.",
    parameters = "vertices: Seq[(VertexId, Label)], edges: Seq[(SourceVertexId, TargetVertexId, Label)]"
  )
  def graph[ID, VL, EL](vertices: Seq[(ID, VL)], edges: Iterable[(ID, ID, EL)]): Unit = {
    val indexMap = vertices.map{ case (id, label) => id }.zip(0 to vertices.size).toMap
    val graph = Graph(
      vertices.map{ case (id, label) => label.toString},
      edges.map{ case (sourceId, targetId, label) => (indexMap(sourceId), indexMap(targetId), label.toString)}
    )
    serve(graph)
  }

  @Help(
    category = "Scala",
    shortDescription = "Plots a scatter plot",
    longDescription = "Plots a scatter plot of the given points. A point is represented as a pair of X and Y coordinates." +
      "Works with both, numeric or nominal values and will plot the axes accordingly.",
    parameters = "values: Seq[(Value, Value)]"
  )
  def scatter[N1, N2](values: Seq[(N1, N2)])(implicit num1: Numeric[N1] = null, num2: Numeric[N2] = null) = {
    serve(Points2D(values)(num1, num2))
  }

  private def createHeatmap[N](values: Seq[Seq[N]], rowNames: Seq[String] = null, colNames: Seq[String] = null)
                              (implicit num: Numeric[N]): Option[Servable] = {
    if (values.size == 0 || values.head.size == 0) {
      println("Can't show empty heatmap!")
      Option.empty
    } else {
      val actualRowNames: Seq[String] = if (rowNames != null) rowNames else (1 to values.size).map(_.toString)
      val actualColNames: Seq[String] = if (colNames != null) colNames else (1 to values.head.size).map(_.toString)
      Option(Matrix2D(values.map(_.map(entry => num.toDouble(entry))), actualRowNames, actualColNames))
    }
  }

  @Help(
    category = "Scala",
    shortDescription = "Plots a heat map",
    longDescription = "Plots the given matrix as a heat map. values(i)(j) corresponds to the element in the ith row" +
      " and the jth column. If no row or column names are specified, the rows or columns will just have incerementing" +
      " numbers as labels.",
    parameters = "values: Seq[Seq[NumericValue]], (optional) rowNames: Seq[String], (optional) colNames: Seq[String]"
  )
  def heatmap[N](values: Seq[Seq[N]], rowNames: Seq[String] = null, colNames: Seq[String] = null)
                (implicit num: Numeric[N]): Unit = {
    serve(createHeatmap(values, rowNames, colNames)(num))
  }

  @Help(
    category = "Scala",
    shortDescription = "Plots a histogram chart of already binned data",
    longDescription = "Plots a histogram chart visualizing the given bins and frequencies. " +
      "The bins are defined by their borders. To specify n bins, you need to pass n+1 borders and n frequencies." +
      "\n\nExample: \n" +
      "* 5 people are between 0 and 18 years old, 10 people between 18 and 25\n" +
      "* bins = [0, 18, 25], frequencies = [5, 10]",
    parameters = "bins: Seq[Numeric], frequencies: Seq[Numeric]"
  )
  def histogram[N1, N2](bins: Seq[N1], frequencies: Seq[N2])(implicit num1: Numeric[N1], num2: Numeric[N2]) = {
    serve(Histogram(bins.map(b => num1.toDouble(b)), frequencies.map(f => num2.toLong(f))))
  }

  @Help(
    category = "Scala",
    shortDescription = "Plots a line chart",
    longDescription = "Plots a line chart visualizing the given value sequence.",
    parameters = "values: Seq[NumericValue]"
  )
  def line[N](values: Seq[N])(implicit num: Numeric[N]) = {
    lines(List("data"), List(values))
  }

  @Help(
    category = "Scala",
    shortDescription = "Plots a line chart with multiple lines",
    longDescription = "Plots a line chart with multiple lines. Each line corresponds to one of the value sequences " +
      "and is labeled according to the labels specified.",
    parameters = "labels: Seq[String], values: Seq[Seq[NumericValue]]"
  )
  def lines[N](labels: Seq[String], values: Seq[Seq[N]])(implicit num: Numeric[N]) = {
    val series = labels.zip(values).map{ case (label, values) => Series(label, values) }
    indexedPlot(series, ChartTypeEnum.Line)
  }

  private val DEFAULT_BAR_TITLE = "data"

  @Help(
    category = "Scala",
    shortDescription = "Plots a bar chart with an indexed x-axis.",
    longDescription = "Plots a bar chart with an indexed x-axis visualizing the given value sequence.",
    parameters = "values: Seq[NumericValue]"
  )
  def bar[N](values: Seq[N], title: String)(implicit num: Numeric[N]): Unit = {
    bars(List(title), List(values))
  }

  def bar[N](values: Seq[N])(implicit num: Numeric[N]): Unit =
    bar(values, DEFAULT_BAR_TITLE)

  @Help(
    category = "Scala",
    shortDescription = "Plots a bar chart with a categorical x-axis.",
    longDescription = "Plots a bar chart with a categorical x-axis visualizing the given value sequence.",
    parameters = "values: Seq[NumericValue], categories: Seq[String]"
  )
  def bar[N](values: Seq[N], categories: Seq[String], title: String)(implicit num: Numeric[N]): Unit = {
    bars(List(title), List(values), categories)
  }

  def bar[N](values: Seq[N], categories: Seq[String])(implicit num: Numeric[N]): Unit =
    bar(values, categories, DEFAULT_BAR_TITLE)

  @Help(
    category = "Scala",
    shortDescription = "Plots a bar chart with an indexed x-axis and multiple bar colors",
    longDescription = "Plots a bar chart with an indexed x-axis and multiple bar colors. " +
      "Each color corresponds to one of the value sequences " +
      "and is labeled according to the labels specified.",
    parameters = "labels: Seq[String], values: Seq[Seq[NumericValue]]"
  )
  def bars[N](labels: Seq[String], values: Seq[Seq[N]])(implicit num: Numeric[N]) = {
    val series = labels.zip(values).map{ case (label, values) => Series(label, values) }
    indexedPlot(series, ChartTypeEnum.Bar)
  }

  @Help(
    category = "Scala",
    shortDescription = "Plots a bar chart with a categorical x-axis and multiple bar colors",
    longDescription = "Plots a bar chart with a categorical x-axis and multiple bar colors. " +
      "Each color corresponds to one of the value sequences " +
      "and is labeled according to the labels specified.",
    parameters = "labels: Seq[String], values: Seq[Seq[NumericValue]], categories: Seq[String]"
  )
  def bars[N](labels: Seq[String], values: Seq[Seq[N]], categories: Seq[String])(implicit num: Numeric[N]) = {
    val series = labels.zip(values).map{ case (label, values) => Series(label, values) }
    categoricalPlot(series, categories, ChartTypeEnum.Bar)
  }

  @Help(
    category = "Scala",
    shortDescription = "Plots a pie chart with the given value per group",
    longDescription = "Plots a pie chart with the given value per group. The input must contain each key only once.",
    parameters = "keyValuePairs: Iterable[(Key, NumericValue)]"
  )
  def pie[K, V](keyValuePairs: Iterable[(K, V)])(implicit num: Numeric[V]): Unit = {
    indexedPlot(keyValuePairs.map{ case (key, value) => Series(key.toString, List(value))}, ChartTypeEnum.Pie)
  }

  @Help(
    category = "Spark Core",
    shortDescription = "Plots a bar chart with the counts of all distinct values in this RDD",
    longDescription = "Plots a bar chart with the counts of all distinct values in this RDD. This makes most sense for " +
      "non-numeric values that have a relatively low cardinality.",
    parameters = "values: RDD[Value]"
  )
  def bar[V: ClassTag](values: RDD[V], title: String): Unit = {
    val (distinctValues, distinctCounts) =
      values.map((_, 1)).reduceByKey(_ + _).collect.sortBy{ case (value, count) => count }.reverse.unzip
    bar(distinctCounts, distinctValues.map(_.toString), title)
  }

  def bar[V: ClassTag](values: RDD[V]): Unit = bar(values, DEFAULT_BAR_TITLE)

  @Help(
    category = "Spark SQL",
    shortDescription = "Plots a bar chart with the counts of all distinct values in this single columned data frame",
    longDescription = "Plots a bar chart with the counts of all distinct values in this single columned data frame. " +
      "This makes most sense for non-numeric values that have a relatively low cardinality. You can also specify an " +
      "optional value to replace missing values with. If no missing value is specified, Scala's Option trait is used.",
    parameters = "dataFrame: DataFrame, (optional) nullValue: Any"
  )
  def bar(dataFrame: DataFrame, nullValue: Any = null): Unit = {
    if (dataFrame.columns.size != 1) {
      println("Bar function only supported on single columns.")
      println
      help("bar")
    } else {
      val field = dataFrame.schema.fields.head
      val rdd = dataFrame.rdd
      (field.nullable) match {
        case true => bar(rdd.map(row => if (nullValue == null) {
            if (row.isNullAt(0)) Option.empty[Any] else Option(row(0))
          } else {
            if (row.isNullAt(0)) nullValue else row(0)
          }
        ), field.name)
        case false => bar(rdd.map(row => row(0)), field.name)
      }
    }
  }

  @Help(
    category = "Spark Core",
    shortDescription = "Plots a pie chart with the counts of all distinct values in this RDD",
    longDescription = "Plots a pie chart with the counts of all distinct values in this RDD. This makes most sense for " +
      "non-numeric values that have a relatively low cardinality.",
    parameters = "values: RDD[Value]"
  )
  def pie[V: ClassTag](values: RDD[V]): Unit = {
    pie(values.map((_, 1)).reduceByKey(_ + _).collect)
  }

  private val DEFAULT_HISTOGRAM_NUM_BUCKETS = 100

  @Help(
    category = "Spark Core",
    shortDescription = "Plots a histogram of a numerical RDD for the given number of buckets",
    longDescription = "Plots a histogram of a numerical RDD for the given number of buckets. " +
      "The number of buckets parameter is optional having the default value of 100.",
    parameters = "values: RDD[NumericValue], (optional) numBuckets: Int"
  )
  def histogram[N: ClassTag](values: RDD[N], numBuckets: Int = DEFAULT_HISTOGRAM_NUM_BUCKETS)(implicit num: Numeric[N]): Unit = {
    val (buckets, frequencies) = values.map(v => num.toDouble(v)).histogram(numBuckets)
    histogram(buckets, frequencies)
  }

  @Help(
    category = "Spark Core",
    shortDescription = "Plots a histogram of a numerical RDD for the given buckets",
    longDescription = "Plots a histogram of a numerical RDD for the given buckets. " +
      "If the buckets do not include the complete range of possible values, some values will be missing in the histogram.",
    parameters = "values: RDD[NumericValue], buckets: Seq[NumericValue]"
  )
  def histogram[N1: ClassTag, N2: ClassTag](values: RDD[N1], buckets: Seq[N2])
                                           (implicit num1: Numeric[N1], num2: Numeric[N2]): Unit = {
    val frequencies = values.map(v => num1.toLong(v)).histogram(buckets.map(b => num2.toDouble(b)).toArray, false)
    histogram(buckets, frequencies)
  }

  @Help(
    category = "Spark SQL",
    shortDescription = "Plots a histogram of a single column data frame for the given buckets",
    longDescription = "Plots a histogram of a single column data frame for the given buckets. " +
      "If the buckets do not include the complete range of possible values, some values will be missing in the histogram. " +
      "If the column contains null values, they will be ignored in the computation.",
    parameters = "dataFrame: DataFrame, buckets: Seq[NumericValue]"
  )
  def histogram[N: ClassTag](dataFrame: DataFrame, buckets: Seq[N])(implicit num: Numeric[N]): Unit = {
    if (dataFrame.columns.size != 1) {
      println("Histogram function only supported on single columns.")
      println
      help("histogram")
    } else {
      val fieldType = dataFrame.schema.fields.head
      val rdd = dataFrame.rdd
      (fieldType.dataType, fieldType.nullable) match {
        case (DoubleType, true) => histogram(rdd.flatMap(row =>
          if (row.isNullAt(0)) Option.empty[Double] else Option(row.getDouble(0))
        ), buckets)
        case (DoubleType, false) => histogram(rdd.map(row => row.getDouble(0)), buckets)
        case (IntegerType, true) => histogram(rdd.flatMap(row =>
          if (row.isNullAt(0)) Option.empty[Int] else Option(row.getInt(0))
        ), buckets)
        case (IntegerType, false) => histogram(rdd.map(row => row.getInt(0)), buckets)
        case (FloatType, true) => histogram(rdd.flatMap(row =>
          if (row.isNullAt(0)) Option.empty[Float] else Option(row.getFloat(0))
        ), buckets)
        case (FloatType, false) => histogram(rdd.map(row => row.getFloat(0)), buckets)
        case (LongType, true) => histogram(rdd.flatMap(row =>
          if (row.isNullAt(0)) Option.empty[Long] else Option(row.getLong(0))
        ), buckets)
        case (LongType, false) => histogram(rdd.map(row => row.getLong(0)), buckets)
        case _ => println("Histogram only supported for numerical columns.")
      }
    }
  }

  @Help(
    category = "Spark SQL",
    shortDescription = "Plots a histogram of a single column data frame for the given number of buckets",
    longDescription = "Plots a histogram of a single column data frame for the given number of buckets. " +
      "The number of buckets parameter is optional having the default value of 100. " +
      "If the column contains null values, they will be ignored in the computation.",
    parameters = "dataFrame: DataFrame, (optional) numBuckets: Int"
  )
  def histogram(dataFrame: DataFrame, numBuckets: Int): Unit = {
    if (dataFrame.columns.size != 1) {
      println("Histogram function only supported on single columns.")
      println
      help("histogram")
    } else {
      val fieldType = dataFrame.schema.fields.head
      val rdd = dataFrame.rdd
      (fieldType.dataType, fieldType.nullable) match {
        case (DoubleType, true) => histogram(rdd.flatMap(row =>
          if (row.isNullAt(0)) Option.empty[Double] else Option(row.getDouble(0))
        ), numBuckets)
        case (DoubleType, false) => histogram(rdd.map(row => row.getDouble(0)), numBuckets)
        case (IntegerType, true) => histogram(rdd.flatMap(row =>
          if (row.isNullAt(0)) Option.empty[Int] else Option(row.getInt(0))
        ), numBuckets)
        case (IntegerType, false) => histogram(rdd.map(row => row.getInt(0)), numBuckets)
        case (FloatType, true) => histogram(rdd.flatMap(row =>
          if (row.isNullAt(0)) Option.empty[Float] else Option(row.getFloat(0))
        ), numBuckets)
        case (FloatType, false) => histogram(rdd.map(row => row.getFloat(0)), numBuckets)
        case (LongType, true) => histogram(rdd.flatMap(row =>
          if (row.isNullAt(0)) Option.empty[Long] else Option(row.getLong(0))
        ), numBuckets)
        case (LongType, false) => histogram(rdd.map(row => row.getLong(0)), numBuckets)
        case _ => println("Histogram only supported for numerical columns.")
      }
    }
  }

  def histogram(dataFrame: DataFrame): Unit = histogram(dataFrame, DEFAULT_HISTOGRAM_NUM_BUCKETS)

  @Help(
    category = "Spark Core",
    shortDescription = "Plots a pie chart of the reduced values per group",
    longDescription = "Given the already grouped RDD, reduces the values in each group and compares the group using a pie chart.",
    parameters = "groupedValues: RDD[(Key, Iterable[NumericValue])]",
    parameters2 = "reduceFunction: (NumericValue, NumericValue => NumericValue)"
  )
  def pieGroups[K, N](groupValues: RDD[(K, Iterable[N])])
                     (reduceFunction: (N, N) => N)
                     (implicit num: Numeric[N]): Unit = {
    pie(groupValues.map{ case (key, values) => (key, values.reduce(reduceFunction)) }.collect)
  }

  @Help(
    category = "Spark Core",
    shortDescription = "Plots a pie chart of the reduced values per group",
    longDescription = "Groups the given pair RDD, reduces the values in each group and compares the group using a pie chart.",
    parameters = "toBeGroupedValues: RDD[(Key, NumericValue)]",
    parameters2 = "reduceFunction: (NumericValue, NumericValue => NumericValue)"
  )
  def groupAndPie[K: ClassTag, N: ClassTag](toBeGroupedValues: RDD[(K, N)])
                                           (reduceFunction: (N, N) => N)
                                           (implicit num: Numeric[N]): Unit = {
    pie(toBeGroupedValues.reduceByKey(reduceFunction).collect.sortBy{ case (value, count) => count })
  }

  private def table(table: Table): Unit = {
    serve(table)
  }

  private def createTable(head: Seq[String], rows: Seq[Seq[Any]]): Option[Servable] = {
    Option(Table(head, rows))
  }

  @Help(
    category = "Scala",
    shortDescription = "Displays data in tabular format",
    longDescription = "Displays the given rows as a table using the specified head. DDS also shows visualizations of the" +
      "data in the table.",
    parameters = "head: Seq[String], rows: Seq[Seq[Any]]"
  )
  def table(head: Seq[String], rows: Seq[Seq[Any]]): Unit = {
    serve(createTable(head, rows))
  }

  @Help(
    category = "Scala",
    shortDescription = "Shows a sequence",
    longDescription = "Shows a sequence. In addition to a tabular view DDS also shows visualizations" +
      "of the data.",
    parameters = "sequence: Seq[T]"
  )
  def show[V](sequence: Seq[V])(implicit tag: TypeTag[V]): Unit = {
    val vType = tag.tpe
    if (sequence.length == 0) {
      println("Sequence is empty!")
    } else {
      val result = if (vType <:< typeOf[Product] && !(vType <:< typeOf[Option[_]]) && !(vType <:< typeOf[Iterable[_]])) {
        def getMembers[T: TypeTag] = typeOf[T].members.sorted.collect {
          case m: MethodSymbol if m.isCaseAccessor => m
        }.toList
        val header = getMembers[V].map(_.name.toString.replace("_", ""))
        val rows = sequence.map(product => product.asInstanceOf[Product].productIterator.toSeq).toSeq
        Table(header, rows)
      } else {
        Table(List("sequence"), sequence.map(c => List(c)).toList)
      }
      table(result)
    }
  }

  private val DEFAULT_SHOW_SAMPLE_SIZE = 100

  @Help(
    category = "Spark Core",
    shortDescription = "Shows the first rows of an RDD",
    longDescription = "Shows the first rows of an RDD. In addition to a tabular view DDS also shows visualizations" +
      "of the data. The second argument is optional and determines the sample size.",
    parameters = "rdd: RDD[T], (optional) sampleSize: Int"
  )
  def show[V](rdd: RDD[V], sampleSize: Int)(implicit tag: TypeTag[V]): Unit = {
    val vType = tag.tpe
    if (vType <:< typeOf[Row]) {
      // RDD of rows but w/o a schema
      val values = rdd.take(sampleSize).map(_.asInstanceOf[Row].toSeq)
      table((1 to values.head.size).map(_.toString), values)
    } else {
      // Normal RDD
      show(rdd.take(sampleSize))(tag)
    }
  }
  def show[V](rdd: RDD[V])(implicit tag: TypeTag[V]): Unit =
    show(rdd, DEFAULT_SHOW_SAMPLE_SIZE)(tag)

  private def createShow(dataFrame: DataFrame, sampleSize: Int): Option[Servable] = {
    val fields = dataFrame.schema.fields
    val nullableColumns = (0 to fields.size - 1).zip(fields).filter {
      case (index, field) => field.nullable
    }.map {
      case (index, nullableField) => index
    }.toSet
    val values = dataFrame.take(sampleSize).map(row =>
      (0 to row.size).zip(row.toSeq).map { case (index, element) =>
        if (nullableColumns.contains(index))
          Option(element)
        else
          element
      }
    )
    val fieldNames = dataFrame.schema.fields.map(field => {
      s"""${field.name} [${field.dataType.toString.replace("Type", "")}${if (field.nullable) "*" else ""}]"""
    })
    createTable(fieldNames, values)
  }

  @Help(
    category = "Spark SQL",
    shortDescription = "Shows the first rows of a DataFrame",
    longDescription = "Shows the first rows of a DataFrame. In addition to a tabular view DDS also shows visualizations" +
      "of the data. The second argument is optional and determines the sample size.",
    parameters = "rdd: DataFrame, (optional) sampleSize: Int"
  )
  def show(dataFrame: DataFrame, sampleSize: Int): Unit =
    serve(createShow(dataFrame, sampleSize))

  def show(dataFrame: DataFrame): Unit =
    show(dataFrame, DEFAULT_SHOW_SAMPLE_SIZE)

  @Help(
    category = "Spark GraphX",
    shortDescription = "Plots a graph based on a vertex sample",
    longDescription = "Plots a sample of a graph layouted by the D3 force layout. The sample is calculated based on a" +
      " vertex sample. All edges which do not have both source and destination in the vertex sample, will be discarded. " +
      "You can also specify a vertex filter to apply before taking the sample.",
    parameters = "graph: Graph[VD, ED], (optional) sampleSize: Int, (optional) vertexFilter: (VertexId, VD) => Boolean"
  )
  def showVertexSample[VD, ED](graph: graphx.Graph[VD, ED],
                               sampleSize: Int = 20,
                               vertexFilter: (VertexId, VD) => Boolean = (id: VertexId, attr: VD) => true): Unit = {
    val vertexSample = graph.vertices.filter{
      case (id, attr) => vertexFilter(id, attr)
    }.take(sampleSize).map{ case (id, attr) => id }.toSet
    val sampledGraph = graph.subgraph(
      edge => vertexSample.contains(edge.srcId) && vertexSample.contains(edge.dstId),
      (vertexId, vertexAttr) => vertexSample.contains(vertexId)
    )
    DDS.graph(sampledGraph.vertices.collect.toSeq, sampledGraph.edges.collect.map(edge => (edge.srcId, edge.dstId, edge.attr)))
  }

  @Help(
    category = "Spark GraphX",
    shortDescription = "Plots a graph based on an edge sample",
    longDescription = "Plots a sample of a graph layouted by the D3 force layout. The sample is calculated based on an" +
      " edge sample. All vertices not being either source or target of these edge are discarded." +
      " You can also specify an edge filter before taking the sample.",
    parameters = "graph: Graph[VD, ED], (optional) sampleSize: Int, (optional) edgeFilter: (Edge[ED]) => Boolean"
  )
  def showEdgeSample[VD, ED](graph: graphx.Graph[VD, ED],
                             sampleSize: Int = 20,
                             edgeFilter: (Edge[ED]) => Boolean = (edge: Edge[ED]) => true): Unit = {
    val edgeSample = graph.edges.filter(edgeFilter).take(sampleSize)
    val verticesToKeep = edgeSample.map(_.srcId).toSet ++ edgeSample.map(_.dstId).toSet
    val vertexSample = graph.vertices.filter{ case (id, attr) => verticesToKeep.contains(id) }.collect
    DDS.graph(vertexSample, edgeSample.map(edge => (edge.srcId, edge.dstId, edge.attr)))
  }

  @Help(
    category = "Spark GraphX",
    shortDescription = "Plots some statistics about the connected components of a graph",
    longDescription = "Plots the vertex and edge count of all connected components in the given graph.",
    parameters = "graph: Graph[VD, ED]"
  )
  def connectedComponents[VD: ClassTag, ED: ClassTag](graph: graphx.Graph[VD, ED]): Unit = {
    val connectedComponents = graph.connectedComponents()
    val vertexCounts = connectedComponents.vertices.map{
      case (id, connectedComponent) => (connectedComponent, 1)
    }.reduceByKey(_ + _)
    val edgeCounts = connectedComponents.edges.map(e => (e.srcId, 1)).join(
      connectedComponents.vertices
    ).map{
      case (id, (count, connectedComponent)) => (connectedComponent, count)
    }.reduceByKey(_ + _)
    val counts = vertexCounts.leftOuterJoin(edgeCounts)
    table(
      List("Connected Component", "#Vertices", "#Edges"),
      counts.map{ case (connectedComponent, (numVertices, numEdges)) =>
        List(connectedComponent, numVertices, numEdges.getOrElse(0))
      }.collect
    )
  }

  private def createCorrelation(dataFrame: DataFrame): Option[Servable] = {
    def showError = println("Correlation only supported for RDDs with multiple numerical columns.")
    val schema = dataFrame.schema
    val fields = schema.fields
    if (fields.size >= 2) {
      val numericalFields = fields.zipWithIndex.filter{ case (field, idx) => {
        val dataType = field.dataType
        (dataType == DoubleType || dataType == FloatType || dataType == IntegerType || dataType == LongType)
      }}
      val numericalFieldIndexes = numericalFields.map{ case (field, idx) => idx }.toSet
      if (numericalFields.size >= 2) {
        val corrAgg = dataFrame.rdd.aggregate(new CorrelationAggregator((numericalFields.size))) (
          (agg, row) => {
            val numericalCells = row.toSeq.zipWithIndex.filter{ case (element, idx) => numericalFieldIndexes.contains(idx) }
            val numericalValues = numericalCells.zip(numericalFields).map {
              case ((element, elementIdx), (elementType, typeIdx)) => {
                require(elementIdx == typeIdx, s"Element index ($elementIdx) did not equal type index ($typeIdx)")
                val dataType = elementType.dataType
                if (element == null)
                  Option.empty[Double]
                else
                  Option[Double](
                    if (dataType == DoubleType) element.asInstanceOf[Double]
                    else if (dataType == FloatType) element.asInstanceOf[Float].toDouble
                    else if (dataType == IntegerType) element.asInstanceOf[Int].toDouble
                    else if (dataType == LongType) element.asInstanceOf[Long].toDouble
                    else element.toString.toDouble // fall back, should not happen
                  )
              }
            }
            agg.iterate(numericalValues)
          },
          (agg1, agg2) => agg1.merge(agg2)
        )
        var corrMatrix: mutable.Seq[mutable.Seq[Double]] = new ArrayBuffer(corrAgg.numColumns) ++
          List.fill(corrAgg.numColumns)(new ArrayBuffer[Double](corrAgg.numColumns) ++
            List.fill(corrAgg.numColumns)(0d))
        for (((i, j), corr) <- corrAgg.pearsonCorrelations) {
          corrMatrix(i)(j) = corr
        }
        val fieldNames = numericalFields.map{ case (field, idx) => field.name }
        createHeatmap(corrMatrix, fieldNames, fieldNames)
      } else {
        showError
        Option.empty
      }
    } else {
      showError
      Option.empty
    }
  }

  @Help(
    category = "Spark SQL",
    shortDescription = "Computes pearson correlation between numerical columns",
    longDescription = "Computes pearson correlation between numerical columns. There need to be at least two numerical," +
      " non-nullable columns in the table. The columns must not be nullable.",
    parameters = "dataFrame: DataFrame"
  )
  def correlation(dataFrame: DataFrame): Unit = {
    serve(createCorrelation(dataFrame))
  }

  def createMutualInformation(dataFrame: DataFrame): Option[Servable] = {
    def showError = println("Mutual information only supported for RDDs with at least one column.")
    val schema = dataFrame.schema
    val fields = schema.fields
    if (fields.size >= 1) {
      val corrAgg = dataFrame.rdd.aggregate(new MutualInformationAggregator(fields.size)) (
        (agg, row) => agg.iterate(row.toSeq),
        (agg1, agg2) => agg1.merge(agg2)
      )
      var mutualInformationMatrix: mutable.Seq[mutable.Seq[Double]] = new ArrayBuffer(corrAgg.numColumns) ++
        List.fill(corrAgg.numColumns)(new ArrayBuffer[Double](corrAgg.numColumns) ++
          List.fill(corrAgg.numColumns)(0d))
      for (((i, j), corr) <- corrAgg.mutualInformation) {
        mutualInformationMatrix(i)(j) = corr
      }
      val fieldNames = fields.map(_.name)
      createHeatmap(mutualInformationMatrix, fieldNames, fieldNames)
    } else {
      showError
      Option.empty
    }
  }

  @Help(
    category = "Spark SQL",
    shortDescription = "Computes mutual information between columns",
    longDescription = "Computes mutual information between columns. It will treat all columns as nominal variables and " +
      "thus not work well with real numerical data. Internally it uses the natural logarithm.",
    parameters = "dataFrame: DataFrame"
  )
  def mutualInformation(dataFrame: DataFrame) = {
    serve(createMutualInformation(dataFrame))
  }

  @Help(
    category = "Spark SQL",
    shortDescription = "Calculates the median of a numeric dataset",
    longDescription = "Calculates the median of a numeric dataset. " +
      "Note that this operation requires ordering of the elements in each partition plus lookup operations, " +
      "which makes it rather expensive.",
    parameters = "values: RDD[NumericValue]"
  )
  def median[N: ClassTag](values: RDD[N])(implicit num: Numeric[N]): Unit = {
    val sorted = values.sortBy(identity).zipWithIndex().map{
      case (v, idx) => (idx, v)
    }
    val count = sorted.count
    if (count > 0) {
      val median: Double = if (count % 2 == 0) {
        val r = count / 2
        val l = r - 1
        num.toDouble(num.plus(sorted.lookup(l).head, sorted.lookup(r).head)) * 0.5
      } else {
        num.toDouble(sorted.lookup(count / 2).head)
      }
      table(List("median"), List(List(median)))
    } else {
      println("Median is not defined on an empty RDD!")
    }
  }

  private def createSummarize[N: ClassTag](values: RDD[N])(implicit num: Numeric[N] = null): Option[Servable] = {
    if (num != null) {
      Option(Table.fromStatCounter(values.stats()))
    } else {
      val cardinality = values.distinct.count
      if (cardinality > 0) {
        val valueCounts = values.map((_, 1)).reduceByKey(_ + _)
        val (mode, modeCount) = valueCounts.max()(Ordering.by { case (value, count) => count})
        createTable(
          List("mode", "cardinality"),
          List(List(mode, cardinality))
        )
      } else {
        println("Summarize function requires a non-empty RDD!")
        Option.empty
      }
    }
  }

  @Help(
    category = "Spark Core",
    shortDescription = "Shows some basic summary statistics of the given dataset",
    longDescription = "Shows some basic summary statistics of the given dataset.\n" +
      "Statistics for numeric values are: count, sum, min, max, mean, stdev, variance\n" +
      "Statistics for nominal values are: mode, cardinality",
    parameters = "values: RDD[NumericValue]"
  )
  def summarize[N: ClassTag](values: RDD[N])(implicit num: Numeric[N] = null): Unit = {
    serve(createSummarize(values))
  }

  @Help(
    category = "Spark Core",
    shortDescription = "Shows some basic summary statistics of the given groups",
    longDescription = "Shows some basic summary statistics of the given groups. " +
      "Statistics are: count, sum, min, max, mean, stdev, variance.",
    parameters = "groupedValues: RDD[(Key, Iterable[NumericValue])]"
  )
  def summarizeGroups[K, N](groupValues: RDD[(K, Iterable[N])])(implicit num: Numeric[N]): Unit = {
    val statCounters = groupValues.map{ case (key, values) =>
      (key, StatCounter(values.map(num.toDouble(_))))
    }.map{ case (key, stat) =>
      (key.toString, stat)
    }.collect
    val (labels, stats) = statCounters.unzip
    table(Table.fromStatCounters(labels, stats))
  }

  @Help(
    category = "Spark Core",
    shortDescription = "Shows some basic summary statistics of the given groups",
    longDescription = "Shows some basic summary statistics of the given groups. " +
      "Statistics are: count, sum, min, max, mean, stdev, variance.",
    parameters = "toBeGroupedValues: RDD[(Key, NumericValue)]"
  )
  def groupAndSummarize[K: ClassTag, N: ClassTag](toBeGroupedValues: RDD[(K, N)])(implicit num: Numeric[N]): Unit = {
    summarizeGroups(toBeGroupedValues.groupByKey())
  }

  @Help(
    category = "Spark SQL",
    shortDescription = "Gives an overview of the given data set in form of a dashboard.",
    longDescription = "Gives an overview of the given data set in form of a dashboard. " +
      "The dashboard contains a sample, measures for column dependencies and summary statistics for each column. " +
      "It might make sense to cache the data set before passing it to this function.",
    parameters = "dataFrame: DataFrame"
  )
  def dashboard(dataFrame: DataFrame): Unit = {
    val rdd = dataFrame.rdd
    val columnTypes = dataFrame.schema.fields

    val summaries = for (i <- 0 to dataFrame.columns.size - 1; columnType = columnTypes(i))
      yield {
        def createSummarizeOf[T: ClassTag](implicit num: Numeric[T] = null) = {
          val column = rdd.flatMap(row => {
            Option(row(i)).map(_.asInstanceOf[T])
          })
          createSummarize(column)
        }
        columnType.dataType match {
          case DoubleType => createSummarizeOf[Double]
          case IntegerType => createSummarizeOf[Int]
          case FloatType => createSummarizeOf[Float]
          case LongType => createSummarizeOf[Long]
          case default => createSummarizeOf[Any]
        }
      }

    def toCell(maybeServable: Option[Servable]) = maybeServable.map(servable => List(servable)).getOrElse(List.empty)
    serve(CompositeServable(List(
      toCell(createShow(dataFrame, DEFAULT_SHOW_SAMPLE_SIZE)),
      toCell(createCorrelation(dataFrame)) ++ toCell(createMutualInformation(dataFrame))
    ) ++ summaries.map(summary => toCell(summary))))
  }

}
