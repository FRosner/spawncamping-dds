package de.frosner.dds.core

import de.frosner.dds.servables.c3.ChartTypeEnum.ChartType
import de.frosner.dds.servables.c3._
import de.frosner.dds.servables.graph.Graph
import de.frosner.dds.servables.histogram.Histogram
import de.frosner.dds.servables.tabular.Table
import org.apache.spark.SparkContext._
import org.apache.spark.graphx
import org.apache.spark.rdd.RDD
import org.apache.spark.util.StatCounter

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

  private val helper = Helper(this.getClass)

  private var servable: Option[Servable] = Option.empty

  private var server: Option[Server] = Option.empty
  private var serverNumber = 1

  private[core] def start(server: Server): Unit = {
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
    shortDescription = "Starts the DDS Web UI bound to the given interface and port",
    longDescription = "Starts the DDS Web UI bound to the given interface and port. You can stop it by calling stop().",
    parameters = "interface: String, port: Int"
  )
  def start(interface: String, port: Int): Unit = {
    start(SprayServer("dds-" + serverNumber, interface = interface, port = port, launchBrowser = true))
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

  private def serve(servable: Servable) = {
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
    category = "Generic Plots",
    shortDescription = "Plots a graph",
    longDescription = "Plots a graph layouted by the D3 force layout.",
    parameters = "vertices: Seq[(VertexId, Label)], edges: Seq[(SourceVertexId, TargetVertexId)]"
  )
  def graph[ID, L](vertices: Seq[(ID, L)], edges: Iterable[(ID, ID)]): Unit = {
    val indexMap = vertices.map{ case (id, label) => id }.zip(0 to vertices.size).toMap
    val graph = Graph(
      vertices.map{ case (id, label) => label.toString},
      edges.map{ case (sourceId, targetId) => (indexMap(sourceId), indexMap(targetId))}
    )
    serve(graph)
  }

  @Help(
    category = "Generic Plots",
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
    category = "Generic Plots",
    shortDescription = "Plots a line chart",
    longDescription = "Plots a line chart visualizing the given value sequence.",
    parameters = "values: Seq[NumericValue]"
  )
  def line[N](values: Seq[N])(implicit num: Numeric[N]) = {
    lines(List("data"), List(values))
  }

  @Help(
    category = "Generic Plots",
    shortDescription = "Plots a line chart with multiple lines",
    longDescription = "Plots a line chart with multiple lines. Each line corresponds to one of the value sequences " +
      "and is labeled according to the labels specified.",
    parameters = "labels: Seq[String], values: Seq[Seq[NumericValue]]"
  )
  def lines[N](labels: Seq[String], values: Seq[Seq[N]])(implicit num: Numeric[N]) = {
    val series = labels.zip(values).map{ case (label, values) => Series(label, values) }
    indexedPlot(series, ChartTypeEnum.Line)
  }

  @Help(
    category = "Generic Plots",
    shortDescription = "Plots a bar chart with an indexed x-axis.",
    longDescription = "Plots a bar chart with an indexed x-axis visualizing the given value sequence.",
    parameters = "values: Seq[NumericValue]"
  )
  def bar[N](values: Seq[N])(implicit num: Numeric[N]) = {
    bars(List("data"), List(values))
  }

  @Help(
    category = "Generic Plots",
    shortDescription = "Plots a bar chart with a categorical x-axis.",
    longDescription = "Plots a bar chart with a categorical x-axis visualizing the given value sequence.",
    parameters = "values: Seq[NumericValue], categories: Seq[String]"
  )
  def bar[N](values: Seq[N], categories: Seq[String])(implicit num: Numeric[N]) = {
    bars(List("data"), List(values), categories)
  }

  @Help(
    category = "Generic Plots",
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
    category = "Generic Plots",
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
    category = "Generic Plots",
    shortDescription = "Plots a pie chart with the given value per group",
    longDescription = "Plots a pie chart with the given value per group. The input must contain each key only once.",
    parameters = "keyValuePairs: Iterable[(Key, NumericValue)]"
  )
  def pie[K, V](keyValuePairs: Iterable[(K, V)])(implicit num: Numeric[V]): Unit = {
    indexedPlot(keyValuePairs.map{ case (key, value) => Series(key.toString, List(value))}, ChartTypeEnum.Pie)
  }

  @Help(
    category = "RDD Analysis",
    shortDescription = "Plots a bar chart with the counts of all distinct values in this RDD",
    longDescription = "Plots a bar chart with the counts of all distinct values in this RDD. This makes most sense for " +
      "non-numeric values that have a relatively low cardinality.",
    parameters = "values: RDD[Value]"
  )
  def bar[V: ClassTag](values: RDD[V]): Unit = {
    val (distinctValues, distinctCounts) =
      values.map((_, 1)).reduceByKey(_ + _).collect.sortBy{ case (value, count) => count }.reverse.unzip
    bar(distinctCounts, distinctValues.map(_.toString))
  }

  @Help(
    category = "RDD Analysis",
    shortDescription = "Plots a pie chart with the counts of all distinct values in this RDD",
    longDescription = "Plots a pie chart with the counts of all distinct values in this RDD. This makes most sense for " +
      "non-numeric values that have a relatively low cardinality.",
    parameters = "values: RDD[Value]"
  )
  def pie[V: ClassTag](values: RDD[V]): Unit = {
    pie(values.map((_, 1)).reduceByKey(_ + _).collect)
  }

  @Help(
    category = "RDD Analysis",
    shortDescription = "Plots a histogram of a numerical RDD for the given number of buckets",
    longDescription = "Plots a histogram of a numerical RDD for the given number of buckets. " +
      "The number of buckets parameter is optional having the default value of 10.",
    parameters = "values: RDD[NumericValue], (optional) numBuckets: Int"
  )
  def histogram[N: ClassTag](values: RDD[N], numBuckets: Int = 10)(implicit num: Numeric[N]): Unit = {
    val (buckets, frequencies) = values.map(v => num.toDouble(v)).histogram(numBuckets)
    histogram(buckets, frequencies)
  }

  @Help(
    category = "RDD Analysis",
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
    category = "RDD Analysis",
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
    category = "RDD Analysis",
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

  @Help(
    category = "Generic Plots",
    shortDescription = "Displays data in tabular format",
    longDescription = "Displays the given rows as a table using the specified head. DDS also shows visualizations of the" +
      "data in the table.",
    parameters = "head: Seq[String], rows: Seq[Seq[Any]]"
  )
  def table(head: Seq[String], rows: Seq[Seq[Any]]): Unit = {
    table(Table(head, rows))
  }

  @Help(
    category = "Generic Plots",
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
      val result = if (vType <:< typeOf[Product] && !(vType <:< typeOf[Option[_]])) {
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

  @Help(
    category = "RDD Analysis",
    shortDescription = "Shows the first rows of an RDD",
    longDescription = "Shows the first rows of an RDD. In addition to a tabular view DDS also shows visualizations" +
      "of the data. The second argument is optional and determines the sample size.",
    parameters = "rdd: RDD[T], (optional) sampleSize: Int"
  )
  def show[V](rdd: RDD[V], sampleSize: Int = 200)(implicit tag: TypeTag[V]): Unit = {
    show(rdd.take(sampleSize))(tag)
  }

  @Help(
    category = "RDD Analysis",
    shortDescription = "Plots a sample of a graph",
    longDescription = "Plots a sample of a graph layouted by the D3 force layout. The sample is calculated based on a" +
      " vertex sample. All edges which do not have both source and destination in the vertex sample, will be discarded.",
    parameters = "graph: Graph, (optional) sampleSize: Int"
  )
  def show[VD, ED](graph: graphx.Graph[VD, ED], sampleSize: Int): Unit = {
    val vertexSample = graph.vertices.take(sampleSize).map{ case (id, attr) => id }.toSet
    val sampledGraph = graph.subgraph(
      edge => vertexSample.contains(edge.srcId) && vertexSample.contains(edge.dstId),
      (vertexId, vertexAttr) => vertexSample.contains(vertexId)
    )
    DDS.graph(sampledGraph.vertices.collect.toSeq, sampledGraph.edges.collect.map(edge => (edge.srcId, edge.dstId)))
  }

  /*
   * Need to manually define default argument sampleSize because of Scala language specs. See:
   * http://stackoverflow.com/questions/4652095/why-does-the-scala-compiler-disallow-overloaded-methods-with-default-arguments
   */
  def show[VD, ED](graph: graphx.Graph[VD, ED]): Unit = show(graph, 20)

    @Help(
    category = "RDD Analysis",
    shortDescription = "Shows some basic summary statistics of the given dataset",
    longDescription = "Shows some basic summary statistics of the given dataset.\n" +
      "Statistics for numeric values are: count, sum, min, max, mean, stdev, variance\n" +
      "Statistics for nominal values are: mode, cardinality",
    parameters = "values: RDD[NumericValue]"
  )
  def summarize[N: ClassTag](values: RDD[N])(implicit num: Numeric[N] = null): Unit = {
    if (num != null) {
      table(Table.fromStatCounter(values.stats()))
    } else {
      val cardinality = values.distinct.count
      val valueCounts = values.map((_, 1)).reduceByKey(_ + _)
      val (mode, modeCount) = valueCounts.max()(Ordering.by{ case (value, count) => count })
      table(
        List("label", "mode", "cardinality"),
        List(List("data", mode, cardinality))
      )
    }
  }

  @Help(
    category = "RDD Analysis",
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
    category = "RDD Analysis",
    shortDescription = "Shows some basic summary statistics of the given groups",
    longDescription = "Shows some basic summary statistics of the given groups. " +
      "Statistics are: count, sum, min, max, mean, stdev, variance.",
    parameters = "toBeGroupedValues: RDD[(Key, NumericValue)]"
  )
  def groupAndSummarize[K: ClassTag, N: ClassTag](toBeGroupedValues: RDD[(K, N)])(implicit num: Numeric[N]): Unit = {
    summarizeGroups(toBeGroupedValues.groupByKey())
  }

}


