package de.frosner.dds.core

import de.frosner.dds.analytics.{ColumnsStatisticsAggregator, CorrelationAggregator, DateColumnStatisticsAggregator, MutualInformationAggregator}
import de.frosner.dds.servables.composite.{EmptyServable, CompositeServable}
import de.frosner.dds.servables.histogram.Histogram
import de.frosner.dds.servables.tabular.{KeyValueSequence, Table}
import de.frosner.dds.util.DataFrameUtils._
import org.apache.log4j.Logger
import org.apache.spark.graphx
import org.apache.spark.graphx.{Edge, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
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

  {
    ManifestMetaData.logWelcomeMessage()
  }

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

  @Help(
    category = "Scala",
    shortDescription = "Plots a graph",
    longDescription = "Plots a graph layouted by the D3 force layout.",
    parameters = "vertices: Seq[(VertexId, Label)], edges: Seq[(SourceVertexId, TargetVertexId, Label)]"
  )
  def graph[ID, VL, EL](vertices: Seq[(ID, VL)], edges: Iterable[(ID, ID, EL)]): Unit = {
    serve(ScalaFunctions.createGraph(vertices, edges))
  }

  @Help(
    category = "Scala",
    shortDescription = "Prints a key value pair list",
    longDescription = "Prints a key value pair list.",
    parameters = "pairs: Seq[(Key, Value)]"
  )
  def keyValuePairs(pairs: List[(Any, Any)]): Unit = {
    serve(ScalaFunctions.createKeyValuePairs(pairs, ""))
  }

  @Help(
    category = "Scala",
    shortDescription = "Plots a scatter plot",
    longDescription = "Plots a scatter plot of the given points. A point is represented as a pair of X and Y coordinates." +
      "Works with both, numeric or nominal values and will plot the axes accordingly.",
    parameters = "values: Seq[(Value, Value)]"
  )
  def scatter[N1, N2](values: Seq[(N1, N2)])(implicit num1: Numeric[N1] = null, num2: Numeric[N2] = null) = {
    serve(ScalaFunctions.createScatter(values)(num1, num2))
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
    serve(ScalaFunctions.createHeatmap(values, rowNames, colNames)(num))
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
  def histogram[N1, N2](bins: Seq[N1], frequencies: Seq[N2])(implicit num1: Numeric[N1], num2: Numeric[N2]): Unit = {
    serve(ScalaFunctions.createHistogram(bins, frequencies)(num1, num2))
  }

  @Help(
    category = "Scala",
    shortDescription = "Plots a line chart",
    longDescription = "Plots a line chart visualizing the given value sequence.",
    parameters = "values: Seq[NumericValue]"
  )
  def line[N](values: Seq[N])(implicit num: Numeric[N]) = {
    serve(ScalaFunctions.createLine(values)(num))
  }

  @Help(
    category = "Scala",
    shortDescription = "Plots a line chart with multiple lines",
    longDescription = "Plots a line chart with multiple lines. Each line corresponds to one of the value sequences " +
      "and is labeled according to the labels specified.",
    parameters = "labels: Seq[String], values: Seq[Seq[NumericValue]]"
  )
  def lines[N](labels: Seq[String], values: Seq[Seq[N]])(implicit num: Numeric[N]) = {
    serve(ScalaFunctions.createLines(labels, values)(num))
  }

  private val DEFAULT_BAR_TITLE = "data"

  @Help(
    category = "Scala",
    shortDescription = "Plots a bar chart with an indexed x-axis.",
    longDescription = "Plots a bar chart with an indexed x-axis visualizing the given value sequence.",
    parameters = "values: Seq[NumericValue], (optional) title: String"
  )
  def bar[N](values: Seq[N], title: String)(implicit num: Numeric[N]): Unit = {
    serve(ScalaFunctions.createBar(values, title)(num))
  }

  def bar[N](values: Seq[N])(implicit num: Numeric[N]): Unit =
    bar(values, DEFAULT_BAR_TITLE)

  @Help(
    category = "Scala",
    shortDescription = "Plots a bar chart with a categorical x-axis.",
    longDescription = "Plots a bar chart with a categorical x-axis visualizing the given value sequence.",
    parameters = "values: Seq[NumericValue], categories: Seq[String], (optional) title: String"
  )
  def bar[N](values: Seq[N], categories: Seq[String], title: String)(implicit num: Numeric[N]): Unit = {
    serve(ScalaFunctions.createBar(values, categories, title)(num))
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
    serve(ScalaFunctions.createBars(labels, values)(num))
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
    serve(ScalaFunctions.createBars(labels, values, categories)(num))
  }

  @Help(
    category = "Scala",
    shortDescription = "Plots a pie chart with the given value per group",
    longDescription = "Plots a pie chart with the given value per group. The input must contain each key only once.",
    parameters = "keyValuePairs: Iterable[(Key, NumericValue)]"
  )
  def pie[K, V](keyValuePairs: Iterable[(K, V)])(implicit num: Numeric[V]): Unit = {
    serve(ScalaFunctions.createPie(keyValuePairs)(num))
  }

  @Help(
    category = "Scala",
    shortDescription = "Displays data in tabular format",
    longDescription = "Displays the given rows as a table using the specified head. DDS also shows visualizations of the" +
      "data in the table.",
    parameters = "head: Seq[String], rows: Seq[Seq[Any]]"
  )
  def table(head: Seq[String], rows: Seq[Seq[Any]]): Unit = {
    serve(ScalaFunctions.createTable(head, rows))
  }

  @Help(
    category = "Scala",
    shortDescription = "Shows a sequence",
    longDescription = "Shows a sequence. In addition to a tabular view DDS also shows visualizations" +
      "of the data.",
    parameters = "sequence: Seq[T]"
  )
  def show[V](sequence: Seq[V])(implicit tag: TypeTag[V]): Unit = {
    serve(ScalaFunctions.createShow(sequence)(tag))
  }

  private[core] val DEFAULT_SHOW_SAMPLE_SIZE = 100

  @Help(
    category = "Spark Core",
    shortDescription = "Plots a bar chart with the counts of all distinct values in this RDD",
    longDescription = "Plots a bar chart with the counts of all distinct values in this RDD. This makes most sense for " +
      "non-numeric values that have a relatively low cardinality.",
    parameters = "values: RDD[Value], (optional) title: String"
  )
  def bar[V: ClassTag](values: RDD[V], title: String): Unit = {
    serve(SparkCoreFunctions.createBar(values, title))
  }

  def bar[V: ClassTag](values: RDD[V]): Unit = bar(values, DEFAULT_BAR_TITLE)

  @Help(
    category = "Spark Core",
    shortDescription = "Plots a pie chart with the counts of all distinct values in this RDD",
    longDescription = "Plots a pie chart with the counts of all distinct values in this RDD. This makes most sense for " +
      "non-numeric values that have a relatively low cardinality.",
    parameters = "values: RDD[Value]"
  )
  def pie[V: ClassTag](values: RDD[V]): Unit = {
    serve(SparkCoreFunctions.createPie(values))
  }

  @Help(
    category = "Spark Core",
    shortDescription = "Plots a histogram of a numerical RDD for the given number of buckets",
    longDescription = "Plots a histogram of a numerical RDD for the given number of buckets. " +
      "The number of buckets parameter is optional - if omitted, Sturge's formula is used to determine an optimal number of bins.",
    parameters = "values: RDD[NumericValue], (optional) numBuckets: Int"
  )
  def histogram[N: ClassTag](values: RDD[N], numBuckets: Int)(implicit num: Numeric[N]): Unit = {
    serve(SparkCoreFunctions.createHistogram(values, Option(numBuckets)))
  }

  def histogram[N: ClassTag](values: RDD[N])(implicit num: Numeric[N]): Unit = {
    serve(SparkCoreFunctions.createHistogram(values, Option.empty))
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
    serve(SparkCoreFunctions.createHistogram(values, buckets))
  }

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
    serve(SparkCoreFunctions.createPieGroups(groupValues)(reduceFunction)(num))
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
    serve(SparkCoreFunctions.createGroupAndPie(toBeGroupedValues)(reduceFunction))
  }

  @Help(
    category = "Spark Core",
    shortDescription = "Shows the first rows of an RDD",
    longDescription = "Shows the first rows of an RDD. In addition to a tabular view DDS also shows visualizations" +
      "of the data. The second argument is optional and determines the sample size.",
    parameters = "rdd: RDD[T], (optional) sampleSize: Int"
  )
  def show[V](rdd: RDD[V], sampleSize: Int)(implicit tag: TypeTag[V]): Unit = {
    serve(SparkCoreFunctions.createShow(rdd, sampleSize)(tag))
  }
  def show[V](rdd: RDD[V])(implicit tag: TypeTag[V]): Unit =
    show(rdd, DEFAULT_SHOW_SAMPLE_SIZE)(tag)

  @Help(
    category = "Spark Core",
    shortDescription = "Calculates the median of a numeric dataset",
    longDescription = "Calculates the median of a numeric dataset. " +
      "Note that this operation requires ordering of the elements in each partition plus lookup operations, " +
      "which makes it rather expensive.",
    parameters = "values: RDD[NumericValue]"
  )
  def median[N: ClassTag](values: RDD[N])(implicit num: Numeric[N]): Unit = {
    serve(SparkCoreFunctions.createMedian(values))
  }

  @Help(
    category = "Spark Core",
    shortDescription = "Shows some basic summary statistics of the given dataset",
    longDescription = "Shows some basic summary statistics of the given dataset.\n" +
      "Statistics for numeric values are: count, sum, min, max, mean, stdev, variance\n" +
      "Statistics for nominal values are: mode, cardinality",
    parameters = "values: RDD[Value]"
  )
  def summarize[N: ClassTag](values: RDD[N])(implicit num: Numeric[N] = null): Unit = {
    serve(SparkCoreFunctions.createSummarize(values))
  }

  @Help(
    category = "Spark Core",
    shortDescription = "Shows some basic summary statistics of the given groups",
    longDescription = "Shows some basic summary statistics of the given groups. " +
      "Statistics are: count, sum, min, max, mean, stdev, variance.",
    parameters = "groupedValues: RDD[(Key, Iterable[NumericValue])]"
  )
  def summarizeGroups[K, N](groupValues: RDD[(K, Iterable[N])])(implicit num: Numeric[N]): Unit = {
    serve(SparkCoreFunctions.createSummarizeGroups(groupValues)(num))
  }

  @Help(
    category = "Spark Core",
    shortDescription = "Shows some basic summary statistics of the given groups",
    longDescription = "Shows some basic summary statistics of the given groups. " +
      "Statistics are: count, sum, min, max, mean, stdev, variance.",
    parameters = "toBeGroupedValues: RDD[(Key, NumericValue)]"
  )
  def groupAndSummarize[K: ClassTag, N: ClassTag](toBeGroupedValues: RDD[(K, N)])(implicit num: Numeric[N]): Unit = {
    serve(SparkCoreFunctions.createGroupAndSummarize(toBeGroupedValues))
  }

  @Help(
    category = "Spark SQL",
    shortDescription = "Plots a bar chart with the counts of all distinct values in this single columned data frame",
    longDescription = "Plots a bar chart with the counts of all distinct values in this single columned data frame. " +
      "This makes most sense for non-numeric values that have a relatively low cardinality. You can also specify an " +
      "optional value to replace missing values with. If no missing value is specified, Scala's Option trait is used.",
    parameters = "dataFrame: DataFrame, (optional) nullValue: Any"
  )
  def bar(dataFrame: DataFrame, nullValue: Any = null): Unit = {
    serve(SparkSqlFunctions.createBar(dataFrame, nullValue))
  }

  @Help(
    category = "Spark SQL",
    shortDescription = "Plots a pie chart with the counts of all distinct values in this single columned data frame",
    longDescription = "Plots a pie chart with the counts of all distinct values in this single columned data frame. " +
      "This makes most sense for non-numeric values that have a relatively low cardinality. You can also specify an " +
      "optional value to replace missing values with. If no missing value is specified, Scala's Option trait is used.",
    parameters = "dataFrame: DataFrame, (optional) nullValue: Any"
  )
  def pie(dataFrame: DataFrame, nullValue: Any = null): Unit = {
    serve(SparkSqlFunctions.createPie(dataFrame, nullValue))
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
    serve(SparkSqlFunctions.createHistogram(dataFrame, buckets))
  }

  @Help(
    category = "Spark SQL",
    shortDescription = "Plots a histogram of a single column data frame for the given number of buckets",
    longDescription = "Plots a histogram of a single column data frame for the given number of buckets. " +
      "The number of buckets parameter is optional - if omitted, Sturge's formula is used to determine an optimal number of bins. " +
      "If the column contains null values, they will be ignored in the computation.",
    parameters = "dataFrame: DataFrame, (optional) numBuckets: Int"
  )
  def histogram(dataFrame: DataFrame, numBuckets: Int): Unit = {
    serve(SparkSqlFunctions.createHistogram(dataFrame, Option(numBuckets)))
  }

  def histogram(dataFrame: DataFrame): Unit = {
    serve(SparkSqlFunctions.createHistogram(dataFrame, Option.empty))
  }

  @Help(
    category = "Spark SQL",
    shortDescription = "Shows the first rows of a DataFrame",
    longDescription = "Shows the first rows of a DataFrame. In addition to a tabular view DDS also shows visualizations" +
      "of the data. The second argument is optional and determines the sample size.",
    parameters = "rdd: DataFrame, (optional) sampleSize: Int"
  )
  def show(dataFrame: DataFrame, sampleSize: Int): Unit =
    serve(SparkSqlFunctions.createShow(dataFrame, sampleSize))

  def show(dataFrame: DataFrame): Unit =
    show(dataFrame, DEFAULT_SHOW_SAMPLE_SIZE)

  @Help(
    category = "Spark SQL",
    shortDescription = "Computes pearson correlation between numerical columns",
    longDescription = "Computes pearson correlation between numerical columns. There need to be at least two numerical," +
      " non-nullable columns in the table. The columns must not be nullable.",
    parameters = "dataFrame: DataFrame"
  )
  def correlation(dataFrame: DataFrame): Unit = {
    serve(SparkSqlFunctions.createCorrelation(dataFrame))
  }

  @Help(
    category = "Spark SQL",
    shortDescription = "Computes mutual information between columns",
    longDescription = "Computes mutual information between columns. It will treat all columns as nominal variables and " +
      "thus not work well with real numerical data. Internally it uses the natural logarithm. It offers the pure " +
      "mutual information as well as a metric variant.\n\n" +
      "Possible normalization options: \"metric\" (default), \"none\".",
    parameters = "dataFrame: DataFrame, (optional) normalization: String"
  )
  def mutualInformation(dataFrame: DataFrame, normalization: String = MutualInformationAggregator.DEFAULT_NORMALIZATION) = {
    serve(SparkSqlFunctions.createMutualInformation(dataFrame, normalization))
  }

  @Help(
    category = "Spark SQL",
    shortDescription = "Calculates the median of a numeric data frame column",
    longDescription = "Calculates the median of a numeric data frame column. " +
      "Note that this operation requires ordering of the elements in each partition plus lookup operations, " +
      "which makes it rather expensive.",
    parameters = "dataFrame: DataFrame"
  )
  def median(dataFrame: DataFrame): Unit = {
    serve(SparkSqlFunctions.createMedian(dataFrame))
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
    serve(SparkSqlFunctions.createDashboard(dataFrame))
  }

  @Help(
    category = "Spark SQL",
    shortDescription = "Gives an overview of the columns of a data frame",
    longDescription = "Gives an overview of the columns of a data frame. " +
      "For each column it will display some summary statistics based on the column data type.",
    parameters = "dataFrame: DataFrame"
  )
  def summarize(dataFrame: DataFrame): Unit = {
    serve(SparkSqlFunctions.createSummarize(dataFrame, Servable.DEFAULT_TITLE))
  }

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

}
