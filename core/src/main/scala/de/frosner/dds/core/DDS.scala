package de.frosner.dds.core

import de.frosner.dds.core.Server.TransformedServable
import de.frosner.dds.servables.Servable

import de.frosner.replhelper.{Helper, Help}

import org.apache.log4j.Logger
import org.apache.spark.graphx
import org.apache.spark.graphx.{Edge, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StructType, StringType, StructField}


import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

import de.frosner.dds.analytics.MutualInformationAggregator

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

  private val coreHelper = Helper(this.getClass)
  
  private var serverHelper: Option[Helper[_ <: Server]] = Option.empty

  private var server: Option[Server] = Option.empty

  @Help(
    category = "Servers",
    shortDescription = "Use a given server to serve DDS results",
    longDescription = "Use a given server to serve DDS results. Results from DDS core functions will be processed " +
      "by the specified server. It may have side effects."
  )
  def setServer(server: Server): Unit = {
      logger.debug(s"Attempting to register $server")
    if (this.server.isDefined) {
      println("There is already a server registered! " +
        "Please unregister it before registering a new one using 'unsetServer()'.")
    } else {
      this.server = Some(server)
      serverHelper = Some(Helper(server.getClass))
      this.server.map(_.init())
    }
  }

  @Help(
    category = "Servers",
    shortDescription = "Stop using the current server",
    longDescription = "Stop using the current server. Results from DDS core functions will be returned " +
      "as plain servable objects."
  )
  def unsetServer(): Unit = {
    logger.debug("Attempting to unregister a registered server.")
    if (!server.isDefined) {
      println("No server set. Register one using the 'setServer(server: Server)'.")
    } else {
      val actualServer = server.get
      logger.debug(s"Sending tear down request to server $actualServer")
      actualServer.tearDown()
    }
    resetServers()
  }

  private[core] def resetServers(): Unit = {
    server = None
    serverHelper = None
  }

  @Help(
    category = "Help",
    shortDescription = "Shows available commands",
    longDescription = "Shows all commands available in DDS."
  )
  def help() = {
    val out = System.out
    coreHelper.printAllMethods(out)
    out.println("")
    serverHelper.foreach(_.printAllMethods(out))
  }

  @Help(
    category = "Help",
    shortDescription = "Explains given command",
    longDescription = "Explains the given command.",
    parameters = "commandName: String"
  )
  def help(methodName: String) = {
    val out = System.out
    coreHelper.printMethods(methodName, out)
    serverHelper.foreach(_.printMethods(methodName, out))
  }

  private def serve(maybeServable: Option[Servable]): Option[TransformedServable] =
    maybeServable.map(servable => serve(servable))

  private def serve(servable: Servable): TransformedServable = {
    server.map { server =>
      logger.debug(s"Serving $servable to $server")
      server.serve(servable)
    }.getOrElse {
      logger.debug(s"No server registered to serve $servable to")
      servable
    }
  }

  private def sampleString[T](iterable: Iterable[T], sampleSize: Int): String = {
    s"""(${iterable.take(sampleSize).mkString(", ")}${if (iterable.size > sampleSize) ", ..." else ""})"""
  }

  @Help(
    category = "Scala",
    shortDescription = "Plots a graph",
    longDescription = "Plots a graph layouted by the D3 force layout.",
    parameters = "vertices: Seq[(VertexId, Label)], edges: Seq[(SourceVertexId, TargetVertexId, Label)]"
  )
  def graph[ID, VL, EL](vertices: Seq[(ID, VL)], edges: Iterable[(ID, ID, EL)]): Option[Any] = {
    serve(ScalaFunctions.createGraph(
      vertices = vertices.map{ case (id, label) => (id, label.toString) },
      edges = edges.map{ case (inId, outId, label) => (inId, outId, label.toString) },
      title = s"""Graph ${sampleString(vertices, 1)}"""
    ))
  }

  @Help(
    category = "Scala",
    shortDescription = "Prints a key value pair list",
    longDescription = "Prints a key value pair list.",
    parameters = "pairs: Seq[(Key, Value)]"
  )
  def keyValuePairs[K, V](pairs: List[(K, V)]): Option[Any] = {
    val title = s"Pairs ${sampleString(pairs, 1)}"
    serve(ScalaFunctions.createKeyValuePairs(
      pairs = pairs.map{ case (key, value) => (key.toString, value) },
      title = title
    ))
  }

  @Help(
    category = "Scala",
    shortDescription = "Plots a scatter plot",
    longDescription = "Plots a scatter plot of the given points. A point is represented as a pair of X and Y coordinates." +
      "Works with both, numeric or nominal values and will plot the axes accordingly.",
    parameters = "values: Seq[(Value, Value)]"
  )
  def scatter[N1, N2](values: Seq[(N1, N2)])(implicit num1: Numeric[N1] = null, num2: Numeric[N2] = null): Option[Any] = {
    val (xValues, yValues) = values.unzip
    serve(ScalaFunctions.createScatter(
      values = values,
      title = s"Scatter Plot ${sampleString(values, 1)}"
    ))
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
                (implicit num: Numeric[N]): Option[Any] = {
    serve(ScalaFunctions.createHeatmap(
      values = values.map(_.map(num.toDouble(_))),
      rowNames = rowNames,
      colNames = colNames,
      title = s"Heatmap (${values.size} x ${values.head.size})"
    ))
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
  def histogram[N1, N2](bins: Seq[N1], frequencies: Seq[N2])(implicit num1: Numeric[N1], num2: Numeric[N2]): Option[Any] = {
    serve(ScalaFunctions.createHistogram(
      bins = bins.map(num1.toDouble(_)),
      frequencies = frequencies.map(num2.toLong(_)),
      title = s"Histogram (${bins.size} bins)"
    ))
  }

  @Help(
    category = "Scala",
    shortDescription = "Plots a bar chart with an indexed x-axis.",
    longDescription = "Plots a bar chart with an indexed x-axis visualizing the given value sequence.",
    parameters = "values: Seq[NumericValue], (optional) title: String"
  )
  def bar[N](values: Seq[N])(implicit num: Numeric[N]): Option[Any] = {
    serve(ScalaFunctions.createBar(
      values = values.map(num.toDouble(_)),
      seriesName = "values",
      title = s"Indexed bar chart ${sampleString(values, 2)}"
    ))
  }

  @Help(
    category = "Scala",
    shortDescription = "Plots a bar chart with a categorical x-axis.",
    longDescription = "Plots a bar chart with a categorical x-axis visualizing the given value sequence.",
    parameters = "values: Seq[NumericValue], categories: Seq[String], (optional) title: String"
  )
  def bar[N](values: Seq[N], categories: Seq[String])(implicit num: Numeric[N]): Option[Any] = {
    serve(ScalaFunctions.createBar(
      values = values.map(num.toDouble(_)),
      categories = categories,
      seriesName = "values",
      title = s"Categorical bar chart ${sampleString(categories, 2)}"
    ))
  }

  @Help(
    category = "Scala",
    shortDescription = "Plots a bar chart with an indexed x-axis and multiple bar colors",
    longDescription = "Plots a bar chart with an indexed x-axis and multiple bar colors. " +
      "Each color corresponds to one of the value sequences " +
      "and is labeled according to the labels specified.",
    parameters = "labels: Seq[String], values: Seq[Seq[NumericValue]]"
  )
  def bars[N](labels: Seq[String], values: Seq[Seq[N]])(implicit num: Numeric[N]): Option[Any] = {
    serve(ScalaFunctions.createBars(
      labels = labels,
      values = values.map(_.map(num.toDouble(_))),
      title = s"Indexed multi-bar chart (${labels.size} bars)"
    ))
  }

  @Help(
    category = "Scala",
    shortDescription = "Plots a bar chart with a categorical x-axis and multiple bar colors",
    longDescription = "Plots a bar chart with a categorical x-axis and multiple bar colors. " +
      "Each color corresponds to one of the value sequences " +
      "and is labeled according to the labels specified.",
    parameters = "labels: Seq[String], values: Seq[Seq[NumericValue]], categories: Seq[String]"
  )
  def bars[N](labels: Seq[String], values: Seq[Seq[N]], categories: Seq[String])(implicit num: Numeric[N]): Option[Any] = {
    serve(ScalaFunctions.createBars(
      labels = labels,
      values = values.map(_.map(num.toDouble(_))),
      categories = categories,
      title = s"Categorical multi-bar chart (${labels.size} bars)"
    ))
  }

  @Help(
    category = "Scala",
    shortDescription = "Plots a pie chart with the given value per group",
    longDescription = "Plots a pie chart with the given value per group. The input must contain each key only once.",
    parameters = "keyValuePairs: Iterable[(Key, NumericValue)]"
  )
  def pie[K, V](keyValuePairs: Iterable[(K, V)])(implicit num: Numeric[V]): Option[Any] = {
    serve(ScalaFunctions.createPie(
      keyValuePairs = keyValuePairs.map{ case (key, value) => (key.toString, num.toDouble(value)) },
      title = s"Pie chart (${keyValuePairs.size} values)"
    ))
  }

  @Help(
    category = "Scala",
    shortDescription = "Displays data in tabular format",
    longDescription = "Displays the given rows as a table using the specified column names. " +
      "All cells will be converted to String.",
    parameters = "head: Seq[String], rows: Seq[Seq[Any]]"
  )
  def table(head: Seq[String], rows: Seq[Seq[Any]]): Option[Any] = {
    val schema = StructType(head.map(name => StructField(name, StringType, true)))
    val stringRows = rows.map(_.map(_.toString))
    serve(ScalaFunctions.createTable(schema, stringRows.map(Row(_: _*)), s"Table ${sampleString(head, 2)}"))
  }

  // TODO table method that takes the schema

  @Help(
    category = "Scala",
    shortDescription = "Shows a sequence",
    longDescription = "Shows a sequence. In addition to a tabular view DDS also shows visualizations" +
      "of the data.",
    parameters = "sequence: Seq[T]"
  )
  def show[V](sequence: Seq[V])(implicit tag: TypeTag[V]): Option[Any] = {
    serve(ScalaFunctions.createShow(sequence, s"Table of Seq${sampleString(sequence, 3)}")(tag))
  }

  private[core] val DEFAULT_SHOW_SAMPLE_SIZE = 100

  @Help(
    category = "Spark Core",
    shortDescription = "Plots a bar chart with the counts of all distinct values in this RDD",
    longDescription = "Plots a bar chart with the counts of all distinct values in this RDD. This makes most sense for " +
      "non-numeric values that have a relatively low cardinality.",
    parameters = "values: RDD[Value]"
  )
  def bar[V: ClassTag](values: RDD[V]): Option[Any] = {
    serve(SparkCoreFunctions.createBar(values, "values", s"Bar chart of $values"))
  }

  @Help(
    category = "Spark Core",
    shortDescription = "Plots a pie chart with the counts of all distinct values in this RDD",
    longDescription = "Plots a pie chart with the counts of all distinct values in this RDD. This makes most sense for " +
      "non-numeric values that have a relatively low cardinality.",
    parameters = "values: RDD[Value]"
  )
  def pie[V: ClassTag](values: RDD[V]): Option[Any] = {
    serve(SparkCoreFunctions.createPie(values, s"Pie chart of $values"))
  }

  @Help(
    category = "Spark Core",
    shortDescription = "Plots a histogram of a numerical RDD for the given number of buckets",
    longDescription = "Plots a histogram of a numerical RDD for the given number of buckets. " +
      "The number of buckets parameter is optional - if omitted, Sturge's formula is used to determine an optimal number of bins.",
    parameters = "values: RDD[NumericValue], (optional) numBuckets: Int"
  )
  def histogram[N: ClassTag](values: RDD[N], numBuckets: Int)(implicit num: Numeric[N]): Option[Any] = {
    serve(SparkCoreFunctions.createHistogram(values, Option(numBuckets), s"Histogram of $values ($numBuckets buckets)"))
  }

  def histogram[N: ClassTag](values: RDD[N])(implicit num: Numeric[N]): Option[Any] = {
    serve(SparkCoreFunctions.createHistogram(values, Option.empty, s"Histogram of $values (automatic #buckets)"))
  }

  @Help(
    category = "Spark Core",
    shortDescription = "Plots a histogram of a numerical RDD for the given buckets",
    longDescription = "Plots a histogram of a numerical RDD for the given buckets. " +
      "If the buckets do not include the complete range of possible values, some values will be missing in the histogram.",
    parameters = "values: RDD[NumericValue], buckets: Seq[NumericValue]"
  )
  def histogram[N1, N2](values: RDD[N1], buckets: Seq[N2])
                       (implicit num1: Numeric[N1], num2: Numeric[N2]): Option[Any] = {
    serve(SparkCoreFunctions.createHistogram(
      values = values,
      buckets = buckets.map(num2.toDouble(_)),
      title = s"Histogram of $values (manual ${buckets.size} buckets)"
    ))
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
                     (implicit num: Numeric[N]): Option[Any] = {
    val title = s"Pie chart of $groupValues (already grouped)"
    serve(SparkCoreFunctions.createPieGroups(groupValues, title)(reduceFunction)(num))
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
                                           (implicit num: Numeric[N]): Option[Any] = {
    val title = s"Pie chart of $toBeGroupedValues (to be grouped)"
    serve(SparkCoreFunctions.createGroupAndPie(toBeGroupedValues, title)(reduceFunction))
  }

  @Help(
    category = "Spark Core",
    shortDescription = "Shows the first rows of an RDD",
    longDescription = "Shows the first rows of an RDD. In addition to a tabular view DDS also shows visualizations" +
      "of the data. The second argument is optional and determines the sample size.",
    parameters = "rdd: RDD[T], (optional) sampleSize: Int"
  )
  def show[V](rdd: RDD[V], sampleSize: Int)(implicit tag: TypeTag[V]): Option[Any] = {
    serve(SparkCoreFunctions.createShow(rdd, sampleSize, s"Sample of $rdd ($sampleSize rows)")(tag))
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
  def median[N: ClassTag](values: RDD[N])(implicit num: Numeric[N]): Option[Any] = {
    serve(SparkCoreFunctions.createMedian(values, s"Median of $values"))
  }

  @Help(
    category = "Spark Core",
    shortDescription = "Shows some basic summary statistics of the given dataset",
    longDescription = "Shows some basic summary statistics of the given dataset.\n" +
      "Statistics for numeric values are: count, sum, min, max, mean, stdev, variance\n" +
      "Statistics for nominal values are: mode, cardinality",
    parameters = "values: RDD[Value]"
  )
  def summarize[N: ClassTag](values: RDD[N])(implicit num: Numeric[N] = null): Option[Any] = {
    serve(SparkCoreFunctions.createSummarize(values, s"Summary of $values"))
  }

  @Help(
    category = "Spark Core",
    shortDescription = "Shows some basic summary statistics of the given groups",
    longDescription = "Shows some basic summary statistics of the given groups. " +
      "Statistics are: count, sum, min, max, mean, stdev, variance.",
    parameters = "groupedValues: RDD[(Key, Iterable[NumericValue])]"
  )
  def summarizeGroups[K, N](groupValues: RDD[(K, Iterable[N])])(implicit num: Numeric[N]): Option[Any] = {
    val title = s"Group summary of $groupValues (already grouped)"
    serve(SparkCoreFunctions.createSummarizeGroups(groupValues, title)(num))
  }

  @Help(
    category = "Spark Core",
    shortDescription = "Shows some basic summary statistics of the given groups",
    longDescription = "Shows some basic summary statistics of the given groups. " +
      "Statistics are: count, sum, min, max, mean, stdev, variance.",
    parameters = "toBeGroupedValues: RDD[(Key, NumericValue)]"
  )
  def groupAndSummarize[K: ClassTag, N: ClassTag](toBeGroupedValues: RDD[(K, N)])(implicit num: Numeric[N]): Option[Any] = {
    val title = s"Group summary of $toBeGroupedValues (to be grouped)"
    serve(SparkCoreFunctions.createGroupAndSummarize(toBeGroupedValues, title))
  }

  @Help(
    category = "Spark SQL",
    shortDescription = "Plots a bar chart with the counts of all distinct values in this single columned data frame",
    longDescription = "Plots a bar chart with the counts of all distinct values in this single columned data frame. " +
      "This makes most sense for non-numeric values that have a relatively low cardinality. You can also specify an " +
      "optional value to replace missing values with. If no missing value is specified, Scala's Option trait is used.",
    parameters = "dataFrame: DataFrame, (optional) nullValue: Any"
  )
  def bar(dataFrame: DataFrame, nullValue: Any = null): Option[Any] = {
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
  def pie(dataFrame: DataFrame, nullValue: Any = null): Option[Any] = {
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
  def histogram[N: ClassTag](dataFrame: DataFrame, buckets: Seq[N])(implicit num: Numeric[N]): Option[Any] = {
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
  def histogram(dataFrame: DataFrame, numBuckets: Int): Option[Any] = {
    serve(SparkSqlFunctions.createHistogram(dataFrame, Option(numBuckets)))
  }

  def histogram(dataFrame: DataFrame): Option[Any] = {
    serve(SparkSqlFunctions.createHistogram(dataFrame, Option.empty))
  }

  @Help(
    category = "Spark SQL",
    shortDescription = "Shows the first rows of a DataFrame",
    longDescription = "Shows the first rows of a DataFrame. In addition to a tabular view DDS also shows visualizations" +
      "of the data. The second argument is optional and determines the sample size.",
    parameters = "rdd: DataFrame, (optional) sampleSize: Int"
  )
  def show(dataFrame: DataFrame, sampleSize: Int): Option[Any] =
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
  def correlation(dataFrame: DataFrame): Option[Any] = {
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
  def mutualInformation(dataFrame: DataFrame,
                        normalization: String = MutualInformationAggregator.DEFAULT_NORMALIZATION): Option[Any] = {
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
  def median(dataFrame: DataFrame): Option[Any] = {
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
  def dashboard(dataFrame: DataFrame): Option[Any] = {
    serve(SparkSqlFunctions.createDashboard(dataFrame))
  }

  @Help(
    category = "Spark SQL",
    shortDescription = "Gives an overview of the columns of a data frame",
    longDescription = "Gives an overview of the columns of a data frame. " +
      "For each column it will display some summary statistics based on the column data type.",
    parameters = "dataFrame: DataFrame"
  )
  def summarize(dataFrame: DataFrame): Option[Any] = {
    serve(SparkSqlFunctions.createSummarize(dataFrame))
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
                               vertexFilter: (VertexId, VD) => Boolean = (id: VertexId, attr: VD) => true): Option[Any] = {
    serve(SparkGraphxFunctions.createShowVertexSample(graph, sampleSize, vertexFilter))
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
                             edgeFilter: (Edge[ED]) => Boolean = (edge: Edge[ED]) => true): Option[Any] = {
    serve(SparkGraphxFunctions.createShowEdgeSample(graph, sampleSize, edgeFilter))
  }

  @Help(
    category = "Spark GraphX",
    shortDescription = "Plots some statistics about the connected components of a graph",
    longDescription = "Plots the vertex and edge count of all connected components in the given graph.",
    parameters = "graph: Graph[VD, ED]"
  )
  def connectedComponents[VD: ClassTag, ED: ClassTag](graph: graphx.Graph[VD, ED]): Option[Any] = {
    serve(SparkGraphxFunctions.createConnectedComponents(graph))
  }

}
