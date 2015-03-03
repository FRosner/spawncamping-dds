package de.frosner.dds.core

import de.frosner.dds.chart.ChartTypeEnum.ChartType
import de.frosner.dds.chart._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.StatCounter

import scala.reflect.runtime.universe._
import scala.reflect.ClassTag

/**
 * Hacks applied here:
 *
 * - ClassTags are needed for conversion to PairRDD
 *   http://mail-archives.apache.org/mod_mbox/incubator-spark-user/201404.mbox/%3CCANGvG8o-EWeETtYb3VGpmSR9ZvJui8vPO-aLsKj7xTMYQgsPAg@mail.gmail.com%3E
 */
object DDS {

  private val helper = Helper(this.getClass)

  private var chart: Option[Servable] = Option.empty

  private var chartServer: Option[ChartServer] = Option.empty
  private var serverNumber = 1

  private[core] def start(server: ChartServer): Unit = {
    if (chartServer.isDefined) {
      println("Server already started! Type 'help()' to see a list of available commands.")
    } else {
      chartServer = Option(server)
      serverNumber += 1
      chartServer.map(_.start())
    }
  }

  @Help(
    category = "Web UI",
    shortDescription = "Starts the DDS Web UI",
    longDescription = "Starts the DDS Web UI bound to the default interface and port. You can stop it by calling stop()."
  )
  def start(): Unit = {
    start(SprayChartServer("dds-" + serverNumber))
  }

  @Help(
    category = "Web UI",
    shortDescription = "Starts the DDS Web UI bound to the given interface and port",
    longDescription = "Starts the DDS Web UI bound to the given interface and port. You can stop it by calling stop().",
    parameters = "interface: String, port: Int"
  )
  def start(interface: String, port: Int): Unit = {
    start(SprayChartServer("dds-" + serverNumber, interface = interface, port = port, launchBrowser = true))
  }

  private[core] def resetServer() = {
    chartServer = Option.empty
  }

  @Help(
    category = "Web UI",
    shortDescription = "Stops the DDS Web UI",
    longDescription = "Stops the DDS Web UI. You can restart it again by calling start()."
  )
  def stop() = {
    if (!chartServer.isDefined) {
      println("No server there to stop! Type 'start()' to start one.")
    } else {
      chartServer.map(_.stop())
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

  private def seriesPlot[N](series: Iterable[Series[N]], chartTypes: ChartTypes)(implicit num: Numeric[N]): Unit = {
    require(series.size == chartTypes.types.size)
    val chart = Chart(SeriesData(series, chartTypes))
    chartServer.map(_.serve(chart))
  }

  private def seriesPlot[N](series: Iterable[Series[N]], chartType: ChartType)(implicit num: Numeric[N]): Unit = {
    seriesPlot(series, ChartTypes.multiple(chartType, series.size))
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
    seriesPlot(series, ChartTypeEnum.Line)
  }

  @Help(
    category = "Generic Plots",
    shortDescription = "Plots a bar chart",
    longDescription = "Plots a bar chart visualizing the given value sequence.",
    parameters = "values: Seq[NumericValue]"
  )
  def bar[N](values: Seq[N])(implicit num: Numeric[N]) = {
    bars(List("data"), List(values))
  }

  @Help(
    category = "Generic Plots",
    shortDescription = "Plots a bar chart with multiple bar colors",
    longDescription = "Plots a bar chart with multiple bar colors. Each color corresponds to one of the value sequences " +
      "and is labeled according to the labels specified.",
    parameters = "labels: Seq[String], values: Seq[Seq[NumericValue]]"
  )
  def bars[N](labels: Seq[String], values: Seq[Seq[N]])(implicit num: Numeric[N]) = {
    val series = labels.zip(values).map{ case (label, values) => Series(label, values) }
    seriesPlot(series, ChartTypeEnum.Bar)
  }

  @Help(
    category = "Generic Plots",
    shortDescription = "Plots a pie chart with the given value per group",
    longDescription = "Plots a pie chart with the given value per group. The input must contain each key only once.",
    parameters = "keyValuePairs: Iterable[(Key, NumericValue)]"
  )
  def pie[K, V](keyValuePairs: Iterable[(K, V)])(implicit num: Numeric[V]) = {
    seriesPlot(keyValuePairs.map{ case (key, value) => Series(key.toString, List(value))}, ChartTypeEnum.Pie)
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
    pie(toBeGroupedValues.reduceByKey(reduceFunction).collect)
  }
  
  private def table(table: Table): Unit = {
    chartServer.map(_.serve(table))
  }

  @Help(
    category = "Generic Plots",
    shortDescription = "Displays a table",
    longDescription = "Displays the given rows as a table using the specified head.",
    parameters = "head: Seq[String], rows: Seq[Seq[Any]]"
  )
  def table(head: Seq[String], rows: Seq[Seq[Any]]): Unit = {
    table(Table(head, rows))
  }

  @Help(
    category = "RDD Analysis",
    shortDescription = "Shows the first rows of an RDD",
    longDescription = "Shows the first rows of an RDD. The second argument is optional and determines the sample size.",
    parameters = "rdd: RDD[T], (optional) sampleSize: Int"
  )
  def show[V](rdd: RDD[V], sampleSize: Int = 20)(implicit tag: TypeTag[V]): Unit = {
    val vType = tag.tpe
    val sample = rdd.take(sampleSize)
    if (sample.length == 0) {
      println("RDD is empty!")
    } else {
      val result = if (vType <:< typeOf[Product]) {
        val header = (1 to sample(0).asInstanceOf[Product].productArity).map("column" + _)
        val rows = sample.map(product => product.asInstanceOf[Product].productIterator.toSeq).toSeq
        Table(header, rows)
      } else {
        Table(List("column1"), sample.map(c => List(c)).toList)
      }
      table(result)
    }
  }

  @Help(
    category = "RDD Analysis",
    shortDescription = "Shows some basic summary statistics of the given dataset",
    longDescription = "Shows some basic summary statistics of the given dataset. " +
      "Statistics are: count, sum, min, max, mean, stdev, variance.",
    parameters = "values: RDD[NumericValue]"
  )
  def summarize[N](values: RDD[N])(implicit num: Numeric[N]): Unit = {
    table(Table.fromStatCounter(values.stats()))
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

  def main(args: Array[String]): Unit = {
    start()
  }

}


