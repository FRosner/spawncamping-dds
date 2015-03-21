package de.frosner.dds.core

import de.frosner.dds.servables.c3._
import de.frosner.dds.servables.graph.Graph
import de.frosner.dds.servables.histogram.Histogram
import de.frosner.dds.servables.tabular.Table
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.util.StatCounter
import org.scalamock.scalatest.MockFactory
import org.scalatest._

class DDSTest extends FlatSpec with Matchers with MockFactory with BeforeAndAfterEach with BeforeAndAfterAll {
  
  class MockedServer extends Server {
    var lastServed: Option[Servable] = Option.empty
    override def start(): Unit = {}
    override def stop(): Unit = {}
    override def serve(servable: Servable): Unit = lastServed = Option(servable)
  }

  private var stubbedServer: Server = _
  private var mockedServer: MockedServer = _
  private val sc: SparkContext = new SparkContext("local", this.getClass.toString)

  override def afterAll() = {
    sc.stop()
  }

  override def beforeEach() {
    stubbedServer = stub[Server]
    mockedServer = new MockedServer()
  }

  override def afterEach {
    DDS.resetServer()
  }

  "DDS" should "start the chart server when start() is executed" in {
    DDS.start(stubbedServer)
    (stubbedServer.start _).verify().once()
  }

  it should "tear the server down when stop() is executed" in {
    DDS.start(stubbedServer)
    DDS.stop()
    (stubbedServer.stop _).verify().once()
  }

  it should "not start another server if one is started already" in {
    DDS.start(stubbedServer)
    DDS.start(stubbedServer)
    (stubbedServer.start _).verify().once()
  }

  it should "do nothing when stopped the second time" in {
    DDS.start(stubbedServer)
    DDS.stop()
    DDS.stop()
    (stubbedServer.stop _).verify().once()
  }

  it should "print an error message when a chart is served without the server being started" in {
    DDS.line(List(1,2,3))
  }

  "Generic plot functions" should "serve correct pie chart" in {
    DDS.start(stubbedServer)
    DDS.pie(List(
      ("a", 3),
      ("b", 3),
      ("c", 5)
    ))

    val expectedChartTypes = ChartTypes.multiple(ChartTypeEnum.Pie, 3)
    val expectedChartSeries = List(
      Series("a", List(3)),
      Series("b", List(3)),
      Series("c", List(5))
    )
    val expectedChart = Chart(SeriesData(expectedChartSeries, expectedChartTypes))
    (stubbedServer.serve _).verify(expectedChart)
  }

  it should "serve correct indexed line chart with single value sequence" in {
    DDS.start(stubbedServer)
    DDS.line(List(1,2,3))

    val expectedChartType = ChartTypes(ChartTypeEnum.Line)
    val expectedChartSeries = List(Series("data", List(1,2,3)))
    val expectedChart = Chart(SeriesData(expectedChartSeries, expectedChartType))
    (stubbedServer.serve _).verify(expectedChart)
  }

  it should "serve correct indexed line chart with multiple value sequences" in {
    DDS.start(stubbedServer)
    DDS.lines(List("a", "b"), List(List(1,2,3), List(3,2,1)))

    val expectedChartTypes = ChartTypes.multiple(ChartTypeEnum.Line, 2)
    val expectedChartSeries = List(
      Series("a", List(1,2,3)),
      Series("b", List(3,2,1))
    )
    val expectedChart = Chart(SeriesData(expectedChartSeries, expectedChartTypes))
    (stubbedServer.serve _).verify(expectedChart)
  }

  it should "serve correct indexed bar chart with single value sequence" in {
    DDS.start(stubbedServer)
    DDS.bar(List(1,2,3))

    val expectedChartType = ChartTypes(ChartTypeEnum.Bar)
    val expectedChartSeries = List(Series("data", List(1,2,3)))
    val expectedChart = Chart(SeriesData(expectedChartSeries, expectedChartType))
    (stubbedServer.serve _).verify(expectedChart)
  }

  it should "serve correct indexed bar chart with multiple value sequences" in {
    DDS.start(stubbedServer)
    DDS.bars(List("a", "b"), List(List(1,2,3), List(3,2,1)))

    val expectedChartTypes = ChartTypes.multiple(ChartTypeEnum.Bar, 2)
    val expectedChartSeries = List(
      Series("a", List(1,2,3)),
      Series("b", List(3,2,1))
    )
    val expectedChart = Chart(SeriesData(expectedChartSeries, expectedChartTypes))
    (stubbedServer.serve _).verify(expectedChart)
  }

  it should "serve correct categorical bar chart with single value sequence" in {
    DDS.start(stubbedServer)
    DDS.bar(List(1,2,3), List("a", "b", "c"))

    val expectedChartType = ChartTypes(ChartTypeEnum.Bar)
    val expectedChartSeries = List(Series("data", List(1,2,3)))
    val expectedChart = Chart(
      SeriesData(expectedChartSeries, expectedChartType),
      XAxis.categorical(List("a", "b", "c"))
    )
    (stubbedServer.serve _).verify(expectedChart)
  }

  it should "serve correct categorical bar chart with multiple value sequences" in {
    DDS.start(stubbedServer)
    DDS.bars(List("a", "b"), List(List(1,2,3), List(3,2,1)), List("x", "y", "z"))

    val expectedChartTypes = ChartTypes.multiple(ChartTypeEnum.Bar, 2)
    val expectedChartSeries = List(
      Series("a", List(1,2,3)),
      Series("b", List(3,2,1))
    )
    val expectedChart = Chart(
      SeriesData(expectedChartSeries, expectedChartTypes),
      XAxis.categorical(List("x", "y", "z"))
    )
    (stubbedServer.serve _).verify(expectedChart)
  }

  it should "serve a correct table" in {
    DDS.start(stubbedServer)
    DDS.table(List("c1", "c2"), List(List(5, "a"), List(3, "b")))

    (stubbedServer.serve _).verify(Table(List("c1", "c2"), List(List(5, "a"), List(3, "b"))))
  }

  it should "serve a correct histogram" in {
    DDS.start(mockedServer)
    DDS.histogram(List(0, 5, 15), List(3, 8))

    val actualHistogram = mockedServer.lastServed.get.asInstanceOf[Histogram]
    actualHistogram.bins.toList shouldBe List(0.0, 5.0, 15.0)
    actualHistogram.frequencies.toList shouldBe List(3.0, 8.0)
  }

  it should "serve a correct graph" in {
    DDS.start(mockedServer)
    DDS.graph(List((1, "label1"), (5, "label5")), List((1,1), (1,5)))

    val actualGraph = mockedServer.lastServed.get.asInstanceOf[Graph]
    actualGraph.vertices.toList shouldBe List("label1", "label5")
    actualGraph.edges.toList shouldBe List((0, 0), (0, 1))
  }

  "A correct pie chart" should "be served from a single value RDD" in {
    DDS.start(mockedServer)
    val values = sc.makeRDD(List(1,1,1,2,3,3))
    DDS.pie(values)

    val actualChartData = mockedServer.lastServed.get.asInstanceOf[Chart].data.asInstanceOf[SeriesData[Int]]
    actualChartData.series.toList shouldBe List(
      Series("1", List(3)),
      Series("3", List(2)),
      Series("2", List(1))
    )
    actualChartData.types shouldBe ChartTypes.multiple(ChartTypeEnum.Pie, 3)
  }

  "A correct bar chart" should "be served from a single value RDD" in {
    DDS.start(mockedServer)
    val values = sc.makeRDD(List(1,1,1,2,3,3))
    DDS.bar(values)

    val actualChart = mockedServer.lastServed.get.asInstanceOf[Chart]
    actualChart.xAxis shouldBe XAxis.categorical(List("1", "3", "2"))
    val actualChartData = actualChart.data.asInstanceOf[SeriesData[Int]]
    actualChartData.series.toList shouldBe List(
      Series("data", List(3, 2, 1))
    )
    actualChartData.types shouldBe ChartTypes.multiple(ChartTypeEnum.Bar, 1)
  }

  "A correct histogram chart" should "be served from a single numeric value RDD with numBins fixed" in {
    DDS.start(mockedServer)
    val values = sc.makeRDD(List(1,1,1,2,3,3))
    DDS.histogram(values, 4)

    val actualChart = mockedServer.lastServed.get.asInstanceOf[Histogram]
    // Bug in Spark -> 3 bins don't work
    // Histogram(ArrayBuffer(1.0, 1.6666666666666665, 2.333333333333333, 3.0),ArrayBuffer(3.0, 1.0, 0.0))
    actualChart.bins.toList shouldBe(List(1.0, 1.5, 2.0, 2.5, 3.0))
    actualChart.frequencies.toList shouldBe List(3,0,1,2)
  }

  it should "be served from a single numeric value RDD with bins fixed" in {
    DDS.start(mockedServer)
    val values = sc.makeRDD(List(1,1,1,2,3,3))
    DDS.histogram(values, List(0.5, 1.5, 2.5, 3.5))

    val actualChart = mockedServer.lastServed.get.asInstanceOf[Histogram]
    actualChart.bins.toList shouldBe List(0.5, 1.5, 2.5, 3.5)
    actualChart.frequencies.toList shouldBe List(3,1,2)
  }

  "Correct pie chart from RDD after groupBy" should "be served when values are already grouped" in {
    DDS.start(stubbedServer)
    val groupedRdd = sc.makeRDD(List(("a", 1), ("a", 2), ("b", 3), ("c", 5))).groupBy(_._1).
      mapValues(values => values.map{ case (key, value) => value} )
    DDS.pieGroups(groupedRdd)(_ + _)

    val expectedChartTypes = ChartTypes.multiple(ChartTypeEnum.Pie, 3)
    val expectedChartSeries = List(
      Series("a", List(3)),
      Series("b", List(3)),
      Series("c", List(5))
    )
    val expectedChart = Chart(SeriesData(expectedChartSeries, expectedChartTypes))
    (stubbedServer.serve _).verify(expectedChart)
  }

  it should "be served when values are not grouped, yet" in {
    DDS.start(stubbedServer)
    val toBeGroupedRdd = sc.makeRDD(List(("a", 1), ("a", 2), ("b", 3), ("c", 5)))
    DDS.groupAndPie(toBeGroupedRdd)(_ + _)

    val expectedChartTypes = ChartTypes.multiple(ChartTypeEnum.Pie, 3)
    val expectedChartSeries = List(
      Series("a", List(3)),
      Series("b", List(3)),
      Series("c", List(5))
    )
    val expectedChart = Chart(SeriesData(expectedChartSeries, expectedChartTypes))
    (stubbedServer.serve _).verify(expectedChart)
  }

  /**
   * org.apache.spark.util.StatCounter does not have a properly defined equals method -> String comparison
   */
  "Correct summary table from RDD after groupBy" should "be served when values are already grouped" in {
    DDS.start(mockedServer)
    val groupedRdd = sc.makeRDD(List(("a", 1), ("a", 2), ("b", 3), ("c", 5))).groupBy(_._1).
      mapValues(values => values.map{ case (key, value) => value} )
    DDS.summarizeGroups(groupedRdd)

    val aCounter = StatCounter(1D, 2D)
    val bCounter = StatCounter(3D)
    val cCounter = StatCounter(5D)
    val resultTable = mockedServer.lastServed.get.asInstanceOf[Table]
    resultTable.head.toList shouldBe List("label", "count", "sum", "min", "max", "mean", "stdev", "variance")
    resultTable.rows.toList shouldBe List(
      List("a", aCounter.count, aCounter.sum, aCounter.min, aCounter.max, aCounter.mean, aCounter.stdev, aCounter.variance),
      List("b", bCounter.count, bCounter.sum, bCounter.min, bCounter.max, bCounter.mean, bCounter.stdev, bCounter.variance),
      List("c", cCounter.count, cCounter.sum, cCounter.min, cCounter.max, cCounter.mean, cCounter.stdev, cCounter.variance)
    )
  }

  it should "be served when values are not grouped, yet" in {
    DDS.start(mockedServer)
    val toBeGroupedRdd = sc.makeRDD(List(("a", 1), ("a", 2), ("b", 3), ("c", 5)))
    DDS.groupAndSummarize(toBeGroupedRdd)

    val aCounter = StatCounter(1D, 2D)
    val bCounter = StatCounter(3D)
    val cCounter = StatCounter(5D)
    val resultTable = mockedServer.lastServed.get.asInstanceOf[Table]
    resultTable.head.toList shouldBe List("label", "count", "sum", "min", "max", "mean", "stdev", "variance")
    resultTable.rows.toList shouldBe List(
      List("a", aCounter.count, aCounter.sum, aCounter.min, aCounter.max, aCounter.mean, aCounter.stdev, aCounter.variance),
      List("b", bCounter.count, bCounter.sum, bCounter.min, bCounter.max, bCounter.mean, bCounter.stdev, bCounter.variance),
      List("c", cCounter.count, cCounter.sum, cCounter.min, cCounter.max, cCounter.mean, cCounter.stdev, cCounter.variance)
    )
  }

  "A correct summary table from a single value RDD" should "be served for numeric values" in {
    DDS.start(mockedServer)
    val valueRdd = sc.makeRDD(List(1,2,3))
    DDS.summarize(valueRdd)

    val counter = StatCounter(1D, 2D, 3D)
    val resultTable = mockedServer.lastServed.get.asInstanceOf[Table]
    resultTable.head.toList shouldBe List("label", "count", "sum", "min", "max", "mean", "stdev", "variance")
    resultTable.rows.toList shouldBe List(
      List("data", counter.count, counter.sum, counter.min, counter.max, counter.mean, counter.stdev, counter.variance)
    )
  }

  it should "be served for non-numeric values" in {
    DDS.start(mockedServer)
    val valueRdd = sc.makeRDD(List("a", "b", "b", "c"))
    DDS.summarize(valueRdd)

    val resultTable = mockedServer.lastServed.get.asInstanceOf[Table]
    resultTable.head.toList shouldBe List("label", "mode", "cardinality")
    resultTable.rows.toList shouldBe List(
      List("data", "b", 3)
    )
  }

  "A correct table" should "be printed from generic RDD of single values" in {
    DDS.start(mockedServer)
    val rdd = sc.makeRDD(List(1))
    DDS.show(rdd)

    val resultTable = mockedServer.lastServed.get.asInstanceOf[Table]
    resultTable.head.toList shouldBe List("sequence")
    resultTable.rows.toList shouldBe List(List(1))
  }

  it should "be printed from generic RDD of tuples" in {
    DDS.start(mockedServer)
    val rdd = sc.makeRDD(List(("a", 1), ("b", 2)))
    DDS.show(rdd)

    val resultTable = mockedServer.lastServed.get.asInstanceOf[Table]
    resultTable.head.toList shouldBe List("1", "2")
    resultTable.rows.toList shouldBe List(List("a", 1), List("b", 2))
  }

  it should "be printed from generic RDD of case classes" in {
    DDS.start(mockedServer)
    val rdd = sc.makeRDD(List(DummyCaseClass("a", 1), DummyCaseClass("b", 2)))
    DDS.show(rdd)

    val resultTable = mockedServer.lastServed.get.asInstanceOf[Table]
    resultTable.head.toList shouldBe List("arg1", "arg2")
    resultTable.rows.toList shouldBe List(List("a", 1), List("b", 2))
  }

  it should "be printed from a sequence of single values" in {
    DDS.start(mockedServer)
    val sequence = List(1, 2)
    DDS.show(sequence)

    val resultTable = mockedServer.lastServed.get.asInstanceOf[Table]
    resultTable.head.toList shouldBe List("sequence")
    resultTable.rows.toList shouldBe List(List(1), List(2))
  }

  it should "be printed from a sequence of tuples" in {
    DDS.start(mockedServer)
    val sequence = List(("a", 1), ("b", 2))
    DDS.show(sequence)

    val resultTable = mockedServer.lastServed.get.asInstanceOf[Table]
    resultTable.head.toList shouldBe List("1", "2")
    resultTable.rows.toList shouldBe List(List("a", 1), List("b", 2))
  }

  it should "be printed from a sequence of case classes" in {
    DDS.start(mockedServer)
    val sequence = List(DummyCaseClass("a", 1), DummyCaseClass("b", 2))
    DDS.show(sequence)

    val resultTable = mockedServer.lastServed.get.asInstanceOf[Table]
    resultTable.head.toList shouldBe List("arg1", "arg2")
    resultTable.rows.toList shouldBe List(List("a", 1), List("b", 2))
  }

  "A table" should "print only as many rows as specified" in {
    DDS.start(mockedServer)
    val rdd = sc.makeRDD(List(1,2,3,4,5))
    DDS.show(rdd, 3)

    val resultTable = mockedServer.lastServed.get.asInstanceOf[Table]
    resultTable.head.toList shouldBe List("sequence")
    resultTable.rows.toList shouldBe List(List(1), List(2), List(3))
  }

  "Help" should "work" in {
    DDS.help()
    DDS.help("start")
  }

}

/**
 * Needs to be defined top level in order to have a [[scala.reflect.runtime.universe.TypeTag]].
 */
private [core] case class DummyCaseClass(arg1: String, arg2: Int)
