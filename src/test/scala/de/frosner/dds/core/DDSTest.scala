package de.frosner.dds.core

import de.frosner.dds.servables.c3._
import de.frosner.dds.servables.graph.Graph
import de.frosner.dds.servables.histogram.Histogram
import de.frosner.dds.servables.matrix.Matrix2D
import de.frosner.dds.servables.scatter.Points2D
import de.frosner.dds.servables.tabular.Table
import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext, graphx}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.test.TestSQLContext
import org.apache.spark.util.StatCounter
import org.apache.spark.sql.{SQLContext, Row}
import org.scalamock.scalatest.MockFactory
import org.scalatest._

class DDSTest extends FlatSpec with Matchers with MockFactory with BeforeAndAfterEach with BeforeAndAfterAll {

  val epsilon = 0.000001
  
  class MockedServer extends Server {
    var lastServed: Option[Servable] = Option.empty
    override def start(): Unit = {}
    override def stop(): Unit = {}
    override def serve(servable: Servable): Unit = lastServed = Option(servable)
  }

  private var stubbedServer: Server = _
  private var mockedServer: MockedServer = _
  private var sc: SparkContext = _
  private var sql: SQLContext = _

  override def beforeAll() = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName(this.getClass.toString)
      .set("spark.driver.allowMultipleContexts", "true")
    sc = new SparkContext(conf)
    sql = new SQLContext(sc)
  }

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
    DDS.graph(List((1, "label1"), (5, "label5")), List((1,1,"a"), (1,5,"b")))

    val actualGraph = mockedServer.lastServed.get.asInstanceOf[Graph]
    actualGraph.vertices.toList shouldBe List("label1", "label5")
    actualGraph.edges.toList shouldBe List((0, 0, "a"), (0, 1, "b"))
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

  "A correct median table" should "be served from an even-sized numerical RDD" in {
    DDS.start(mockedServer)
    val valueRDD = sc.makeRDD(List(1,2,3,4))
    DDS.median(valueRDD)

    val resultTable = mockedServer.lastServed.get.asInstanceOf[Table]
    resultTable.head.toList shouldBe List("median")
    resultTable.rows.toList shouldBe List(List(2.5))
  }

  it should "be served from an odd-sized numerical RDD" in {
    DDS.start(mockedServer)
    val valueRDD = sc.makeRDD(List(1,2,3.3,4,15))
    DDS.median(valueRDD)

    val resultTable = mockedServer.lastServed.get.asInstanceOf[Table]
    resultTable.head.toList shouldBe List("median")
    resultTable.rows.toList shouldBe List(List(3.3))
  }

  it should "be served from a single value numerical RDD" in {
    DDS.start(mockedServer)
    val valueRDD = sc.makeRDD(List(1))
    DDS.median(valueRDD)

    val resultTable = mockedServer.lastServed.get.asInstanceOf[Table]
    resultTable.head.toList shouldBe List("median")
    resultTable.rows.toList shouldBe List(List(1))
  }

  it should "not be served from an empty RDD" in {
    DDS.start(mockedServer)
    val valueRDD = sc.makeRDD(List.empty[Double])
    DDS.median(valueRDD)

    val resultTable = mockedServer.lastServed.isEmpty shouldBe true
  }

  "A correct summary table from a single value RDD" should "be served for numeric values" in {
    DDS.start(mockedServer)
    val valueRdd = sc.makeRDD(List(1,2,3))
    DDS.summarize(valueRdd)

    val counter = StatCounter(1D, 2D, 3D)
    val resultTable = mockedServer.lastServed.get.asInstanceOf[Table]
    resultTable.head.toList shouldBe List("count", "sum", "min", "max", "mean", "stdev", "variance")
    resultTable.rows.toList shouldBe List(
      List(counter.count, counter.sum, counter.min, counter.max, counter.mean, counter.stdev, counter.variance)
    )
  }

  it should "be served for non-numeric values" in {
    DDS.start(mockedServer)
    val valueRdd = sc.makeRDD(List("a", "b", "b", "c"))
    DDS.summarize(valueRdd)

    val resultTable = mockedServer.lastServed.get.asInstanceOf[Table]
    resultTable.head.toList shouldBe List("mode", "cardinality")
    resultTable.rows.toList shouldBe List(
      List("b", 3)
    )
  }

  it should "not be served from an empty RDD" in {
    DDS.start(mockedServer)
    val valueRdd = sc.makeRDD(List.empty[String])
    DDS.summarize(valueRdd)

    val resultTable = mockedServer.lastServed.isEmpty shouldBe true
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

  it should "be printed from generic RDD of collections" in {
    DDS.start(mockedServer)
    val rdd = sc.makeRDD(List(List("a", 1), List("b", 2)))
    DDS.show(rdd)

    val resultTable = mockedServer.lastServed.get.asInstanceOf[Table]
    resultTable.head.toList shouldBe List("sequence")
    resultTable.rows.toList shouldBe List(List(List("a", 1)), List(List("b", 2)))
  }

  it should "be printed from SchemaRDD w/o nullable columns" in {
    DDS.start(mockedServer)
    val rdd = sc.parallelize(List(Row(1, "5", 5d), Row(3, "g", 5d)))
    val schemaRdd = sql.applySchema(rdd, StructType(List(
      StructField("first", IntegerType, false),
      StructField("second", StringType, false),
      StructField("third", DoubleType, false)
    )))
    DDS.show(schemaRdd)
    val resultTable = mockedServer.lastServed.get.asInstanceOf[Table]
    resultTable.head.toList shouldBe List("first [Integer]", "second [String]", "third [Double]")
    resultTable.rows.toList.map(_.toList) shouldBe List(List(1, "5", 5d), List(3, "g", 5d))
  }

  it should "be printed from SchemaRDD w/ nullable columns" in {
    DDS.start(mockedServer)
    val rdd = sc.parallelize(List(Row(null), Row(1)))
    val schemaRdd = sql.applySchema(rdd, StructType(List(
      StructField("first", IntegerType, true)
    )))
    DDS.show(schemaRdd)
    val resultTable = mockedServer.lastServed.get.asInstanceOf[Table]
    resultTable.head.toList shouldBe List("first [Integer*]")
    resultTable.rows.toList.map(_.toList) shouldBe List(List(Option.empty[Int]), List(Option(1)))
  }

  it should "be printed from RDD[Row]" in {
    DDS.start(mockedServer)
    val rdd = sc.parallelize(List(Row(1, "a"), Row(2, "b")))
    DDS.show(rdd)
    val resultTable = mockedServer.lastServed.get.asInstanceOf[Table]
    resultTable.head.toList shouldBe List("1", "2")
    resultTable.rows.toList.map(_.toList) shouldBe List(List(1, "a"), List(2, "b"))
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

  it should "be printed from a sequence of Options" in {
    DDS.start(mockedServer)
    val sequence = List(Option(5), None)
    DDS.show(sequence)

    val resultTable = mockedServer.lastServed.get.asInstanceOf[Table]
    resultTable.head.toList shouldBe List("sequence")
    resultTable.rows.toList shouldBe List(List(Option(5)), List(None))
  }

  "A table" should "print only as many rows as specified" in {
    DDS.start(mockedServer)
    val rdd = sc.makeRDD(List(1,2,3,4,5))
    DDS.show(rdd, 3)

    val resultTable = mockedServer.lastServed.get.asInstanceOf[Table]
    resultTable.head.toList shouldBe List("sequence")
    resultTable.rows.toList shouldBe List(List(1), List(2), List(3))
  }

  "A correct vertex sampled graph" should "be printed when the full graph is taken" in {
    DDS.start(mockedServer)
    val vertices: RDD[(graphx.VertexId, String)] = sc.makeRDD(List((0L, "a"), (5L, "b"), (10L, "c")))
    val edges: RDD[graphx.Edge[String]] = sc.makeRDD(List(graphx.Edge(0L, 5L, "a-b"), graphx.Edge(5L, 10L, "b-c")))
    DDS.showVertexSample(graphx.Graph(vertices, edges), 20)

    val resultGraph = mockedServer.lastServed.get.asInstanceOf[Graph]
    resultGraph.vertices.toSet shouldBe Set("a", "b", "c")
    val vertexList = resultGraph.vertices.toList
    resultGraph.edges.toSet shouldBe Set(
      (vertexList.indexOf("a"), vertexList.indexOf("b"), "a-b"),
      (vertexList.indexOf("b"), vertexList.indexOf("c"), "b-c")
    )
  }

  it should "be printed when a single vertex is taken" in {
    DDS.start(mockedServer)
    val vertices: RDD[(graphx.VertexId, String)] = sc.makeRDD(List((0L, "a"), (5L, "b"), (10L, "c")))
    val edges: RDD[graphx.Edge[String]] = sc.makeRDD(List(graphx.Edge(0L, 5L, "a-b"), graphx.Edge(5L, 10L, "b-c")))
    DDS.showVertexSample(graphx.Graph(vertices, edges), 1)

    val resultGraph = mockedServer.lastServed.get.asInstanceOf[Graph]
    resultGraph.vertices.toSet shouldBe Set("a")
    resultGraph.edges.toSet shouldBe Set.empty
  }

  it should "be printed when a bigger vertex sample is taken" in {
    DDS.start(mockedServer)
    val vertices: RDD[(graphx.VertexId, String)] = sc.makeRDD(List((0L, "a"), (10L, "b"), (5L, "c")))
    val edges: RDD[graphx.Edge[String]] = sc.makeRDD(List(graphx.Edge(0L, 10L, "a-b"), graphx.Edge(5L, 10L, "c-b")))
    DDS.showVertexSample(graphx.Graph(vertices, edges), 2)

    val resultGraph = mockedServer.lastServed.get.asInstanceOf[Graph]
    resultGraph.vertices.toSet shouldBe Set("a", "b")
    val vertexList = resultGraph.vertices.toList
    resultGraph.edges.toSet shouldBe Set(
      (vertexList.indexOf("a"), vertexList.indexOf("b"), "a-b")
    )
  }

  it should "be printed when a vertex sample is taken and a vertex filter is specified" in {
    DDS.start(mockedServer)
    val vertices: RDD[(graphx.VertexId, String)] = sc.makeRDD(List((0L, "a"), (10L, "a"), (5L, "c")))
    val edges: RDD[graphx.Edge[String]] = sc.makeRDD(List(graphx.Edge(5L, 10L, "c-a")))
    DDS.showVertexSample(graphx.Graph(vertices, edges), 3, (id: VertexId, label: String) => label == "a")

    val resultGraph = mockedServer.lastServed.get.asInstanceOf[Graph]
    resultGraph.vertices.toList shouldBe List("a", "a")
    val vertexList = resultGraph.vertices.toList
    resultGraph.edges.toSet shouldBe Set.empty
  }

  "A correct edge sampled graph" should "be printed when the full graph is taken" in {
    DDS.start(mockedServer)
    val vertices: RDD[(graphx.VertexId, String)] = sc.makeRDD(List((0L, "a"), (5L, "b"), (10L, "c")))
    val edges: RDD[graphx.Edge[String]] = sc.makeRDD(List(graphx.Edge(0L, 5L, "a-b"), graphx.Edge(5L, 10L, "b-c")))
    DDS.showEdgeSample(graphx.Graph(vertices, edges), 20)

    val resultGraph = mockedServer.lastServed.get.asInstanceOf[Graph]
    resultGraph.vertices.toSet shouldBe Set("a", "b", "c")
    val vertexList = resultGraph.vertices.toList
    resultGraph.edges.toSet shouldBe Set(
      (vertexList.indexOf("a"), vertexList.indexOf("b"), "a-b"),
      (vertexList.indexOf("b"), vertexList.indexOf("c"), "b-c")
    )
  }

  it should "be printed when a single edge is taken" in {
    DDS.start(mockedServer)
    val vertices: RDD[(graphx.VertexId, String)] = sc.makeRDD(List((0L, "a"), (5L, "b"), (10L, "c")))
    val edges: RDD[graphx.Edge[String]] = sc.makeRDD(List(graphx.Edge(0L, 5L, "a-b"), graphx.Edge(5L, 10L, "b-c")))
    DDS.showEdgeSample(graphx.Graph(vertices, edges), 1)

    val resultGraph = mockedServer.lastServed.get.asInstanceOf[Graph]
    resultGraph.vertices.toSet shouldBe Set("a", "b")
    val vertexList = resultGraph.vertices.toList
    resultGraph.edges.toSet shouldBe Set(
      (vertexList.indexOf("a"), vertexList.indexOf("b"), "a-b")
    )
  }

  it should "be printed when a bigger edge sample is taken" in {
    DDS.start(mockedServer)
    val vertices: RDD[(graphx.VertexId, String)] = sc.makeRDD(List((0L, "a"), (10L, "b"), (5L, "c")))
    val edges: RDD[graphx.Edge[String]] = sc.makeRDD(List(
      graphx.Edge(0L, 10L, "a-b"),
      graphx.Edge(5L, 10L, "c-b"),
      graphx.Edge(0L, 5L, "a-c")
    ))
    DDS.showEdgeSample(graphx.Graph(vertices, edges), 2)

    val resultGraph = mockedServer.lastServed.get.asInstanceOf[Graph]
    resultGraph.vertices.toSet shouldBe Set("a", "b", "c")
    val vertexList = resultGraph.vertices.toList
    resultGraph.edges.toSet == Set(
      (vertexList.indexOf("a"), vertexList.indexOf("b"), "a-b"),
      (vertexList.indexOf("c"), vertexList.indexOf("b"), "c-b")
    ) || resultGraph.edges.toSet == Set(
      (vertexList.indexOf("c"), vertexList.indexOf("b"), "c-b"),
      (vertexList.indexOf("a"), vertexList.indexOf("a"), "a-c")
    ) || resultGraph.edges.toSet == Set(
      (vertexList.indexOf("a"), vertexList.indexOf("b"), "a-b"),
      (vertexList.indexOf("a"), vertexList.indexOf("c"), "a-c")
    ) shouldBe true
  }

  it should "be printed when an all edges are taken and an edge filter is specified" in {
    DDS.start(mockedServer)
    val vertices: RDD[(graphx.VertexId, String)] = sc.makeRDD(List((0L, "b"), (10L, "a"), (5L, "c")))
    val edges: RDD[graphx.Edge[String]] = sc.makeRDD(List(graphx.Edge(5L, 10L, "c-a")))
    DDS.showEdgeSample(graphx.Graph(vertices, edges), 3, (edge: Edge[String]) => edge.srcId == 5L)

    val resultGraph = mockedServer.lastServed.get.asInstanceOf[Graph]
    resultGraph.vertices.toList shouldBe List("a", "c")
    val vertexList = resultGraph.vertices.toList
    resultGraph.edges.toSet shouldBe Set((1, 0, "c-a"))
  }

  it should "be printed when an edges sample is taken and an edge filter is specified" in {
    DDS.start(mockedServer)
    val vertices: RDD[(graphx.VertexId, String)] = sc.makeRDD(List((0L, "b"), (10L, "a"), (5L, "c")))
    val edges: RDD[graphx.Edge[String]] = sc.makeRDD(List(graphx.Edge(5L, 10L, "c-a"), graphx.Edge(10L, 5L, "a-c")))
    DDS.showEdgeSample(graphx.Graph(vertices, edges), 1, (edge: Edge[String]) => edge.srcId == 5L)

    val resultGraph = mockedServer.lastServed.get.asInstanceOf[Graph]
    resultGraph.vertices.toList shouldBe List("a", "c")
    val vertexList = resultGraph.vertices.toList
    resultGraph.edges.toSet == Set((1, 0, "c-a")) || resultGraph.edges.toSet == Set((0, 1, "a-c")) shouldBe true
  }

  "A correct connected components summary" should "be computed for a single connected component" in {
    DDS.start(mockedServer)
    val vertices: RDD[(graphx.VertexId, String)] = sc.makeRDD(List(
      (1L, "a"),
      (2L, "b"),
      (3L, "c"),
      (4L, "d"),
      (5L, "e"),
      (6L, "f")
    ))
    val edges: RDD[graphx.Edge[String]] = sc.makeRDD(List(
      graphx.Edge(1L, 2L, "a-b"),
      graphx.Edge(2L, 3L, "b-c"),
      graphx.Edge(3L, 4L, "c-d"),
      graphx.Edge(5L, 4L, "e-d"),
      graphx.Edge(6L, 5L, "f-e"),
      graphx.Edge(6L, 4L, "f-d")
    ))
    DDS.connectedComponents(graphx.Graph(vertices, edges))

    val resultTable = mockedServer.lastServed.get.asInstanceOf[Table]
    resultTable.head.toList shouldBe List("Connected Component", "#Vertices", "#Edges")
    resultTable.rows.map(_.toList) shouldBe List(
      List(1L, 6, 6)
    )
  }

  it should "be computed for a graph with multiple connected components" in {
    DDS.start(mockedServer)
    val vertices: RDD[(graphx.VertexId, String)] = sc.makeRDD(List(
      (1L, "a"),
      (2L, "b"),
      (3L, "c"),
      (4L, "d"),
      (5L, "e"),
      (6L, "f")
    ))
    val edges: RDD[graphx.Edge[String]] = sc.makeRDD(List(
      graphx.Edge(1L, 2L, "a-b"),
      graphx.Edge(2L, 3L, "b-c"),
      graphx.Edge(5L, 4L, "e-d")
    ))
    DDS.connectedComponents(graphx.Graph(vertices, edges))

    val resultTable = mockedServer.lastServed.get.asInstanceOf[Table]
    resultTable.head.toList shouldBe List("Connected Component", "#Vertices", "#Edges")
    resultTable.rows.map(_.toList).toSet shouldBe Set(
      List(1L, 3, 2),
      List(4L, 2, 1),
      List(6L, 1, 0)
    )
  }

  "A correct scatterplot" should "be constructed from numeric values" in {
    DDS.start(mockedServer)
    val points = List((1,2), (3,4))
    DDS.scatter(points)

    val resultPoints = mockedServer.lastServed.get.asInstanceOf[Points2D[Int, Int]]
    resultPoints.points.toList shouldBe List((1, 2), (3, 4))
  }

  it should "be constructed from nominal values" in {
    DDS.start(mockedServer)
    val points = List(("a",2), ("b",4))
    DDS.scatter(points)

    val resultPoints = mockedServer.lastServed.get.asInstanceOf[Points2D[Int, Int]]
    resultPoints.points.toList shouldBe List(("a", 2), ("b", 4))
  }

  "A correct heatmap" should "be served with default row and column names" in {
    DDS.start(mockedServer)
    DDS.heatmap(List(List(1,2), List(3, 4)))

    val resultMatrix = mockedServer.lastServed.get.asInstanceOf[Matrix2D]
    resultMatrix.entries shouldBe List(List(1,2), List(3,4))
    resultMatrix.rowNames.toList shouldBe List("1", "2")
    resultMatrix.colNames.toList shouldBe List("1", "2")
  }

  it should "be served with with given row and column names" in {
    DDS.start(mockedServer)
    DDS.heatmap(List(List(1,2), List(3, 4)), rowNames = List("a", "b"), colNames = List("c", "d"))

    val resultMatrix = mockedServer.lastServed.get.asInstanceOf[Matrix2D]
    resultMatrix.entries shouldBe List(List(1,2), List(3,4))
    resultMatrix.rowNames.toList shouldBe List("a", "b")
    resultMatrix.colNames.toList shouldBe List("c", "d")
  }

  "A correct correlation heatmap" should "be served from an RDD with two integer columns" in {
    DDS.start(mockedServer)
    val rdd = sc.makeRDD(List(Row(1, 3), Row(2, 2), Row(3, 1)))
    val schemaRdd = sql.applySchema(rdd, StructType(List(
      StructField("first", IntegerType, false),
      StructField("second", IntegerType, false)
    )))
    DDS.correlation(schemaRdd)

    val resultMatrix = mockedServer.lastServed.get.asInstanceOf[Matrix2D]
    resultMatrix.colNames.toList shouldBe List("first", "second")
    resultMatrix.rowNames.toList shouldBe List("first", "second")
    val corrMatrix = resultMatrix.entries.toSeq.map(_.toSeq)
    corrMatrix(0)(0) should be (1d +- epsilon)
    corrMatrix(0)(1) should be (-1d +- epsilon)
    corrMatrix(1)(0) should be (-1d +- epsilon)
    corrMatrix(1)(1) should be (1d +- epsilon)
  }

  it should "not be served from an RDD with not enough numerical columns" in {
    DDS.start(mockedServer)
    val rdd = sc.makeRDD(List(Row(1d, "c"), Row(2d, "a"), Row(3d, "b")))
    val schemaRdd = sql.applySchema(rdd, StructType(List(
      StructField("first", DoubleType, false),
      StructField("second", StringType, false)
    )))
    DDS.correlation(schemaRdd)

    mockedServer.lastServed.isEmpty shouldBe true
  }

  it should "be served from an RDD with two double columns" in {
    DDS.start(mockedServer)
    val rdd = sc.makeRDD(List(Row(1d, 3d), Row(2d, 2d), Row(3d, 1d)))
    val schemaRdd = sql.applySchema(rdd, StructType(List(
      StructField("first", DoubleType, false),
      StructField("second", DoubleType, false)
    )))
    DDS.correlation(schemaRdd)

    val resultMatrix = mockedServer.lastServed.get.asInstanceOf[Matrix2D]
    resultMatrix.colNames.toList shouldBe List("first", "second")
    resultMatrix.rowNames.toList shouldBe List("first", "second")
    val corrMatrix = resultMatrix.entries.toSeq.map(_.toSeq)
    corrMatrix(0)(0) should be (1d +- epsilon)
    corrMatrix(0)(1) should be (-1d +- epsilon)
    corrMatrix(1)(0) should be (-1d +- epsilon)
    corrMatrix(1)(1) should be (1d +- epsilon)
  }

  it should "be served from an RDD with three double columns but ignore the nullable one" in {
    DDS.start(mockedServer)
    val rdd = sc.makeRDD(List(Row(1d, 3d, null), Row(2d, 2d, 2d), Row(3d, 1d, 2d)))
    val schemaRdd = sql.applySchema(rdd, StructType(List(
      StructField("first", DoubleType, false),
      StructField("second", DoubleType, false),
      StructField("third", DoubleType, true)
    )))
    DDS.correlation(schemaRdd)

    val resultMatrix = mockedServer.lastServed.get.asInstanceOf[Matrix2D]
    resultMatrix.colNames.toList shouldBe List("first", "second")
    resultMatrix.rowNames.toList shouldBe List("first", "second")
    val corrMatrix = resultMatrix.entries.toSeq.map(_.toSeq)
    corrMatrix(0)(0) should be (1d +- epsilon)
    corrMatrix(0)(1) should be (-1d +- epsilon)
    corrMatrix(1)(0) should be (-1d +- epsilon)
    corrMatrix(1)(1) should be (1d +- epsilon)
  }

  it should "be served from an RDD containing some non-numerical columns" in {
    DDS.start(mockedServer)
    val rdd = sc.makeRDD(List(Row("a", 1d, true, 3), Row("b", 2d, false, 2), Row("c", 3d, true, 1)))
    val schemaRdd = sql.applySchema(rdd, StructType(List(
      StructField("first", StringType, false),
      StructField("second", DoubleType, false),
      StructField("third", BooleanType, false),
      StructField("fourth", IntegerType, false)
    )))
    DDS.correlation(schemaRdd)

    val resultMatrix = mockedServer.lastServed.get.asInstanceOf[Matrix2D]
    resultMatrix.colNames.toList shouldBe List("second", "fourth")
    resultMatrix.rowNames.toList shouldBe List("second", "fourth")
    val corrMatrix = resultMatrix.entries.toSeq.map(_.toSeq)
    corrMatrix(0)(0) should be (1d +- epsilon)
    corrMatrix(0)(1) should be (-1d +- epsilon)
    corrMatrix(1)(0) should be (-1d +- epsilon)
    corrMatrix(1)(1) should be (1d +- epsilon)
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
