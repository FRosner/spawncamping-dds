package de.frosner.dds.core

import java.sql.Date

import de.frosner.dds.analytics.MutualInformationAggregator
import de.frosner.dds.servables._
import de.frosner.replhelper.Help
import org.apache.spark.graphx.{Edge, VertexId}
import org.apache.spark.{SparkConf, SparkContext, graphx}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.util.StatCounter
import org.apache.spark.sql.{SQLContext, Row}
import org.scalamock.scalatest.MockFactory
import org.scalatest._

class DDSTest extends FlatSpec with Matchers with MockFactory with BeforeAndAfterEach with BeforeAndAfterAll {

  val epsilon = 0.000001
  
  class MockedServer extends Server {
    var lastServed: Option[Servable] = Option.empty
    @Help(
      category = "MockedHelp",
      shortDescription = "Short Mocked Description",
      longDescription = "Long Mocked Description"
    )
    def mockedMethodWithHelp: Unit = {}
    override def init(): Unit = {}
    override def tearDown(): Unit = {}
    override def serve(servable: Servable): Any = lastServed = Option(servable); lastServed
  }

  private var stubbedServer: Server = _
  private var mockedServer: MockedServer = _

  private val sc: SparkContext = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName(this.getClass.toString)
    new SparkContext(conf)
  }
  private val sql: SQLContext = new SQLContext(sc)

  override def afterAll() = {
    sc.stop()
  }

  override def beforeEach() {
    stubbedServer = stub[Server]
    mockedServer = new MockedServer()
  }

  override def afterEach {
    DDS.resetServers()
  }

  "DDS" should "start the chart server when start() is executed" in {
    DDS.setServer(stubbedServer)
    (stubbedServer.init _).verify().once()
  }

  it should "tear the server down when stop() is executed" in {
    DDS.setServer(stubbedServer)
    DDS.unsetServer()
    (stubbedServer.tearDown _).verify().once()
  }

  it should "not start another server if one is started already" in {
    DDS.setServer(stubbedServer)
    DDS.setServer(stubbedServer)
    (stubbedServer.init _).verify().once()
  }

  it should "do nothing when stopped the second time" in {
    DDS.setServer(stubbedServer)
    DDS.unsetServer()
    DDS.unsetServer()
    (stubbedServer.tearDown _).verify().once()
  }

  it should "return the unchanged servable if no server is registered" in {
    val actualChart = DDS.pie(List(("a", 3))).get

    val expectedChart = PieChart(
      "Pie chart (1 values)",
      List(("a", 3d))
    )

    actualChart shouldBe expectedChart
  }

  it should "return the transformed servable if a server is registered" in {
    def transformation(servable: Servable) = servable.getClass.getSimpleName
    class Transformer extends Server {
      override def init(): Unit = {}

      override def serve(servable: Servable): Any = transformation(servable)

      override def tearDown(): Unit = {}
    }

    DDS.setServer(new Transformer())
    val actualServable = DDS.pie(List(("a", 3))).get

    val expectedChart = PieChart(
      "Pie chart (1 values)",
      List(("a", 3d))
    )

    actualServable shouldBe transformation(expectedChart)
  }

  it should "return no servable if preconditions of the method are not met" in {
    val values = sc.makeRDD(List(Row(1, 1), Row(1, 1), Row(1, 1), Row(null, null), Row(3, 3), Row(3, 3)))
    val schema = StructType(List(StructField("values", IntegerType, true), StructField("values", IntegerType, true)))
    val dataFrame = sql.createDataFrame(values, schema)
    DDS.pie(dataFrame) shouldBe None
  }

  "Help" should "print all DDS methods with @Help" in {
    DDS.help()
    DDS.help("start")
  }

  it should "print all server methods with @Help" in {
    DDS.setServer(mockedServer)
    DDS.help()
    DDS.help("mockedMethodWithHelp")
  }

  "Generic plot functions" should "serve correct pie chart" in {
    DDS.setServer(stubbedServer)
    DDS.pie(List(
      ("a", 3),
      ("b", 3),
      ("c", 5)
    ))

    val expectedChart = PieChart(
      "Pie chart (3 values)",
      List(("a", 3d), ("b", 3d), ("c", 5d))
    )
    (stubbedServer.serve _).verify(expectedChart)
  }

  it should "serve correct indexed bar chart with single value sequence" in {
    DDS.setServer(mockedServer)
    DDS.bar(List(1,2,3))

    val actualChart = mockedServer.lastServed.get.asInstanceOf[BarChart]
    actualChart.title shouldBe "Indexed bar chart (1, 2, ...)"
    actualChart.xDomain shouldBe Seq("1", "2", "3")
    actualChart.heights shouldBe List(List(1, 2, 3))
    actualChart.series shouldBe List("values")
  }

  it should "serve correct indexed bar chart with multiple value sequences" in {
    DDS.setServer(stubbedServer)
    DDS.bars(List("a", "b"), List(List(1,2,3), List(3,2,1)))

    val expectedChart = BarChart(
      title = "Indexed multi-bar chart (2 bars)",
      xDomain = List("1", "2", "3"),
      heights = List(List(1, 2, 3), List(3, 2, 1)),
      series = List("a", "b")
    )
    (stubbedServer.serve _).verify(expectedChart)
  }

  it should "serve correct categorical bar chart with single value sequence" in {
    DDS.setServer(stubbedServer)
    DDS.bar(List(1,2,3), List("a", "b", "c"))

    val expectedChart = BarChart(
      title = "Categorical bar chart (a, b, ...)",
      xDomain = List("a", "b", "c"),
      heights = List(List(1, 2, 3)),
      series = List("values")
    )
    (stubbedServer.serve _).verify(expectedChart)
  }

  it should "serve correct categorical bar chart with multiple value sequences" in {
    DDS.setServer(stubbedServer)
    DDS.bars(List("a", "b"), List(List(1,2,3), List(3,2,1)), List("x", "y", "z"))

    val expectedChart = BarChart(
      title = "Categorical multi-bar chart (2 bars)",
      xDomain = List("x", "y", "z"),
      heights = List(List(1, 2, 3), List(3, 2, 1)),
      series = List("a", "b")
    )
    (stubbedServer.serve _).verify(expectedChart)
  }

  it should "serve a correct table" in {
    DDS.setServer(stubbedServer)
    DDS.table(List("c1", "c2"), List(List(5, "a"), List(3, "b")))

    val expectedTable = Table(
      title = "Table (c1, c2)",
      schema = StructType(List(
        StructField("c1", StringType, true),
        StructField("c2", StringType, true)
      )),
      content = List(
        Row("5", "a"),
        Row("3", "b")
      )
    )
    (stubbedServer.serve _).verify(expectedTable)
  }

  it should "serve a correct histogram" in {
    DDS.setServer(mockedServer)
    DDS.histogram(List(0, 5, 15), List(3, 8))

    val actualHistogram = mockedServer.lastServed.get.asInstanceOf[Histogram]
    actualHistogram.title shouldBe "Histogram (3 bins)"
    actualHistogram.bins shouldBe Seq(0.0, 5.0, 15.0)
    actualHistogram.frequencies shouldBe Seq(3.0, 8.0)
  }

  it should "serve a correct graph" in {
    DDS.setServer(mockedServer)
    DDS.graph(List((1, "label1"), (5, "label5")), List((1, 1, "a"), (1, 5, "b")))

    val actualGraph = mockedServer.lastServed.get.asInstanceOf[Graph]
    actualGraph.title shouldBe "Graph ((1,label1), ...)"
    actualGraph.vertices shouldBe Seq("label1", "label5")
    actualGraph.edges shouldBe Seq((0, 0, "a"), (0, 1, "b"))
  }

  "A correct pie chart" should "be served from a single value RDD" in {
    DDS.setServer(mockedServer)
    val values = sc.makeRDD(List(1, 1, 1, 2, 3, 3))
    DDS.pie(values)

    val actualPieChart = mockedServer.lastServed.get.asInstanceOf[PieChart]
    actualPieChart.title shouldBe s"Pie chart of $values"
    actualPieChart.categoryCountPairs shouldBe Seq(
      ("1", 3d),
      ("3", 2d),
      ("2", 1d)
    )
  }

  it should "be served from a single columned non-null numerical data frame" in {
    DDS.setServer(mockedServer)
    val values = sc.makeRDD(List(Row(1), Row(1), Row(1), Row(2), Row(3), Row(3)))
    val schema = StructType(List(StructField("column1", IntegerType, false)))
    val dataFrame = sql.createDataFrame(values, schema)
    DDS.pie(dataFrame)

    val actualPieChart = mockedServer.lastServed.get.asInstanceOf[PieChart]
    actualPieChart.title shouldBe "Pie chart of column1"
    actualPieChart.categoryCountPairs shouldBe Seq(
      ("1", 3d),
      ("3", 2d),
      ("2", 1d)
    )
  }

  it should "be served from a single columned non-null string data frame" in {
    DDS.setServer(mockedServer)
    val values = sc.makeRDD(List(Row("a"), Row("a"), Row("a"), Row("b"), Row("c"), Row("c")))
    val schema = StructType(List(StructField("values", StringType, false)))
    val dataFrame = sql.createDataFrame(values, schema)
    DDS.pie(dataFrame)

    val actualPieChart = mockedServer.lastServed.get.asInstanceOf[PieChart]
    actualPieChart.title shouldBe "Pie chart of values"
    actualPieChart.categoryCountPairs.toSet shouldBe Set(
      ("a", 3),
      ("c", 2),
      ("b", 1)
    )
  }

  it should "be served from a single columned nullable data frame" in {
    DDS.setServer(mockedServer)
    val values = sc.makeRDD(List(Row(1), Row(1), Row(1), Row(null), Row(3), Row(3)))
    val schema = StructType(List(StructField("values", IntegerType, true)))
    val dataFrame = sql.createDataFrame(values, schema)
    DDS.pie(dataFrame)

    val actualPieChart = mockedServer.lastServed.get.asInstanceOf[PieChart]
    actualPieChart.title shouldBe "Pie chart of values"
    actualPieChart.categoryCountPairs.toSet shouldBe Set(
        ("Some(1)", 3d),
        ("Some(3)", 2d),
        ("None", 1d)
    )
  }

  it should "be served from a single columned data frame with a custom null value" in {
    DDS.setServer(mockedServer)
    val values = sc.makeRDD(List(Row(1), Row(1), Row(1), Row(null), Row(3), Row(3)))
    val schema = StructType(List(StructField("values", IntegerType, true)))
    val dataFrame = sql.createDataFrame(values, schema)
    DDS.pie(dataFrame, "NA")

    val actualPieChart = mockedServer.lastServed.get.asInstanceOf[PieChart]
    actualPieChart.title shouldBe "Pie chart of values"
    actualPieChart.categoryCountPairs.toSet shouldBe Set(
        ("1", 3d),
        ("3", 2d),
        ("NA", 1d)
    )
  }

  it should "not be served from a multi columned data frame" in {
    DDS.setServer(mockedServer)
    val values = sc.makeRDD(List(Row(1, 1), Row(1, 1), Row(1, 1), Row(null, null), Row(3, 3), Row(3, 3)))
    val schema = StructType(List(StructField("values", IntegerType, true), StructField("values", IntegerType, true)))
    val dataFrame = sql.createDataFrame(values, schema)
    DDS.pie(dataFrame)

    val actualChart = mockedServer.lastServed.isDefined shouldBe false
  }

  "A correct bar chart" should "be served from a single value RDD" in {
    DDS.setServer(mockedServer)
    val values = sc.makeRDD(List(1,1,1,2,3,3))
    DDS.bar(values)

    val actualChart = mockedServer.lastServed.get.asInstanceOf[BarChart]
    actualChart.title shouldBe s"Bar chart of $values"
    actualChart.series shouldBe List("values")
    actualChart.xDomain shouldBe List("1", "3", "2")
    actualChart.heights shouldBe List(List(3d, 2d, 1d))
  }

  it should "be served from a single columned non-null numerical data frame" in {
    DDS.setServer(mockedServer)
    val values = sc.makeRDD(List(Row(1), Row(1), Row(1), Row(2), Row(3), Row(3)))
    val schema = StructType(List(StructField("values", IntegerType, false)))
    val dataFrame = sql.createDataFrame(values, schema)
    DDS.bar(dataFrame)

    val actualChart = mockedServer.lastServed.get.asInstanceOf[BarChart]
    actualChart.title shouldBe s"Bar chart of values"
    actualChart.series shouldBe List("values")
    actualChart.xDomain shouldBe List("1", "3", "2")
    actualChart.heights shouldBe List(List(3d, 2d, 1d))
  }

  it should "be served from a single columned non-null string data frame" in {
    DDS.setServer(mockedServer)
    val values = sc.makeRDD(List(Row("a"), Row("a"), Row("a"), Row("b"), Row("c"), Row("c")))
    val schema = StructType(List(StructField("values", StringType, false)))
    val dataFrame = sql.createDataFrame(values, schema)
    DDS.bar(dataFrame)

    val actualChart = mockedServer.lastServed.get.asInstanceOf[BarChart]
    actualChart.title shouldBe s"Bar chart of values"
    actualChart.series shouldBe List("values")
    actualChart.xDomain shouldBe List("a", "c", "b")
    actualChart.heights shouldBe List(List(3, 2, 1))
  }

  it should "be served from a single columned nullable data frame" in {
    DDS.setServer(mockedServer)
    val values = sc.makeRDD(List(Row(1), Row(1), Row(1), Row(null), Row(3), Row(3)))
    val schema = StructType(List(StructField("values", IntegerType, true)))
    val dataFrame = sql.createDataFrame(values, schema)
    DDS.bar(dataFrame)

    val actualChart = mockedServer.lastServed.get.asInstanceOf[BarChart]
    actualChart.title shouldBe s"Bar chart of values"
    actualChart.series shouldBe List("values")
    actualChart.xDomain shouldBe List("Some(1)", "Some(3)", "None")
    actualChart.heights shouldBe List(List(3d, 2d, 1d))
  }

  it should "be served from a single columned data frame with a custom null value" in {
    DDS.setServer(mockedServer)
    val values = sc.makeRDD(List(Row(1), Row(1), Row(1), Row(null), Row(3), Row(3)))
    val schema = StructType(List(StructField("values", IntegerType, true)))
    val dataFrame = sql.createDataFrame(values, schema)
    DDS.bar(dataFrame, "NA")

    val actualChart = mockedServer.lastServed.get.asInstanceOf[BarChart]
    actualChart.title shouldBe s"Bar chart of values"
    actualChart.series shouldBe List("values")
    actualChart.xDomain shouldBe List("1", "3", "NA")
    actualChart.heights shouldBe List(List(3d, 2d, 1d))
  }

  it should "not be served from a multi columned data frame" in {
    DDS.setServer(mockedServer)
    val values = sc.makeRDD(List(Row(1, 1), Row(1, 1), Row(1, 1), Row(null, null), Row(3, 3), Row(3, 3)))
    val schema = StructType(List(StructField("values", IntegerType, true), StructField("values", IntegerType, true)))
    val dataFrame = sql.createDataFrame(values, schema)
    DDS.bar(dataFrame)

    val actualChart = mockedServer.lastServed.isDefined shouldBe false
  }

  "A correct histogram chart" should "be served from a single numeric value RDD with numBins fixed" in {
    DDS.setServer(mockedServer)
    val values = sc.makeRDD(List(1,1,1,2,3,3))
    DDS.histogram(values, 4)

    val actualChart = mockedServer.lastServed.get.asInstanceOf[Histogram]
    // Bug in Spark 1.2 -> 3 bins don't work
    // Histogram(ArrayBuffer(1.0, 1.6666666666666665, 2.333333333333333, 3.0),ArrayBuffer(3.0, 1.0, 0.0))
    actualChart.title shouldBe s"Histogram of $values (4 buckets)"
    actualChart.bins shouldBe Seq(1.0, 1.5, 2.0, 2.5, 3.0)
    actualChart.frequencies shouldBe Seq(3,0,1,2)
  }

  it should "be served from a single numeric value RDD with bins fixed" in {
    DDS.setServer(mockedServer)
    val values = sc.makeRDD(List(1,1,1,2,3,3))
    DDS.histogram(values, List(0.5, 1.5, 2.5, 3.5))

    val actualChart = mockedServer.lastServed.get.asInstanceOf[Histogram]
    actualChart.title shouldBe s"Histogram of $values (manual 4 buckets)"
    actualChart.bins shouldBe Seq(0.5, 1.5, 2.5, 3.5)
    actualChart.frequencies shouldBe Seq(3,1,2)
  }

  it should "be served from a single column data frame with a non-nullable integer column and fixed bins" in {
    DDS.setServer(mockedServer)
    val values = sc.makeRDD(List(Row(1), Row(1), Row(1), Row(2), Row(3), Row(3)))
    val schema = StructType(List(StructField("values", IntegerType, false)))
    val dataFrame = sql.createDataFrame(values, schema)
    DDS.histogram(dataFrame, List(0.5, 1.5, 2.5, 3.5))

    val actualChart = mockedServer.lastServed.get.asInstanceOf[Histogram]
    actualChart.title shouldBe "Histogram on values"
    actualChart.bins shouldBe Seq(0.5, 1.5, 2.5, 3.5)
    actualChart.frequencies shouldBe Seq(3,1,2)
  }

  it should "be served from a single column data frame with a non-nullable integer column and fixed number of bins" in {
    DDS.setServer(mockedServer)
    val values = sc.makeRDD(List(Row(1), Row(1), Row(1), Row(2), Row(3), Row(3)))
    val schema = StructType(List(StructField("values", IntegerType, false)))
    val dataFrame = sql.createDataFrame(values, schema)
    DDS.histogram(dataFrame, 4)

    val actualChart = mockedServer.lastServed.get.asInstanceOf[Histogram]
    actualChart.title shouldBe "Histogram on values"
    actualChart.bins shouldBe Seq(1.0, 1.5, 2.0, 2.5, 3.0)
    actualChart.frequencies shouldBe Seq(3,0,1,2)
  }

  it should "be served from a single column data frame with a nullable double column and fixed bins" in {
    DDS.setServer(mockedServer)
    val values = sc.makeRDD(List(Row(1d), Row(1d), Row(null), Row(2d), Row(null), Row(3d)))
    val schema = StructType(List(StructField("values", DoubleType, true)))
    val dataFrame = sql.createDataFrame(values, schema)
    DDS.histogram(dataFrame, List(0.5, 1.5, 2.5, 3.5))

    val actualChart = mockedServer.lastServed.get.asInstanceOf[Histogram]
    actualChart.title shouldBe "Histogram on values"
    actualChart.bins shouldBe Seq(0.5, 1.5, 2.5, 3.5)
    actualChart.frequencies shouldBe Seq(2,1,1)
  }

  it should "be served from a single column data frame with a nullable double column and fixed number of bins" in {
    DDS.setServer(mockedServer)
    val values = sc.makeRDD(List(Row(1d), Row(null), Row(1d), Row(2d), Row(null), Row(3d)))
    val schema = StructType(List(StructField("values", DoubleType, true)))
    val dataFrame = sql.createDataFrame(values, schema)
    DDS.histogram(dataFrame, 4)

    val actualChart = mockedServer.lastServed.get.asInstanceOf[Histogram]
    actualChart.title shouldBe s"Histogram on values"
    actualChart.bins shouldBe Seq(1.0, 1.5, 2.0, 2.5, 3.0)
    actualChart.frequencies shouldBe Seq(2,0,1,1)
  }

  it should "be served from a single column data frame with all null values" in {
    DDS.setServer(mockedServer)
    val values = sc.makeRDD(List(Row(null), Row(null), Row(null), Row(null), Row(null), Row(null)))
    val schema = StructType(List(StructField("values", DoubleType, true)))
    val dataFrame = sql.createDataFrame(values, schema)
    DDS.histogram(dataFrame, 4)

    val actualChart = mockedServer.lastServed.isEmpty shouldBe true
  }

  it should "not be served from a data frame with more than one column and fixed bins" in {
    DDS.setServer(mockedServer)
    val values = sc.makeRDD(List(Row(1, "a"), Row(1, "a"), Row(1, "a"), Row(2, "a"), Row(3, "a"), Row(3, "a")))
    val schema = StructType(List(StructField("values", IntegerType, false), StructField("strings", StringType, false)))
    val dataFrame = sql.createDataFrame(values, schema)
    DDS.histogram(dataFrame, List(0.5, 1.5, 2.5, 3.5))

    val actualChart = mockedServer.lastServed.isDefined shouldBe false
  }

  it should "not be served from a data frame with more than one column and fixed number of bins" in {
    DDS.setServer(mockedServer)
    val values = sc.makeRDD(List(Row(1, "a"), Row(1, "a"), Row(1, "a"), Row(2, "a"), Row(3, "a"), Row(3, "a")))
    val schema = StructType(List(StructField("values", IntegerType, false), StructField("strings", StringType, false)))
    val dataFrame = sql.createDataFrame(values, schema)
    DDS.histogram(dataFrame, 3)

    val actualChart = mockedServer.lastServed.isDefined shouldBe false
  }

  it should "not be served when a negative number of bins is specified" in {
    DDS.setServer(mockedServer)
    val values = sc.makeRDD(List(Row(1d), Row(1d), Row(null), Row(2d), Row(null), Row(3d)))
    val schema = StructType(List(StructField("values", DoubleType, true)))
    val dataFrame = sql.createDataFrame(values, schema)
    DDS.histogram(dataFrame, -2)

    val actualChart = mockedServer.lastServed.isDefined shouldBe false
  }

  it should "not be served when the number of bins is not bigger than 1" in {
    DDS.setServer(mockedServer)
    val values = sc.makeRDD(List(Row(1d), Row(1d), Row(null), Row(2d), Row(null), Row(3d)))
    val schema = StructType(List(StructField("values", DoubleType, true)))
    val dataFrame = sql.createDataFrame(values, schema)
    DDS.histogram(dataFrame, 1)

    val actualChart = mockedServer.lastServed.isDefined shouldBe false
  }

  it should "be served from a single numeric value RDD, binned according to Sturge's formula" in {
    DDS.setServer(mockedServer)
    val values = sc.makeRDD(List(0,5,15,3,8))
    DDS.histogram(values)

    val actualChart = mockedServer.lastServed.get.asInstanceOf[Histogram]
    actualChart.title shouldBe s"Histogram of $values (automatic #buckets)"
    actualChart.bins shouldBe Seq(0,3.75,7.5,11.25,15.0)
    actualChart.frequencies shouldBe Seq(2,1,1,1)
  }

  it should "be served from a single numeric column in a data frame, binned according to Sturge's formula" in {
    DDS.setServer(mockedServer)
    val values = sc.makeRDD(List(Row(0), Row(5), Row(15), Row(3), Row(8)))
    val schema = StructType(List(StructField("values", IntegerType, false)))
    val dataFrame = sql.createDataFrame(values, schema)
    DDS.histogram(dataFrame)

    val actualChart = mockedServer.lastServed.get.asInstanceOf[Histogram]
    actualChart.title shouldBe "Histogram on values"
    actualChart.bins shouldBe Seq(0,3.75,7.5,11.25,15.0)
    actualChart.frequencies shouldBe Seq(2,1,1,1)
  }

  "Correct pie chart from RDD after groupBy" should "be served when values are already grouped" in {
    DDS.setServer(mockedServer)
    val groupedRdd = sc.makeRDD(List(("a", 1), ("a", 2), ("b", 3), ("c", 5))).groupBy(_._1).
      mapValues(values => values.map{ case (key, value) => value} )
    DDS.pieGroups(groupedRdd)(_ + _)

    val actualPieChart = mockedServer.lastServed.get.asInstanceOf[PieChart]
    actualPieChart.title shouldBe s"Pie chart of $groupedRdd (already grouped)"
    actualPieChart.categoryCountPairs shouldBe Seq(("a", 3), ("b", 3), ("c", 5))
  }

  it should "be served when values are not grouped, yet" in {
    DDS.setServer(mockedServer)
    val toBeGroupedRdd = sc.makeRDD(List(("a", 1), ("a", 2), ("b", 3), ("c", 5)))
    DDS.groupAndPie(toBeGroupedRdd)(_ + _)

    val actualPieChart = mockedServer.lastServed.get.asInstanceOf[PieChart]
    actualPieChart.title shouldBe s"Pie chart of $toBeGroupedRdd (to be grouped)"
    actualPieChart.categoryCountPairs shouldBe Seq(("a", 3), ("b", 3), ("c", 5))
  }

  "Correct summary table from RDD after groupBy" should "be served when values are already grouped" in {
    DDS.setServer(mockedServer)
    val groupedRdd = sc.makeRDD(List(("a", 1), ("a", 2), ("b", 3), ("c", 5))).groupBy(_._1).
      mapValues(values => values.map{ case (key, value) => value} )
    DDS.summarizeGroups(groupedRdd)

    val aCounter = StatCounter(1D, 2D)
    val bCounter = StatCounter(3D)
    val cCounter = StatCounter(5D)
    val resultTable = mockedServer.lastServed.get.asInstanceOf[Table]
    resultTable.title shouldBe s"Group summary of $groupedRdd (already grouped)"
    resultTable.schema shouldBe StructType(List(
      StructField("label", StringType, false),
      StructField("count", LongType, false),
      StructField("sum", DoubleType, false),
      StructField("min", DoubleType, false),
      StructField("max", DoubleType, false),
      StructField("mean", DoubleType, false),
      StructField("stdev", DoubleType, false),
      StructField("variance", DoubleType, false)
    ))
    resultTable.content shouldBe Seq(
      Row("a", aCounter.count, aCounter.sum, aCounter.min, aCounter.max, aCounter.mean, aCounter.stdev, aCounter.variance),
      Row("b", bCounter.count, bCounter.sum, bCounter.min, bCounter.max, bCounter.mean, bCounter.stdev, bCounter.variance),
      Row("c", cCounter.count, cCounter.sum, cCounter.min, cCounter.max, cCounter.mean, cCounter.stdev, cCounter.variance)
    )
  }

  it should "be served when values are not grouped, yet" in {
    DDS.setServer(mockedServer)
    val toBeGroupedRdd = sc.makeRDD(List(("a", 1), ("a", 2), ("b", 3), ("c", 5)))
    DDS.groupAndSummarize(toBeGroupedRdd)

    val aCounter = StatCounter(1D, 2D)
    val bCounter = StatCounter(3D)
    val cCounter = StatCounter(5D)
    val resultTable = mockedServer.lastServed.get.asInstanceOf[Table]
    resultTable.title shouldBe s"Group summary of $toBeGroupedRdd (to be grouped)"
    resultTable.schema shouldBe StructType(List(
      StructField("label", StringType, false),
      StructField("count", LongType, false),
      StructField("sum", DoubleType, false),
      StructField("min", DoubleType, false),
      StructField("max", DoubleType, false),
      StructField("mean", DoubleType, false),
      StructField("stdev", DoubleType, false),
      StructField("variance", DoubleType, false)
    ))
    resultTable.content shouldBe Seq(
      Row("a", aCounter.count, aCounter.sum, aCounter.min, aCounter.max, aCounter.mean, aCounter.stdev, aCounter.variance),
      Row("b", bCounter.count, bCounter.sum, bCounter.min, bCounter.max, bCounter.mean, bCounter.stdev, bCounter.variance),
      Row("c", cCounter.count, cCounter.sum, cCounter.min, cCounter.max, cCounter.mean, cCounter.stdev, cCounter.variance)
    )
  }

  "A key value pair servable" should "be served from a list of key value pairs" in {
    DDS.setServer(mockedServer)
    DDS.keyValuePairs(List((1, "a"), (2, "b")))

    val result = mockedServer.lastServed.get.asInstanceOf[KeyValueSequence]
    result.title shouldBe "Pairs ((1,a), ...)"
    result.keyValuePairs shouldBe List(("1", "a"), ("2", "b"))
  }

  it should "not be served if the key value sequence is empty" in {
    DDS.setServer(mockedServer)
    DDS.keyValuePairs(List.empty)

    mockedServer.lastServed.isDefined shouldBe false
  }

  "A correct median table" should "be served from an even-sized numerical RDD" in {
    DDS.setServer(mockedServer)
    val valueRDD = sc.makeRDD(List(1,2,3,4))
    DDS.median(valueRDD)

    val resultTable = mockedServer.lastServed.get.asInstanceOf[Table]
    resultTable.title shouldBe s"Median of $valueRDD"
    resultTable.schema shouldBe StructType(List(
      StructField("median", DoubleType, false)
    ))
    resultTable.content shouldBe Seq(Row(2.5))
  }

  it should "be served from an odd-sized numerical RDD" in {
    DDS.setServer(mockedServer)
    val valueRDD = sc.makeRDD(List(1,2,3.3,4,15))
    DDS.median(valueRDD)

    val resultTable = mockedServer.lastServed.get.asInstanceOf[Table]
    resultTable.title shouldBe s"Median of $valueRDD"
    resultTable.schema shouldBe StructType(List(
      StructField("median", DoubleType, false)
    ))
    resultTable.content shouldBe Seq(Row(3.3))
  }

  it should "be served from a single value numerical RDD" in {
    DDS.setServer(mockedServer)
    val valueRDD = sc.makeRDD(List(1))
    DDS.median(valueRDD)

    val resultTable = mockedServer.lastServed.get.asInstanceOf[Table]
    resultTable.title shouldBe s"Median of $valueRDD"
    resultTable.schema shouldBe StructType(List(
      StructField("median", DoubleType, false)
    ))
    resultTable.content shouldBe Seq(Row(1))
  }

  it should "not be served from an empty RDD" in {
    DDS.setServer(mockedServer)
    val valueRDD = sc.makeRDD(List.empty[Double])
    DDS.median(valueRDD)

    val resultTable = mockedServer.lastServed.isEmpty shouldBe true
  }

  it should "be served from a single columned non-null numerical data frame" in {
    DDS.setServer(mockedServer)
    val values = sc.makeRDD(List(Row(1), Row(2), Row(1), Row(1), Row(3)))
    val schema = StructType(List(StructField("values", IntegerType, false)))
    val dataFrame = sql.createDataFrame(values, schema)
    DDS.median(dataFrame)

    val resultTable = mockedServer.lastServed.get.asInstanceOf[Table]
    resultTable.title shouldBe "Median on values"
    resultTable.schema shouldBe StructType(List(
      StructField("median", DoubleType, false)
    ))
    resultTable.content shouldBe Seq(Row(1))
  }

  it should "be served from a single columned nullable numerical data frame" in {
    DDS.setServer(mockedServer)
    val values = sc.makeRDD(List(Row(null), Row(2), Row(null), Row(1), Row(3)))
    val schema = StructType(List(StructField("values", IntegerType, true)))
    val dataFrame = sql.createDataFrame(values, schema)
    DDS.median(dataFrame)

    val resultTable = mockedServer.lastServed.get.asInstanceOf[Table]
    resultTable.title shouldBe "Median on values"
    resultTable.schema shouldBe StructType(List(
      StructField("median", DoubleType, false)
    ))
    resultTable.content shouldBe Seq(Row(2))
  }

  it should "not be served from a single columned nominal data frame" in {
    DDS.setServer(mockedServer)
    val values = sc.makeRDD(List(Row("a"), Row("b"), Row("a"), Row("b"), Row("a")))
    val schema = StructType(List(StructField("values", StringType, false)))
    val dataFrame = sql.createDataFrame(values, schema)
    DDS.median(dataFrame)

    val resultTable = mockedServer.lastServed.isEmpty shouldBe true
  }

  it should "not be served from a multi columned data frame" in {
    DDS.setServer(mockedServer)
    val values = sc.makeRDD(List(Row(1, 1), Row(1, 1), Row(1, 1), Row(null, null), Row(3, 3), Row(3, 3)))
    val schema = StructType(List(StructField("values", IntegerType, true), StructField("values", IntegerType, true)))
    val dataFrame = sql.createDataFrame(values, schema)
    DDS.median(dataFrame)

    val actualChart = mockedServer.lastServed.isDefined shouldBe false
  }

  "A correct summary table from a single value RDD" should "be served for numeric values" in {
    DDS.setServer(mockedServer)
    val valueRdd = sc.makeRDD(List(1,2,3))
    DDS.summarize(valueRdd)

    val counter = StatCounter(1D, 2D, 3D)
    val result = mockedServer.lastServed.get.asInstanceOf[KeyValueSequence]
    result.title shouldBe s"Summary of $valueRdd"
    result.keyValuePairs shouldBe Seq(
      ("Count", counter.count.toString),
      ("Sum", counter.sum.toString),
      ("Min", counter.min.toString),
      ("Max", counter.max.toString),
      ("Mean", counter.mean.toString),
      ("Stdev", counter.stdev.toString),
      ("Variance", counter.variance.toString)
    )
  }

  it should "be served for non-numeric values" in {
    DDS.setServer(mockedServer)
    val valueRdd = sc.makeRDD(List("a", "b", "b", "c"))
    DDS.summarize(valueRdd)

    val result = mockedServer.lastServed.get.asInstanceOf[KeyValueSequence]
    result.title shouldBe s"Summary of $valueRdd"
    result.keyValuePairs shouldBe Seq(
      ("Mode", "b"),
      ("Cardinality", "3")
    )
  }

  it should "not be served from an empty RDD" in {
    DDS.setServer(mockedServer)
    val valueRdd = sc.makeRDD(List.empty[String])
    DDS.summarize(valueRdd)

    val resultTable = mockedServer.lastServed.isEmpty shouldBe true
  }

  "A correct table" should "be printed from generic RDD of single values" in {
    DDS.setServer(mockedServer)
    val rdd = sc.makeRDD(List(1))
    DDS.show(rdd)

    val resultTable = mockedServer.lastServed.get.asInstanceOf[Table]
    resultTable.title shouldBe s"Sample of $rdd (100 rows)"
    resultTable.schema shouldBe StructType(List(StructField("1", IntegerType, false)))
    resultTable.content shouldBe Seq(Row(1))
  }

  it should "be printed from generic RDD of tuples" in {
    DDS.setServer(mockedServer)
    val rdd = sc.makeRDD(List(("a", 1), ("b", 2)))
    DDS.show(rdd)

    val resultTable = mockedServer.lastServed.get.asInstanceOf[Table]
    resultTable.title shouldBe s"Sample of $rdd (100 rows)"
    resultTable.schema shouldBe StructType(List(
      StructField("_1", StringType, true),
      StructField("_2", IntegerType, false)
    ))
    resultTable.content shouldBe Seq(Row("a", 1), Row("b", 2))
  }

  it should "be printed from generic RDD of case classes" in {
    DDS.setServer(mockedServer)
    val rdd = sc.makeRDD(List(DummyCaseClass("a", 1), DummyCaseClass("b", 2)))
    DDS.show(rdd)

    val resultTable = mockedServer.lastServed.get.asInstanceOf[Table]
    resultTable.title shouldBe s"Sample of $rdd (100 rows)"
    resultTable.schema shouldBe StructType(List(
      StructField("arg1", StringType, true),
      StructField("arg2", IntegerType, false)
    ))
    resultTable.content shouldBe Seq(Row("a", 1), Row("b", 2))
  }

  it should "be printed from generic RDD of collections of Any" in {
    DDS.setServer(mockedServer)
    val rdd = sc.makeRDD(List(List("a", 1), List("b", 2)))
    DDS.show(rdd)

    val resultTable = mockedServer.lastServed.get.asInstanceOf[Table]
    resultTable.title shouldBe s"Sample of $rdd (100 rows)"
    resultTable.schema shouldBe StructType(List(
      StructField("1", StringType, true)
    ))
    resultTable.content shouldBe Seq(Row(List("a", 1).toString), Row(List("b", 2).toString))
  }

  it should "be printed from generic RDD of collections of Int" in {
    DDS.setServer(mockedServer)
    val rdd = sc.makeRDD(List(List(1, 2), List(3, 4)))
    DDS.show(rdd)

    val resultTable = mockedServer.lastServed.get.asInstanceOf[Table]
    resultTable.title shouldBe s"Sample of $rdd (100 rows)"
    resultTable.schema shouldBe StructType(List(
      StructField("1", DataTypes.createArrayType(IntegerType, false), true)
    ))
    resultTable.content shouldBe Seq(Row(List(1, 2)), Row(List(3, 4)))
  }

  it should "be printed from SchemaRDD w/o nullable columns" in {
    DDS.setServer(mockedServer)
    val rdd = sc.parallelize(List(Row(1, "5", 5d), Row(3, "g", 5d)))
    val schemaRdd = sql.createDataFrame(rdd, StructType(List(
      StructField("first", IntegerType, false),
      StructField("second", StringType, false),
      StructField("third", DoubleType, false)
    )))
    DDS.show(schemaRdd)
    val resultTable = mockedServer.lastServed.get.asInstanceOf[Table]
    resultTable.title shouldBe s"Sample of first, second, third"
    resultTable.schema shouldBe schemaRdd.schema
    resultTable.content shouldBe Seq(Row(1, "5", 5d), Row(3, "g", 5d))
  }

  it should "be printed from SchemaRDD w/ nullable columns" in {
    DDS.setServer(mockedServer)
    val rdd = sc.parallelize(List(Row(null), Row(1)))
    val schemaRdd = sql.createDataFrame(rdd, StructType(List(
      StructField("first", IntegerType, true)
    )))
    DDS.show(schemaRdd)
    val resultTable = mockedServer.lastServed.get.asInstanceOf[Table]
    resultTable.title shouldBe s"Sample of first"
    resultTable.schema shouldBe schemaRdd.schema
    resultTable.content shouldBe Seq(Row(null), Row(1))
  }

  it should "be printed from RDD[Row]" in {
    DDS.setServer(mockedServer)
    val rdd = sc.parallelize(List(Row(1, "a"), Row(2, "b")))
    DDS.show(rdd)
    val resultTable = mockedServer.lastServed.get.asInstanceOf[Table]
    resultTable.title shouldBe s"Sample of $rdd (100 rows)"
    resultTable.schema shouldBe StructType(List(
      StructField("1", StringType, true)
    ))
    resultTable.content shouldBe Seq(Row(Row(1, "a").toString), Row(Row(2, "b").toString))
  }

  it should "be printed from a sequence of single values" in {
    DDS.setServer(mockedServer)
    val sequence = List(1, 2)
    DDS.show(sequence)

    val resultTable = mockedServer.lastServed.get.asInstanceOf[Table]
    resultTable.title shouldBe s"Table of Seq(1, 2)"
    resultTable.schema shouldBe StructType(List(
      StructField("1", IntegerType, false)
    ))
    resultTable.content shouldBe Seq(Row(1), Row(2))
  }

  it should "be printed from a sequence of tuples" in {
    DDS.setServer(mockedServer)
    val sequence = List(("a", 1), ("b", 2))
    DDS.show(sequence)

    val resultTable = mockedServer.lastServed.get.asInstanceOf[Table]
    resultTable.title shouldBe s"Table of Seq((a,1), (b,2))"
    resultTable.schema shouldBe StructType(List(
      StructField("_1", StringType, true),
      StructField("_2", IntegerType, false)
    ))
    resultTable.content shouldBe Seq(Row("a", 1), Row("b", 2))
  }

  it should "be printed from a sequence of case classes" in {
    DDS.setServer(mockedServer)
    val sequence = List(DummyCaseClass("a", 1), DummyCaseClass("b", 2))
    DDS.show(sequence)

    val resultTable = mockedServer.lastServed.get.asInstanceOf[Table]
    resultTable.title shouldBe s"Table of Seq(DummyCaseClass(a,1), DummyCaseClass(b,2))"
    resultTable.schema shouldBe StructType(List(
      StructField("arg1", StringType, true),
      StructField("arg2", IntegerType, false)
    ))
    resultTable.content shouldBe Seq(Row("a", 1), Row("b", 2))
  }

  it should "be printed from a sequence of Options" in {
    DDS.setServer(mockedServer)
    val sequence = List(Option(5), None)
    DDS.show(sequence)

    val resultTable = mockedServer.lastServed.get.asInstanceOf[Table]
    resultTable.title shouldBe s"Table of Seq(Some(5), None)"
    resultTable.schema shouldBe StructType(List(
      StructField("1", IntegerType, true)
    ))
    resultTable.content shouldBe Seq(Row(Option(5)), Row(None))
  }

  it should "be printed from a sequence of recursive case classes" in {
    DDS.setServer(mockedServer)
    val sequence = List(DummyHierarchicalCaseClass(DummyCaseClass("a", 1), "b"))
    DDS.show(sequence)

    val resultTable = mockedServer.lastServed.get.asInstanceOf[Table]
    resultTable.title shouldBe s"Table of Seq(DummyHierarchicalCaseClass(DummyCaseClass(a,1),b))"
    val innerType = StructType(List(
      StructField("arg1", StringType, true),
      StructField("arg2", IntegerType, false)
    ))
    resultTable.schema shouldBe StructType(List(
      StructField("harg1", innerType, true),
      StructField("harg2", StringType, true)
    ))
    resultTable.content shouldBe Seq(Row(Row("a", 1), "b"))
  }

  "A table" should "print only as many rows as specified" in {
    DDS.setServer(mockedServer)
    val rdd = sc.makeRDD(List(1,2,3,4,5))
    DDS.show(rdd, 3)

    val resultTable = mockedServer.lastServed.get.asInstanceOf[Table]
    resultTable.title shouldBe s"Sample of $rdd (3 rows)"
    resultTable.schema shouldBe StructType(List(
      StructField("1", IntegerType, false)
    ))
    resultTable.content shouldBe Seq(Row(1), Row(2), Row(3))
  }

  "A correct vertex sampled graph" should "be printed when the full graph is taken" in {
    DDS.setServer(mockedServer)
    val vertices: RDD[(graphx.VertexId, String)] = sc.makeRDD(List((0L, "a"), (5L, "b"), (10L, "c")))
    val edges: RDD[graphx.Edge[String]] = sc.makeRDD(List(graphx.Edge(0L, 5L, "a-b"), graphx.Edge(5L, 10L, "b-c")))
    val graph = graphx.Graph(vertices, edges)
    DDS.showVertexSample(graph, 20)

    val resultGraph = mockedServer.lastServed.get.asInstanceOf[Graph]
    resultGraph.title shouldBe s"Vertex sample of $graph"
    val resultVertices = resultGraph.vertices
    resultVertices.toSet shouldBe Set("a", "b", "c")
    val vertexList = resultVertices.toList
    resultGraph.edges.toSet shouldBe Set(
      (vertexList.indexOf("a"), vertexList.indexOf("b"), "a-b"),
      (vertexList.indexOf("b"), vertexList.indexOf("c"), "b-c")
    )
  }

  it should "be printed when a single vertex is taken" in {
    DDS.setServer(mockedServer)
    val vertices: RDD[(graphx.VertexId, String)] = sc.makeRDD(List((0L, "a"), (5L, "b"), (10L, "c")))
    val edges: RDD[graphx.Edge[String]] = sc.makeRDD(List(graphx.Edge(0L, 5L, "a-b"), graphx.Edge(5L, 10L, "b-c")))
    val graph = graphx.Graph(vertices, edges)
    DDS.showVertexSample(graph, 1)

    val resultGraph = mockedServer.lastServed.get.asInstanceOf[Graph]
    resultGraph.title shouldBe s"Vertex sample of $graph"
    resultGraph.vertices.toSet shouldBe Set("a")
    resultGraph.edges.toSet shouldBe Set.empty
  }

  it should "be printed when a bigger vertex sample is taken" in {
    DDS.setServer(mockedServer)
    val vertices: RDD[(graphx.VertexId, String)] = sc.makeRDD(List((0L, "a"), (10L, "b"), (5L, "c")))
    val edges: RDD[graphx.Edge[String]] = sc.makeRDD(List(graphx.Edge(0L, 10L, "a-b"), graphx.Edge(5L, 10L, "c-b")))
    val graph = graphx.Graph(vertices, edges)
    DDS.showVertexSample(graph, 2)

    val resultGraph = mockedServer.lastServed.get.asInstanceOf[Graph]
    resultGraph.title shouldBe s"Vertex sample of $graph"
    resultGraph.vertices.toSet shouldBe Set("a", "b")
    val vertexList = resultGraph.vertices.toList
    resultGraph.edges.toSet shouldBe Set(
      (vertexList.indexOf("a"), vertexList.indexOf("b"), "a-b")
    )
  }

  it should "be printed when a vertex sample is taken and a vertex filter is specified" in {
    DDS.setServer(mockedServer)
    val vertices: RDD[(graphx.VertexId, String)] = sc.makeRDD(List((0L, "a"), (10L, "a"), (5L, "c")))
    val edges: RDD[graphx.Edge[String]] = sc.makeRDD(List(graphx.Edge(5L, 10L, "c-a")))
    val graph = graphx.Graph(vertices, edges)
    DDS.showVertexSample(graph, 3, (id: VertexId, label: String) => label == "a")

    val resultGraph = mockedServer.lastServed.get.asInstanceOf[Graph]
    resultGraph.title shouldBe s"Vertex sample of $graph"
    resultGraph.vertices shouldBe Seq("a", "a")
    val vertexList = resultGraph.vertices.toList
    resultGraph.edges.toSet shouldBe Set.empty
  }

  "A correct edge sampled graph" should "be printed when the full graph is taken" in {
    DDS.setServer(mockedServer)
    val vertices: RDD[(graphx.VertexId, String)] = sc.makeRDD(List((0L, "a"), (5L, "b"), (10L, "c")))
    val edges: RDD[graphx.Edge[String]] = sc.makeRDD(List(graphx.Edge(0L, 5L, "a-b"), graphx.Edge(5L, 10L, "b-c")))
    val graph = graphx.Graph(vertices, edges)
    DDS.showEdgeSample(graph, 20)

    val resultGraph = mockedServer.lastServed.get.asInstanceOf[Graph]
    resultGraph.title shouldBe s"Edge sample of $graph"
    resultGraph.vertices.toSet shouldBe Set("a", "b", "c")
    val vertexList = resultGraph.vertices.toList
    resultGraph.edges.toSet shouldBe Set(
      (vertexList.indexOf("a"), vertexList.indexOf("b"), "a-b"),
      (vertexList.indexOf("b"), vertexList.indexOf("c"), "b-c")
    )
  }

  it should "be printed when a single edge is taken" in {
    DDS.setServer(mockedServer)
    val vertices: RDD[(graphx.VertexId, String)] = sc.makeRDD(List((0L, "a"), (5L, "b"), (10L, "c")))
    val edges: RDD[graphx.Edge[String]] = sc.makeRDD(List(graphx.Edge(0L, 5L, "a-b"), graphx.Edge(5L, 10L, "b-c")))
    val graph = graphx.Graph(vertices, edges)
    DDS.showEdgeSample(graph, 1)

    val resultGraph = mockedServer.lastServed.get.asInstanceOf[Graph]
    resultGraph.title shouldBe s"Edge sample of $graph"
    resultGraph.vertices.toSet shouldBe Set("a", "b")
    val vertexList = resultGraph.vertices.toList
    resultGraph.edges.toSet shouldBe Set(
      (vertexList.indexOf("a"), vertexList.indexOf("b"), "a-b")
    )
  }

  it should "be printed when a bigger edge sample is taken" in {
    DDS.setServer(mockedServer)
    val vertices: RDD[(graphx.VertexId, String)] = sc.makeRDD(List((0L, "a"), (10L, "b"), (5L, "c")))
    val edges: RDD[graphx.Edge[String]] = sc.makeRDD(List(
      graphx.Edge(0L, 10L, "a-b"),
      graphx.Edge(5L, 10L, "c-b"),
      graphx.Edge(0L, 5L, "a-c")
    ))
    val graph = graphx.Graph(vertices, edges)
    DDS.showEdgeSample(graph, 2)

    val resultGraph = mockedServer.lastServed.get.asInstanceOf[Graph]
    resultGraph.title shouldBe s"Edge sample of $graph"
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
    DDS.setServer(mockedServer)
    val vertices: RDD[(graphx.VertexId, String)] = sc.makeRDD(List((0L, "b"), (10L, "a"), (5L, "c")))
    val edges: RDD[graphx.Edge[String]] = sc.makeRDD(List(graphx.Edge(5L, 10L, "c-a")))
    val graph = graphx.Graph(vertices, edges)
    DDS.showEdgeSample(graph, 3, (edge: Edge[String]) => edge.srcId == 5L)

    val resultGraph = mockedServer.lastServed.get.asInstanceOf[Graph]
    resultGraph.title shouldBe s"Edge sample of $graph"
    resultGraph.vertices shouldBe Seq("a", "c")
    val vertexList = resultGraph.vertices.toList
    resultGraph.edges.toSet shouldBe Set((1, 0, "c-a"))
  }

  it should "be printed when an edges sample is taken and an edge filter is specified" in {
    DDS.setServer(mockedServer)
    val vertices: RDD[(graphx.VertexId, String)] = sc.makeRDD(List((0L, "b"), (10L, "a"), (5L, "c")))
    val edges: RDD[graphx.Edge[String]] = sc.makeRDD(List(graphx.Edge(5L, 10L, "c-a"), graphx.Edge(10L, 5L, "a-c")))
    val graph = graphx.Graph(vertices, edges)
    DDS.showEdgeSample(graph, 1, (edge: Edge[String]) => edge.srcId == 5L)

    val resultGraph = mockedServer.lastServed.get.asInstanceOf[Graph]
    resultGraph.title shouldBe s"Edge sample of $graph"
    resultGraph.vertices shouldBe Seq("a", "c")
    val vertexList = resultGraph.vertices.toList
    resultGraph.edges.toSet == Set((1, 0, "c-a")) || resultGraph.edges.toSet == Set((0, 1, "a-c")) shouldBe true
  }

  "A correct connected components summary" should "be computed for a single connected component" in {
    DDS.setServer(mockedServer)
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
    val graph = graphx.Graph(vertices, edges)
    DDS.connectedComponents(graph)

    val resultTable = mockedServer.lastServed.get.asInstanceOf[Table]
    resultTable.title shouldBe s"Connected Components of $graph"
    resultTable.schema shouldBe StructType(List(
      StructField("Connected Component", LongType, false),
      StructField("#Vertices", IntegerType, false),
      StructField("#Edges", IntegerType, false)
    ))
    resultTable.content shouldBe Seq(
      Row(1L, 6, 6)
    )
  }

  it should "be computed for a graph with multiple connected components" in {
    DDS.setServer(mockedServer)
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
    val graph = graphx.Graph(vertices, edges)
    DDS.connectedComponents(graph)

    val resultTable = mockedServer.lastServed.get.asInstanceOf[Table]
    resultTable.title shouldBe s"Connected Components of $graph"
    resultTable.schema shouldBe StructType(List(
      StructField("Connected Component", LongType, false),
      StructField("#Vertices", IntegerType, false),
      StructField("#Edges", IntegerType, false)
    ))
    resultTable.content.size shouldBe 3
    resultTable.content.toSet shouldBe Set(
      Row(1L, 3, 2),
      Row(4L, 2, 1),
      Row(6L, 1, 0)
    )
  }

  "A correct scatterplot" should "be constructed from numeric values" in {
    DDS.setServer(mockedServer)
    val points = List((1,2), (3,4))
    DDS.scatter(points)

    val resultPoints = mockedServer.lastServed.get.asInstanceOf[ScatterPlot]
    resultPoints.title shouldBe s"Scatter Plot ((1,2), ...)"
    resultPoints.points shouldBe Seq((1, 2), (3, 4))
  }

  it should "be constructed from nominal values" in {
    DDS.setServer(mockedServer)
    val points = List(("a",2), ("b",4))
    DDS.scatter(points)

    val resultPoints = mockedServer.lastServed.get.asInstanceOf[ScatterPlot]
    resultPoints.title shouldBe s"Scatter Plot ((a,2), ...)"
    resultPoints.points shouldBe Seq(("a", 2), ("b", 4))
  }

  "A correct heatmap" should "be served with default row and column names" in {
    DDS.setServer(mockedServer)
    DDS.heatmap(List(List(1, 2), List(3, 4)))

    val resultMatrix = mockedServer.lastServed.get.asInstanceOf[Heatmap]
    resultMatrix.title shouldBe s"Heatmap (2 x 2)"
    resultMatrix.content shouldBe List(List(1, 2), List(3, 4))
    resultMatrix.rowNames shouldBe Seq("1", "2")
    resultMatrix.colNames shouldBe Seq("1", "2")
    resultMatrix.zColorZeroes shouldBe Seq(1d, 4d)
  }

  it should "be served with with given row and column names" in {
    DDS.setServer(mockedServer)
    DDS.heatmap(List(List(1, 2, 3), List(3, 4, 5)), rowNames = List("a", "b"), colNames = List("c", "d", "e"))

    val resultMatrix = mockedServer.lastServed.get.asInstanceOf[Heatmap]
    resultMatrix.title shouldBe s"Heatmap (2 x 3)"
    resultMatrix.content shouldBe List(List(1, 2, 3), List(3, 4, 5))
    resultMatrix.rowNames shouldBe Seq("a", "b")
    resultMatrix.colNames shouldBe Seq("c", "d", "e")
    resultMatrix.zColorZeroes shouldBe Seq(1d, 5d)
  }

  // TODO it should ... values contain NaN

  // TODO it should ... values contain only NaN

  "A correct correlation heatmap" should "be served from an RDD with two integer columns" in {
    DDS.setServer(mockedServer)
    val rdd = sc.makeRDD(List(Row(1, 3), Row(2, 2), Row(3, 1)))
    val schemaRdd = sql.createDataFrame(rdd, StructType(List(
      StructField("first", IntegerType, false),
      StructField("second", IntegerType, false)
    )))
    DDS.correlation(schemaRdd)

    val resultMatrix = mockedServer.lastServed.get.asInstanceOf[Heatmap]
    resultMatrix.title shouldBe s"Correlation of first, second"
    resultMatrix.colNames shouldBe Seq("first", "second")
    resultMatrix.rowNames shouldBe Seq("first", "second")
    resultMatrix.zColorZeroes shouldBe Seq(-1d, 0d, 1d)
    val corrMatrix = resultMatrix.content
    corrMatrix(0)(0) should be (1d +- epsilon)
    corrMatrix(0)(1) should be (-1d +- epsilon)
    corrMatrix(1)(0) should be (-1d +- epsilon)
    corrMatrix(1)(1) should be (1d +- epsilon)
  }

  it should "not be served from an RDD with not enough numerical columns" in {
    DDS.setServer(mockedServer)
    val rdd = sc.makeRDD(List(Row(1d, "c"), Row(2d, "a"), Row(3d, "b")))
    val schemaRdd = sql.createDataFrame(rdd, StructType(List(
      StructField("first", DoubleType, false),
      StructField("second", StringType, false)
    )))
    DDS.correlation(schemaRdd)

    mockedServer.lastServed.isEmpty shouldBe true
  }

  it should "be served from an RDD with two double columns" in {
    DDS.setServer(mockedServer)
    val rdd = sc.makeRDD(List(Row(1d, 3d), Row(2d, 2d), Row(3d, 1d)))
    val schemaRdd = sql.createDataFrame(rdd, StructType(List(
      StructField("first", DoubleType, false),
      StructField("second", DoubleType, false)
    )))
    DDS.correlation(schemaRdd)

    val resultMatrix = mockedServer.lastServed.get.asInstanceOf[Heatmap]
    resultMatrix.title shouldBe s"Correlation of first, second"
    resultMatrix.colNames shouldBe Seq("first", "second")
    resultMatrix.rowNames shouldBe Seq("first", "second")
    resultMatrix.zColorZeroes shouldBe Seq(-1d, 0d, 1d)
    val corrMatrix = resultMatrix.content
    corrMatrix(0)(0) should be (1d +- epsilon)
    corrMatrix(0)(1) should be (-1d +- epsilon)
    corrMatrix(1)(0) should be (-1d +- epsilon)
    corrMatrix(1)(1) should be (1d +- epsilon)
  }

  it should "be served from an RDD with three double columns where one is nullable" in {
    DDS.setServer(mockedServer)
    val rdd = sc.makeRDD(List(Row(1d, 3d, null), Row(2d, 2d, 2d), Row(3d, 1d, 3d)))
    val schemaRdd = sql.createDataFrame(rdd, StructType(List(
      StructField("first", DoubleType, false),
      StructField("second", DoubleType, false),
      StructField("third", DoubleType, true)
    )))
    DDS.correlation(schemaRdd)

    val resultMatrix = mockedServer.lastServed.get.asInstanceOf[Heatmap]
    resultMatrix.title shouldBe s"Correlation of first, second, third"
    resultMatrix.colNames shouldBe Seq("first", "second", "third")
    resultMatrix.rowNames shouldBe Seq("first", "second", "third")
    resultMatrix.zColorZeroes shouldBe Seq(-1d, 0d, 1d)
    val corrMatrix = resultMatrix.content
    corrMatrix(0)(0) should be (1d +- epsilon)
    corrMatrix(0)(1) should be (-1d +- epsilon)
    corrMatrix(0)(2) should be (1d +- epsilon)
    corrMatrix(1)(0) should be (-1d +- epsilon)
    corrMatrix(1)(1) should be (1d +- epsilon)
    corrMatrix(1)(2) should be (-1d +- epsilon)
    corrMatrix(2)(0) should be (1d +- epsilon)
    corrMatrix(2)(1) should be (-1d +- epsilon)
    corrMatrix(2)(2) should be (1d +- epsilon)
  }

  it should "be served from an RDD containing some non-numerical columns" in {
    DDS.setServer(mockedServer)
    val rdd = sc.makeRDD(List(Row("a", 1d, true, 3), Row("b", 2d, false, 2), Row("c", 3d, true, 1)))
    val schemaRdd = sql.createDataFrame(rdd, StructType(List(
      StructField("first", StringType, false),
      StructField("second", DoubleType, false),
      StructField("third", BooleanType, false),
      StructField("fourth", IntegerType, false)
    )))
    DDS.correlation(schemaRdd)

    val resultMatrix = mockedServer.lastServed.get.asInstanceOf[Heatmap]
    resultMatrix.title shouldBe s"Correlation of first, second, third, fourth"
    resultMatrix.colNames shouldBe Seq("second", "fourth")
    resultMatrix.rowNames shouldBe Seq("second", "fourth")
    resultMatrix.zColorZeroes shouldBe Seq(-1d, 0d, 1d)
    val corrMatrix = resultMatrix.content
    corrMatrix(0)(0) should be (1d +- epsilon)
    corrMatrix(0)(1) should be (-1d +- epsilon)
    corrMatrix(1)(0) should be (-1d +- epsilon)
    corrMatrix(1)(1) should be (1d +- epsilon)
  }

  it should "set (min, max) Z independent of the actual correlation values to (-1, 1)" in {
    DDS.setServer(mockedServer)
    val rdd = sc.makeRDD(List(Row(10, 32), Row(25, -100), Row(3, 1)))
    val schemaRdd = sql.createDataFrame(rdd, StructType(List(
      StructField("first", IntegerType, false),
      StructField("second", IntegerType, false)
    )))
    DDS.correlation(schemaRdd)

    val resultMatrix = mockedServer.lastServed.get.asInstanceOf[Heatmap]
    resultMatrix.zColorZeroes shouldBe Seq(-1d, 0d, 1d)
    val corrMatrix = resultMatrix.content
    corrMatrix(0)(1) should not be (-1d +- epsilon)
    corrMatrix(1)(0) should not be (-1d +- epsilon)
  }

  /**
   * I used R to compute the expected mutual information values. E.g.:
   *
   * library(entropy)
   * mi.plugin(rbind(c(2/3, 0), c(0, 1/3)), unit="log")
   */
  "A correct mutual information heatmap" should "be served from an RDD with three columns and no normalization" in {
    DDS.setServer(mockedServer)
    val rdd = sc.makeRDD(List(Row("1", "a", "1d"), Row("1", "b", "2d"), Row("2", "b", "3d")))
    val dataFrame = sql.createDataFrame(rdd, StructType(List(
      StructField("first", StringType, false),
      StructField("second", StringType, false),
      StructField("third", StringType, false)
    )))
    DDS.mutualInformation(dataFrame, MutualInformationAggregator.NO_NORMALIZATION)

    val resultMatrix = mockedServer.lastServed.get.asInstanceOf[Heatmap]
    resultMatrix.title shouldBe s"Mutual Information (${MutualInformationAggregator.NO_NORMALIZATION}) of first, second, third"
    resultMatrix.colNames shouldBe Seq("first", "second", "third")
    resultMatrix.rowNames shouldBe Seq("first", "second", "third")
    resultMatrix.zColorZeroes should have length 2
    resultMatrix.zColorZeroes(0) shouldBe 0d
    resultMatrix.zColorZeroes(1) should be (1.098612 +- epsilon)
    val miMatrix = resultMatrix.content
    miMatrix(0)(0) should be (0.6365142 +- epsilon)
    miMatrix(0)(1) should be (0.174416 +- epsilon)
    miMatrix(0)(2) should be (0.6365142 +- epsilon)
    miMatrix(1)(0) should be (0.174416 +- epsilon)
    miMatrix(1)(1) should be (0.6365142 +- epsilon)
    miMatrix(1)(2) should be (0.6365142 +- epsilon)
    miMatrix(2)(0) should be (0.6365142 +- epsilon)
    miMatrix(2)(1) should be (0.6365142 +- epsilon)
    miMatrix(2)(2) should be (1.098612 +- epsilon)
  }

  it should "be served from an RDD with one column and no normalization" in {
    DDS.setServer(mockedServer)
    val rdd = sc.makeRDD(List(Row("1"), Row("1"), Row("2")))
    val dataFrame = sql.createDataFrame(rdd, StructType(List(
      StructField("first", StringType, false)
    )))
    DDS.mutualInformation(dataFrame, MutualInformationAggregator.NO_NORMALIZATION)

    val resultMatrix = mockedServer.lastServed.get.asInstanceOf[Heatmap]
    resultMatrix.title shouldBe s"Mutual Information (${MutualInformationAggregator.NO_NORMALIZATION}) of first"
    resultMatrix.colNames shouldBe Seq("first")
    resultMatrix.rowNames shouldBe Seq("first")
    resultMatrix.zColorZeroes should have length 2
    resultMatrix.zColorZeroes(0) shouldBe 0d
    resultMatrix.zColorZeroes(1) should be (0.6365142 +- epsilon)
    val miMatrix = resultMatrix.content
    miMatrix(0)(0) should be (0.6365142 +- epsilon)
  }

  it should "be served from an RDD with three columns and normalization" in {
    DDS.setServer(mockedServer)
    val rdd = sc.makeRDD(List(Row("1", "a", "1d"), Row("1", "b", "2d"), Row("2", "b", "3d")))
    val dataFrame = sql.createDataFrame(rdd, StructType(List(
      StructField("first", StringType, false),
      StructField("second", StringType, false),
      StructField("third", StringType, false)
    )))
    DDS.mutualInformation(dataFrame, MutualInformationAggregator.METRIC_NORMALIZATION)

    val resultMatrix = mockedServer.lastServed.get.asInstanceOf[Heatmap]
    resultMatrix.title shouldBe s"Mutual Information (${MutualInformationAggregator.METRIC_NORMALIZATION}) " +
      "of first, second, third"
    resultMatrix.colNames shouldBe Seq("first", "second", "third")
    resultMatrix.rowNames shouldBe Seq("first", "second", "third")
    resultMatrix.zColorZeroes should have length 2
    resultMatrix.zColorZeroes(0) shouldBe 0d
    resultMatrix.zColorZeroes(1) should be (1d +- epsilon)
    val miMatrix = resultMatrix.content
    miMatrix(0)(0) should be (1d +- epsilon)
    miMatrix(0)(1) should be (0.2740174 +- epsilon)
    miMatrix(0)(2) should be (0.5793803 +- epsilon)
    miMatrix(1)(0) should be (0.2740174 +- epsilon)
    miMatrix(1)(1) should be (1d +- epsilon)
    miMatrix(1)(2) should be (0.5793803 +- epsilon)
    miMatrix(2)(0) should be (0.5793803 +- epsilon)
    miMatrix(2)(1) should be (0.5793803 +- epsilon)
    miMatrix(2)(2) should be (1d +- epsilon)
  }

  it should "use default normalization if wrong normalization is specified" in {
    DDS.setServer(mockedServer)
    val rdd = sc.makeRDD(List(Row("1", "a", "1d"), Row("1", "b", "2d"), Row("2", "b", "3d")))
    val dataFrame = sql.createDataFrame(rdd, StructType(List(
      StructField("first", StringType, false),
      StructField("second", StringType, false),
      StructField("third", StringType, false)
    )))
    DDS.mutualInformation(dataFrame, "dasaasfsdgsrwefsdf")

    val resultMatrix = mockedServer.lastServed.get.asInstanceOf[Heatmap]
    resultMatrix.title shouldBe s"Mutual Information (${MutualInformationAggregator.DEFAULT_NORMALIZATION})" +
      " of first, second, third"
    resultMatrix.colNames shouldBe Seq("first", "second", "third")
    resultMatrix.rowNames shouldBe Seq("first", "second", "third")
    resultMatrix.zColorZeroes should have length 2
    resultMatrix.zColorZeroes(0) shouldBe 0d
    resultMatrix.zColorZeroes(1) should be (1d +- epsilon)
    val miMatrix = resultMatrix.content
    miMatrix(0)(0) should be (1d +- epsilon)
    miMatrix(0)(1) should be (0.2740174 +- epsilon)
    miMatrix(0)(2) should be (0.5793803 +- epsilon)
    miMatrix(1)(0) should be (0.2740174 +- epsilon)
    miMatrix(1)(1) should be (1d +- epsilon)
    miMatrix(1)(2) should be (0.5793803 +- epsilon)
    miMatrix(2)(0) should be (0.5793803 +- epsilon)
    miMatrix(2)(1) should be (0.5793803 +- epsilon)
    miMatrix(2)(2) should be (1d +- epsilon)
  }

  it should "bin different numerical values using Sturge's formula before computing mutual information" in {
    DDS.setServer(mockedServer)
    val rdd = sc.makeRDD(List(Row(1, 1d, 1f, 1l, 1.toShort), Row(4, 4d, 6f, 6l, 4.toShort), Row(10, 10d, 10f, 10l, 10.toShort)))
    val dataFrame = sql.createDataFrame(rdd, StructType(List(
      StructField("first", IntegerType, false),
      StructField("second", DoubleType, false),
      StructField("third", FloatType, false),
      StructField("fourth", LongType, false),
      StructField("fifth", ShortType, false)
    )))
    DDS.mutualInformation(dataFrame, MutualInformationAggregator.METRIC_NORMALIZATION)

    val resultMatrix = mockedServer.lastServed.get.asInstanceOf[Heatmap]
    resultMatrix.title shouldBe s"Mutual Information (${MutualInformationAggregator.METRIC_NORMALIZATION}) of " +
      "first, second, third, fourth, fifth"
    resultMatrix.colNames shouldBe Seq("first", "second", "third", "fourth", "fifth")
    resultMatrix.rowNames shouldBe Seq("first", "second", "third", "fourth", "fifth")
    resultMatrix.zColorZeroes should have length 2
    resultMatrix.zColorZeroes(0) shouldBe 0d
    resultMatrix.zColorZeroes(1) should be (1d +- epsilon)
    val miMatrix = resultMatrix.content
    miMatrix(0)(0) should be (1d +- epsilon)
    miMatrix(0)(1) should be (1d +- epsilon)
    miMatrix(0)(2) should be (1d +- epsilon)
    miMatrix(0)(3) should be (1d +- epsilon)
    miMatrix(0)(4) should be (1d +- epsilon)
    miMatrix(1)(1) should be (1d +- epsilon)
    miMatrix(1)(2) should be (1d +- epsilon)
    miMatrix(1)(3) should be (1d +- epsilon)
    miMatrix(1)(4) should be (1d +- epsilon)
    miMatrix(2)(2) should be (1d +- epsilon)
    miMatrix(2)(3) should be (1d +- epsilon)
    miMatrix(2)(4) should be (1d +- epsilon)
    miMatrix(3)(3) should be (1d +- epsilon)
    miMatrix(3)(4) should be (1d +- epsilon)
    miMatrix(4)(4) should be (1d +- epsilon)
  }

  it should "bin numerical columns having null values (null as extra bin)" in {
    DDS.setServer(mockedServer)
    val rdd = sc.makeRDD(List(Row(1, 1d, 1f, 1l, 1.toShort), Row(4, 4d, 6f, 6l, 4.toShort), Row(null, null, null, null, null)))
    val dataFrame = sql.createDataFrame(rdd, StructType(List(
      StructField("first", IntegerType, true),
      StructField("second", DoubleType, true),
      StructField("third", FloatType, true),
      StructField("fourth", LongType, true),
      StructField("fifth", ShortType, true)
    )))
    DDS.mutualInformation(dataFrame, MutualInformationAggregator.METRIC_NORMALIZATION)

    val resultMatrix = mockedServer.lastServed.get.asInstanceOf[Heatmap]
    resultMatrix.title shouldBe s"Mutual Information (${MutualInformationAggregator.METRIC_NORMALIZATION}) of " +
      "first, second, third, fourth, fifth"
    resultMatrix.colNames shouldBe Seq("first", "second", "third", "fourth", "fifth")
    resultMatrix.rowNames shouldBe Seq("first", "second", "third", "fourth", "fifth")
    resultMatrix.zColorZeroes should have length 2
    resultMatrix.zColorZeroes(0) shouldBe 0d
    resultMatrix.zColorZeroes(1) should be (1d +- epsilon)
    val miMatrix = resultMatrix.content
    miMatrix(0)(0) should be (1d +- epsilon)
    miMatrix(0)(1) should be (1d +- epsilon)
    miMatrix(0)(2) should be (1d +- epsilon)
    miMatrix(0)(3) should be (1d +- epsilon)
    miMatrix(0)(4) should be (1d +- epsilon)
    miMatrix(1)(1) should be (1d +- epsilon)
    miMatrix(1)(2) should be (1d +- epsilon)
    miMatrix(1)(3) should be (1d +- epsilon)
    miMatrix(1)(4) should be (1d +- epsilon)
    miMatrix(2)(2) should be (1d +- epsilon)
    miMatrix(2)(3) should be (1d +- epsilon)
    miMatrix(2)(4) should be (1d +- epsilon)
    miMatrix(3)(3) should be (1d +- epsilon)
    miMatrix(3)(4) should be (1d +- epsilon)
    miMatrix(4)(4) should be (1d +- epsilon)
  }

  it should "work with numerical columns having only null values (null as extra bin)" in {
    DDS.setServer(mockedServer)
    val rdd = sc.makeRDD(List(Row(null, null, null, null, null), Row(null, null, null, null, null), Row(null, null, null, null, null)))
    val dataFrame = sql.createDataFrame(rdd, StructType(List(
      StructField("first", IntegerType, true),
      StructField("second", DoubleType, true),
      StructField("third", FloatType, true),
      StructField("fourth", LongType, true),
      StructField("fifth", ShortType, true)
    )))
    DDS.mutualInformation(dataFrame, MutualInformationAggregator.METRIC_NORMALIZATION)

    val resultMatrix = mockedServer.lastServed.get.asInstanceOf[Heatmap]
    resultMatrix.title shouldBe s"Mutual Information (${MutualInformationAggregator.METRIC_NORMALIZATION}) of " +
      "first, second, third, fourth, fifth"
    resultMatrix.colNames shouldBe Seq("first", "second", "third", "fourth", "fifth")
    resultMatrix.rowNames shouldBe Seq("first", "second", "third", "fourth", "fifth")
    resultMatrix.zColorZeroes should have length 2
    resultMatrix.zColorZeroes(0).isNaN shouldBe true
    resultMatrix.zColorZeroes(1).isNaN shouldBe true
    val miMatrix = resultMatrix.content
    miMatrix(0)(0).isNaN shouldBe true
    miMatrix(0)(1).isNaN shouldBe true
    miMatrix(0)(2).isNaN shouldBe true
    miMatrix(0)(3).isNaN shouldBe true
    miMatrix(0)(4).isNaN shouldBe true
    miMatrix(1)(1).isNaN shouldBe true
    miMatrix(1)(2).isNaN shouldBe true
    miMatrix(1)(3).isNaN shouldBe true
    miMatrix(1)(4).isNaN shouldBe true
    miMatrix(2)(2).isNaN shouldBe true
    miMatrix(2)(3).isNaN shouldBe true
    miMatrix(2)(4).isNaN shouldBe true
    miMatrix(3)(3).isNaN shouldBe true
    miMatrix(3)(4).isNaN shouldBe true
    miMatrix(4)(4).isNaN shouldBe true
  }

  it should "compute correct mutual information for bins with different ranges" in {
    DDS.setServer(mockedServer)
    val rdd = sc.makeRDD(List(Row(-10, 10d), Row(0, 0d), Row(0, 0d)))
    val dataFrame = sql.createDataFrame(rdd, StructType(List(
      StructField("first", IntegerType, true),
      StructField("second", DoubleType, true)
    )))
    DDS.mutualInformation(dataFrame, MutualInformationAggregator.NO_NORMALIZATION)

    val resultMatrix = mockedServer.lastServed.get.asInstanceOf[Heatmap]
    resultMatrix.title shouldBe s"Mutual Information (${MutualInformationAggregator.NO_NORMALIZATION}) of " +
      "first, second"
    resultMatrix.colNames shouldBe Seq("first", "second")
    resultMatrix.rowNames shouldBe Seq("first", "second")
    resultMatrix.zColorZeroes should have length 2
    resultMatrix.zColorZeroes(0) shouldBe 0d
    resultMatrix.zColorZeroes(1) should be (0.6365142 +- epsilon)
    val miMatrix = resultMatrix.content
    miMatrix(0)(0) should be (0.6365142 +- epsilon)
    miMatrix(0)(1) should be (0.6365142 +- epsilon)
    miMatrix(1)(1) should be (0.6365142 +- epsilon)
  }

  it should "bin NaN values as extra bins" in {
    DDS.setServer(mockedServer)
    val rdd = sc.makeRDD(List(Row(Double.NaN, Double.NaN), Row(0d, 0d)))
    val dataFrame = sql.createDataFrame(rdd, StructType(List(
      StructField("first", DoubleType, false),
      StructField("second", DoubleType, false)
    )))
    DDS.mutualInformation(dataFrame, MutualInformationAggregator.METRIC_NORMALIZATION)

    val resultMatrix = mockedServer.lastServed.get.asInstanceOf[Heatmap]
    resultMatrix.title shouldBe s"Mutual Information (${MutualInformationAggregator.METRIC_NORMALIZATION}) of " +
      "first, second"
    resultMatrix.colNames shouldBe Seq("first", "second")
    resultMatrix.rowNames shouldBe Seq("first", "second")
    resultMatrix.zColorZeroes should have length 2
    resultMatrix.zColorZeroes(0) shouldBe 0d
    resultMatrix.zColorZeroes(1) should be (1d +- epsilon)
    val miMatrix = resultMatrix.content
    miMatrix(0)(0) should be (1d +- epsilon)
    miMatrix(0)(1) should be (1d +- epsilon)
    miMatrix(1)(1) should be (1d +- epsilon)
  }

  it should "still bin NaN values as extra bins when a column contains only NaN values" in {
    DDS.setServer(mockedServer)
    val rdd = sc.makeRDD(List(Row(Double.NaN, Double.NaN), Row(Double.NaN, Double.NaN)))
    val dataFrame = sql.createDataFrame(rdd, StructType(List(
      StructField("first", DoubleType, false),
      StructField("second", DoubleType, false)
    )))
    DDS.mutualInformation(dataFrame, MutualInformationAggregator.NO_NORMALIZATION)

    val resultMatrix = mockedServer.lastServed.get.asInstanceOf[Heatmap]
    resultMatrix.title shouldBe s"Mutual Information (${MutualInformationAggregator.NO_NORMALIZATION}) of " +
      "first, second"
    resultMatrix.colNames shouldBe Seq("first", "second")
    resultMatrix.rowNames shouldBe Seq("first", "second")
    resultMatrix.zColorZeroes shouldBe Seq(0d, 0d)
    val miMatrix = resultMatrix.content
    miMatrix(0)(0) should be (0d +- epsilon)
    miMatrix(0)(1) should be (0d +- epsilon)
    miMatrix(1)(1) should be (0d +- epsilon)
  }

  it should "return None if called with empty dataframe as an argument" in {
    DDS.setServer(mockedServer)
    val rdd = sc.makeRDD(List(Row()))
    val dataFrame = sql.createDataFrame(rdd, StructType(List()))
    DDS.mutualInformation(dataFrame, MutualInformationAggregator.NO_NORMALIZATION)
    mockedServer.lastServed shouldBe None
  }

  "A correct dashboard" should "be served" in {
    DDS.setServer(mockedServer)
    val rdd = sc.parallelize(List(Row(1, "5", 5d), Row(3, "g", 5d), Row(5, "g", 6d)))
    val schema = StructType(List(
      StructField("first", IntegerType, false),
      StructField("second", StringType, false),
      StructField("third", DoubleType, false)
    ))
    val dataFrame = sql.createDataFrame(rdd, schema)
    DDS.dashboard(dataFrame)

    val resultDashboard = mockedServer.lastServed.get.asInstanceOf[Composite]
    resultDashboard.title shouldBe "Dashboard of first, second, third"
    val resultServables = resultDashboard.servables

    resultServables.size shouldBe 3
    resultServables(0).size shouldBe 1
    resultServables(1).size shouldBe 2
    resultServables(2).size shouldBe 1

    val table = resultServables(0)(0).asInstanceOf[Table]
    table.title shouldBe "Sample of first, second, third"
    table.schema shouldBe schema
    table.content shouldBe Seq(
      Row(1, "5", 5d),
      Row(3, "g", 5d),
      Row(5, "g", 6d)
    )

    val correlationServable = resultServables(1)(0).asInstanceOf[Heatmap]
    correlationServable.title shouldBe "Correlation of first, second, third"
    correlationServable.colNames shouldBe Seq("first", "third")
    correlationServable.rowNames shouldBe Seq("first", "third")
    correlationServable.zColorZeroes shouldBe Seq(-1d, 0d, 1d)
    correlationServable.content.size shouldBe 2
    correlationServable.content.foreach(row => row.size shouldBe 2)

    val mutualInformationServable = resultServables(1)(1).asInstanceOf[Heatmap]
    mutualInformationServable.title shouldBe "Mutual Information " +
      s"(${MutualInformationAggregator.DEFAULT_NORMALIZATION}) of first, second, third"
    mutualInformationServable.colNames shouldBe Seq("first", "second", "third")
    mutualInformationServable.rowNames shouldBe Seq("first", "second", "third")
    mutualInformationServable.zColorZeroes(0) shouldBe 0d
    // upper bound depends on the data and is tested somewhere else already so I won't test it here
    mutualInformationServable.content.size shouldBe 3
    mutualInformationServable.content.foreach(row => row.size shouldBe 3)


    val columnStatisticsServable = resultServables(2)(0).asInstanceOf[Composite]
    columnStatisticsServable.title shouldBe "Summary of first, second, third"
    // only look if the stuff looks like a summary statistics to avoid creating this huge set of expectations again
    val columnServables = columnStatisticsServable.servables
    columnServables.size shouldBe 3
    val column1Table = columnServables(0)(0).asInstanceOf[KeyValueSequence]
    column1Table.title shouldBe "Summary statistics of first"
    val column2Table = columnServables(1)(0).asInstanceOf[KeyValueSequence]
    column2Table.title shouldBe "Summary statistics of second"
    val column3Table = columnServables(2)(0).asInstanceOf[KeyValueSequence]
    column3Table.title shouldBe "Summary statistics of third"
  }

  "A correct column statistics" should "be served for normal data frames" in {
    DDS.setServer(mockedServer)
    val rdd = sc.parallelize(List(Row(5, "5", new Date(1)), Row(5, "g", new Date(0)), Row(5, "g", null)))
    val dataFrame = sql.createDataFrame(rdd, StructType(List(
      StructField("first", IntegerType, false),
      StructField("second", StringType, false),
      StructField("third", DateType, true)
    )))
    DDS.summarize(dataFrame)

    val resultSummary = mockedServer.lastServed.get.asInstanceOf[Composite]
    resultSummary.title shouldBe "Summary of first, second, third"
    val resultServables = resultSummary.servables

    resultServables.size shouldBe 3
    resultServables(0).size shouldBe 2
    resultServables(1).size shouldBe 2
    resultServables(2).size shouldBe 4

    val firstKeyValueSequence = resultServables(0)(0).asInstanceOf[KeyValueSequence]
    firstKeyValueSequence.title shouldBe "Summary statistics of first"
    firstKeyValueSequence.keyValuePairs shouldBe Seq(
      ("Total Count", "3"),
      ("Missing Count", "0"),
      ("Non-Missing Count", "3"),
      ("Sum", "15.0"),
      ("Min", "5.0"),
      ("Max", "5.0"),
      ("Mean", "5.0"),
      ("Stdev", "0.0"),
      ("Variance", "0.0")
    )

    val firstHistogram = resultServables(0)(1).asInstanceOf[Histogram]
    firstHistogram.title shouldBe "Histogram on first"
    firstHistogram.bins.size shouldBe 2
    firstHistogram.frequencies.sum shouldBe 3l

    val secondKeyValueSequence = resultServables(1)(0).asInstanceOf[KeyValueSequence]
    secondKeyValueSequence.title shouldBe "Summary statistics of second"
    secondKeyValueSequence.keyValuePairs shouldBe Seq(
      ("Total Count", "3"),
      ("Missing Count", "0"),
      ("Non-Missing Count", "3"),
      ("Mode", "(g,2)"),
      ("Cardinality", "2")
    )

    val secondBar = resultServables(1)(1).asInstanceOf[BarChart]
    secondBar.title shouldBe "Bar of second"
    secondBar.series shouldBe Seq("second")
    secondBar.xDomain shouldBe Seq("g", "5")
    secondBar.heights shouldBe Seq(Seq(2d, 1d))

    val thirdKeyValueSequence = resultServables(2)(0).asInstanceOf[KeyValueSequence]
    thirdKeyValueSequence.title shouldBe "Summary statistics of third"
    thirdKeyValueSequence.keyValuePairs shouldBe Seq(
      ("Total Count", "3"),
      ("Missing Count", "1"),
      ("Non-Missing Count", "2"),
      ("Top Year", "(1970,2)"),
      ("Top Month", "(Jan,2)"),
      ("Top Day", "(Thu,2)")
    )

    val thirdYearBar = resultServables(2)(1).asInstanceOf[BarChart]
    thirdYearBar.title shouldBe "Years in third"
    thirdYearBar.series shouldBe Seq("third")
    thirdYearBar.xDomain shouldBe Seq("1970", "NULL")
    thirdYearBar.heights shouldBe Seq(Seq(2d, 1d))

    val thirdMonthBar = resultServables(2)(2).asInstanceOf[BarChart]
    thirdMonthBar.title shouldBe "Months in third"
    thirdMonthBar.series shouldBe Seq("third")
    thirdMonthBar.xDomain shouldBe Seq("Jan", "NULL")
    thirdMonthBar.heights shouldBe Seq(Seq(2d, 1d))

    val thirdDayBar = resultServables(2)(3).asInstanceOf[BarChart]
    thirdDayBar.title shouldBe "Days in third"
    thirdDayBar.series shouldBe Seq("third")
    thirdDayBar.xDomain shouldBe Seq("Thu", "NULL")
    thirdDayBar.heights shouldBe Seq(Seq(2d, 1d))
  }

  it should "be served for data frames having all null values" in {
    DDS.setServer(mockedServer)
    val rdd = sc.parallelize(List(Row(null, null, null), Row(null, null, null), Row(null, null, null)))
    val dataFrame = sql.createDataFrame(rdd, StructType(List(
      StructField("first", IntegerType, true),
      StructField("second", StringType, true),
      StructField("third", DateType, true)
    )))
    DDS.summarize(dataFrame)
    val resultSummary = mockedServer.lastServed.get.asInstanceOf[Composite]
    resultSummary.title shouldBe "Summary of first, second, third"
    val resultServables = resultSummary.servables

    resultServables.size shouldBe 3
    resultServables(0).size shouldBe 2
    resultServables(1).size shouldBe 2
    resultServables(2).size shouldBe 4

    val firstKeyValues = resultServables(0)(0).asInstanceOf[KeyValueSequence]
    firstKeyValues.title shouldBe "Summary statistics of first"
    val firstKeyValuePairs = firstKeyValues.keyValuePairs
    firstKeyValuePairs(0) shouldBe ("Total Count", "3")
    firstKeyValuePairs(1) shouldBe ("Missing Count", "3")
    firstKeyValuePairs(2) shouldBe ("Non-Missing Count", "0")
    firstKeyValuePairs(3) shouldBe ("Sum", "0.0")
    firstKeyValuePairs(4) shouldBe ("Min", "Infinity")
    firstKeyValuePairs(5) shouldBe ("Max", "-Infinity")
    firstKeyValuePairs(6) shouldBe ("Mean", "NaN")
    firstKeyValuePairs(7) shouldBe ("Stdev", "NaN")
    firstKeyValuePairs(8) shouldBe ("Variance", "NaN")

    val firstHistogram = resultServables(0)(1) shouldBe Blank

    val secondKeyValues = resultServables(1)(0).asInstanceOf[KeyValueSequence]
    secondKeyValues.title shouldBe "Summary statistics of second"
    secondKeyValues.keyValuePairs shouldBe Seq(
      ("Total Count", "3"),
      ("Missing Count", "3"),
      ("Non-Missing Count", "0"),
      ("Mode", "(NULL,3)"),
      ("Cardinality", "1")
    )

    val secondBar = resultServables(1)(1).asInstanceOf[BarChart]
    secondBar.title shouldBe "Bar of second"
    secondBar.series shouldBe Seq("second")
    secondBar.xDomain shouldBe Seq("NULL")
    secondBar.heights shouldBe Seq(Seq(3d))

    val thirdKeyValues = resultServables(2)(0).asInstanceOf[KeyValueSequence]
    thirdKeyValues.title shouldBe "Summary statistics of third"
    thirdKeyValues.keyValuePairs shouldBe Seq(
      ("Total Count", "3"),
      ("Missing Count", "3"),
      ("Non-Missing Count", "0"),
      ("Top Year", "(NULL,3)"),
      ("Top Month", "(NULL,3)"),
      ("Top Day", "(NULL,3)")
    )

    val thirdYearBar = resultServables(2)(1).asInstanceOf[BarChart]
    thirdYearBar.title shouldBe "Years in third"
    thirdYearBar.series shouldBe Seq("third")
    thirdYearBar.xDomain shouldBe Seq("NULL")
    thirdYearBar.heights shouldBe Seq(Seq(3d))

    val thirdMonthBar = resultServables(2)(2).asInstanceOf[BarChart]
    thirdMonthBar.title shouldBe "Months in third"
    thirdMonthBar.series shouldBe Seq("third")
    thirdMonthBar.xDomain shouldBe Seq("NULL")
    thirdMonthBar.heights shouldBe Seq(Seq(3d))

    val thirdDayBar = resultServables(2)(3).asInstanceOf[BarChart]
    thirdDayBar.title shouldBe "Days in third"
    thirdDayBar.series shouldBe Seq("third")
    thirdDayBar.xDomain shouldBe Seq("NULL")
    thirdDayBar.heights shouldBe Seq(Seq(3d))
  }

}

/**
 * Needs to be defined top level in order to have a [[scala.reflect.runtime.universe.TypeTag]].
 */
private [core] case class DummyCaseClass(arg1: String, arg2: Int)
private [core] case class DummyHierarchicalCaseClass(harg1: DummyCaseClass, harg2: String)

