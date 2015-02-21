package de.frosner.dds.core

import de.frosner.dds.chart._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.scalamock.scalatest.MockFactory
import org.scalatest._

class DDSTest extends FlatSpec with Matchers with MockFactory with BeforeAndAfterEach with BeforeAndAfterAll {

  private var server: ChartServer = _
  private val sc: SparkContext = new SparkContext("local", this.getClass.toString)

  override def afterAll() = {
    sc.stop()
  }

  override def beforeEach() {
    server = stub[ChartServer]
  }

  override def afterEach {
    DDS.resetServer()
  }

  "DDS" should "start the chart server when start() is executed" in {
    DDS.start(server)
    (server.start _).verify().once()
  }

  it should "tear the server down when stop() is executed" in {
    DDS.start(server)
    DDS.stop()
    (server.stop _).verify().once()
  }

  it should "not start another server if one is started already" in {
    DDS.start(server)
    DDS.start(server)
    (server.start _).verify().once()
  }

  it should "do nothing when stopped the second time" in {
    DDS.start(server)
    DDS.stop()
    DDS.stop()
    (server.stop _).verify().once()
  }

  "Correct charts from Seq[Numeric]" should "be served by the line plot function" in {
    DDS.start(server)
    DDS.line(List(1,2,3))
    val expectedChart = Chart(SeriesData(Series("data1", List(1, 2, 3)), ChartTypeEnum.Line))
    (server.serve _).verify(expectedChart)
  }

  it should "be served by the pie plot function" in {
    DDS.start(server)
    DDS.pie(List(1,2,3))
    val expectedChart = Chart(SeriesData(Series("data1", List(1, 2, 3)), ChartTypeEnum.Pie))
    (server.serve _).verify(expectedChart)
  }

  it should "be served by the bar plot function" in {
    DDS.start(server)
    DDS.bar(List(1,2,3))
    val expectedChart = Chart(SeriesData(Series("data1", List(1, 2, 3)), ChartTypeEnum.Bar))
    (server.serve _).verify(expectedChart)
  }

  "Correct pie chart from RDD after groupBy" should "be served when values are already grouped" in {
    DDS.start(server)
    val groupedRdd = sc.makeRDD(List(("a", 1), ("a", 2), ("b", 3), ("c", 5))).groupBy(_._1).
      mapValues(values => values.map{ case (key, value) => value} )
    DDS.pie(groupedRdd)

    val expectedChartTypes = ChartTypes.multiple(ChartTypeEnum.Pie, 3)
    val expectedChartSeries = List(
      Series("a", List(3)),
      Series("b", List(3)),
      Series("c", List(5))
    )
    val expectedChart = Chart(SeriesData(expectedChartSeries, expectedChartTypes))
    (server.serve _).verify(expectedChart)
  }

  it should "be served when values are not grouped, yet" in {
    DDS.start(server)
    val groupedRdd = sc.makeRDD(List(("a", 1), ("a", 2), ("b", 3), ("c", 5)))
    DDS.pie(groupedRdd)

    val expectedChartTypes = ChartTypes.multiple(ChartTypeEnum.Pie, 3)
    val expectedChartSeries = List(
      Series("a", List(3)),
      Series("b", List(3)),
      Series("c", List(5))
    )
    val expectedChart = Chart(SeriesData(expectedChartSeries, expectedChartTypes))
    (server.serve _).verify(expectedChart)
  }

}
