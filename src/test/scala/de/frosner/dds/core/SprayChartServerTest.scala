package de.frosner.dds.core

import akka.actor.ActorSystem
import de.frosner.dds.chart.{Stats, DummyData, Chart}
import org.apache.spark.util.StatCounter
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfter, Matchers, FlatSpec}

import scalaj.http.Http

class SprayChartServerTest extends FlatSpec with Matchers with BeforeAndAfter{

  private val waitTime = 2000
  private var testNumber = 0
  private var chartServer: SprayChartServer = _

  before {
    Thread.sleep(waitTime)
    chartServer = SprayChartServer.withoutLaunchingBrowser("server-" + testNumber)
    testNumber += 1
    chartServer.start()
    Thread.sleep(waitTime)
  }

  after {
    Thread.sleep(waitTime)
    chartServer.stop()
    Thread.sleep(waitTime)
  }

  "A chart server" should "be available on localhost with port 8080" in {
    Http("http://localhost:8080").asString shouldBe 'success
  }

  it should "respond with an empty object if no chart is served" in {
    Http("http://localhost:8080/chart/update").asString.body shouldBe "{}"
  }

  it should "respond with a chart object if a chart is served" in {
    val chart = Chart(new DummyData("key", "value"))
    chartServer.serve(chart)
    Http("http://localhost:8080/chart/update").asString.body shouldBe chart.toJsonString
  }

  it should "respond with a stats object if stats are served" in {
    val stats = Stats(StatCounter(0D, 1D, 2D))
    chartServer.serve(stats)
    Http("http://localhost:8080/chart/update").asString.body shouldBe stats.toJsonString
  }

  it should "respond with an empty object after serving a servable once" in {
    val chart = Chart(new DummyData("key", "value"))
    chartServer.serve(chart)
    Http("http://localhost:8080/chart/update").asString.body shouldBe chart.toJsonString
    Http("http://localhost:8080/chart/update").asString.body shouldBe "{}"
  }

}



