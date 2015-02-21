package de.frosner.dds.core

import akka.actor.ActorSystem
import de.frosner.dds.chart.{DummyData, Chart}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfter, Matchers, FlatSpec}

import scalaj.http.Http

class ChartServerTest extends FlatSpec with Matchers with BeforeAndAfter{

  private val waitTime = 2000
  private var testNumber = 0

  before {
    Thread.sleep(waitTime)
    ChartServer.system = ActorSystem("test-system-" + testNumber)
    ChartServer.isInTestMode = true
    ChartServer.actorName += testNumber
    testNumber += 1
    ChartServer.start()
    Thread.sleep(waitTime)
  }

  after {
    Thread.sleep(waitTime)
    ChartServer.stop()
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
    ChartServer.serve(chart)
    Http("http://localhost:8080/chart/update").asString.body shouldBe chart.toJsonString
  }

  it should "respond with an empty object after serving a chart once" in {
    val chart = Chart(new DummyData("key", "value"))
    ChartServer.serve(chart)
    Http("http://localhost:8080/chart/update").asString.body shouldBe chart.toJsonString
    Http("http://localhost:8080/chart/update").asString.body shouldBe "{}"
  }

}



