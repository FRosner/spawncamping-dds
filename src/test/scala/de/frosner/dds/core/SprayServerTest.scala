package de.frosner.dds.core

import de.frosner.dds.core.SprayServer._
import de.frosner.dds.servables.c3.{Chart, DummyData}
import de.frosner.dds.servables.tabular.Table
import org.apache.spark.util.StatCounter
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

import scala.io.Source
import scalaj.http.Http

class SprayServerTest extends FlatSpec with Matchers with BeforeAndAfter{

  private val waitTime = 2000
  private var testNumber = 0
  private var chartServer: SprayServer = _
  private val hostName = Source.fromInputStream(Runtime.getRuntime().exec("hostname").getInputStream).mkString.trim

  before {
    Thread.sleep(waitTime)
    chartServer = SprayServer.withoutLaunchingBrowser("server-" + testNumber)
    testNumber += 1
    chartServer.start()
    Thread.sleep(waitTime)
  }

  after {
    Thread.sleep(waitTime)
    chartServer.stop()
    Thread.sleep(waitTime)
  }

  "A chart server" should s"be available on $DEFAULT_INTERFACE:$DEFAULT_PORT when started with default settings" in {
    Http(s"http://$DEFAULT_INTERFACE:$DEFAULT_PORT").asString shouldBe 'success
  }

  it should "be available on the given interface and port as specified" in {
    assume(hostName != "", "Hostname could not be found. This test requires the 'hostname' command to be present.")
    val port = 25331
    val customServer = SprayServer(
      "custom-server-" + testNumber, interface = hostName, port = port, launchBrowser = false
    )
    customServer.start()
    Thread.sleep(waitTime)
    Http(s"http://$hostName:$port").asString shouldBe 'success
    customServer.stop()
  }

  it should "respond with an empty object if no chart is served" in {
    Http(s"http://$DEFAULT_INTERFACE:$DEFAULT_PORT/chart/update").asString.body shouldBe "{}"
  }

  it should "respond with a chart object if a chart is served" in {
    val chart = Chart(new DummyData("key", "value"))
    chartServer.serve(chart)
    Http(s"http://$DEFAULT_INTERFACE:$DEFAULT_PORT/chart/update").asString.body shouldBe chart.toJsonString
  }

  it should "respond with a table object if stats are served" in {
    val table = Table.fromStatCounter(StatCounter(0D, 1D, 2D))
    chartServer.serve(table)
    Http(s"http://$DEFAULT_INTERFACE:$DEFAULT_PORT/chart/update").asString.body shouldBe table.toJsonString
  }

  it should "respond with an empty object after serving a servable once" in {
    val chart = Chart(new DummyData("key", "value"))
    chartServer.serve(chart)
    Http(s"http://$DEFAULT_INTERFACE:$DEFAULT_PORT/chart/update").asString.body shouldBe chart.toJsonString
    Http(s"http://$DEFAULT_INTERFACE:$DEFAULT_PORT/chart/update").asString.body shouldBe "{}"
  }

}



