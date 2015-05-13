package de.frosner.dds.core

import de.frosner.dds.core.SprayServer._
import de.frosner.dds.servables.c3.{Chart, DummyData}
import de.frosner.dds.servables.tabular.Table
import org.apache.spark.util.StatCounter
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

import scala.io.Source
import scalaj.http.Http

class SprayServerTest extends FlatSpec with Matchers with BeforeAndAfter{

  private val waitTime = 4000
  private var testNumber = 0
  private val hostName = Source.fromInputStream(Runtime.getRuntime().exec("hostname").getInputStream).mkString.trim

  before {
    Thread.sleep(waitTime/2)
    testNumber += 1
  }

  after {
    Thread.sleep(waitTime)
  }

  "A chart server" should s"be available on $DEFAULT_INTERFACE:$DEFAULT_PORT when started with default settings" in {
    val chartServer = SprayServer.withoutLaunchingBrowser("server-" + testNumber)
    chartServer.start()
    Thread.sleep(waitTime)

    Http(s"http://$DEFAULT_INTERFACE:$DEFAULT_PORT").asString shouldBe 'success

    chartServer.stop()
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

  it should "display a meaningful error message when socket is already in use" in {
    val firstServer = SprayServer(
      "first-server-" + testNumber, interface = "localhost", port = 23456, launchBrowser = false
    )
    val secondServer = SprayServer(
      "second-server-" + testNumber, interface = "localhost", port = 23456, launchBrowser = false
    )
    firstServer.start()
    secondServer.start() // see nice error message :P
    firstServer.stop()
  }

  it should "respond with an empty object if no chart is served" in {
    val chartServer = SprayServer.withoutLaunchingBrowser("server-" + testNumber)
    chartServer.start()
    Thread.sleep(waitTime)

    Http(s"http://$DEFAULT_INTERFACE:$DEFAULT_PORT/chart/update").asString.body shouldBe "{}"

    chartServer.stop()
  }

  it should "respond with a chart object if a chart is served" in {
    val chartServer = SprayServer.withoutLaunchingBrowser("server-" + testNumber)
    chartServer.start()
    Thread.sleep(waitTime)

    val chart = Chart(new DummyData("key", "value"))
    chartServer.serve(chart)
    Http(s"http://$DEFAULT_INTERFACE:$DEFAULT_PORT/chart/update").asString.body shouldBe chart.toJsonString

    chartServer.stop()
  }

  it should "respond with a table object if stats are served" in {
    val chartServer = SprayServer.withoutLaunchingBrowser("server-" + testNumber)
    chartServer.start()
    Thread.sleep(waitTime)

    val table = Table.fromStatCounter(StatCounter(0D, 1D, 2D))
    chartServer.serve(table)
    Http(s"http://$DEFAULT_INTERFACE:$DEFAULT_PORT/chart/update").asString.body shouldBe table.toJsonString

    chartServer.stop()
  }

  it should "respond with an empty object after serving a servable once" in {
    val chartServer = SprayServer.withoutLaunchingBrowser("server-" + testNumber)
    chartServer.start()
    Thread.sleep(waitTime)

    val chart = Chart(new DummyData("key", "value"))
    chartServer.serve(chart)
    Http(s"http://$DEFAULT_INTERFACE:$DEFAULT_PORT/chart/update").asString.body shouldBe chart.toJsonString
    Http(s"http://$DEFAULT_INTERFACE:$DEFAULT_PORT/chart/update").asString.body shouldBe "{}"

    chartServer.stop()
  }

  "A secured chart server" should "require HTTP authentication for the index page" in {
    val chartServer = SprayServer("server-" + testNumber, launchBrowser = false, password = Option("test"))
    chartServer.start()
    Thread.sleep(waitTime)

    Http(s"http://$DEFAULT_INTERFACE:$DEFAULT_PORT").asString.isCodeInRange(401, 401) shouldBe true

    chartServer.stop()
  }

  it should "require HTTP authentication for the chart update page" in {
    val chartServer = SprayServer("server-" + testNumber, launchBrowser = false, password = Option("test"))
    chartServer.start()
    Thread.sleep(waitTime)

    Http(s"http://$DEFAULT_INTERFACE:$DEFAULT_PORT/chart/update").asString.isCodeInRange(401, 401) shouldBe true

    chartServer.stop()
  }

  it should "require HTTP authentication for the resource directory" in {
    val chartServer = SprayServer("server-" + testNumber, launchBrowser = false, password = Option("test"))
    chartServer.start()
    Thread.sleep(waitTime)

    Http(s"http://$DEFAULT_INTERFACE:$DEFAULT_PORT/ui/").asString.isCodeInRange(401, 401) shouldBe true

    chartServer.stop()
  }

  it should "reject authentication attempts with wrong passwords" in {
    val chartServer = SprayServer("server-" + testNumber, launchBrowser = false, password = Option("test"))
    chartServer.start()
    Thread.sleep(waitTime)

    val http = Http(s"http://$DEFAULT_INTERFACE:$DEFAULT_PORT/chart/update")
    val httpAuth = http.auth("", "wrongPass")
    httpAuth.asString.isCodeInRange(401, 401) shouldBe true

    chartServer.stop()
  }

  it should "allow authentication attempts with correct passwords" in {
    val chartServer = SprayServer("server-" + testNumber, launchBrowser = false, password = Option("test"))
    chartServer.start()
    Thread.sleep(waitTime)

    val http = Http(s"http://$DEFAULT_INTERFACE:$DEFAULT_PORT/chart/update")
    val httpAuth = http.auth("", "test")
    httpAuth.asString shouldBe 'success

    chartServer.stop()
  }

  it should "allow authentication attempts with correct passwords and ignore any entered user name" in {
    val chartServer = SprayServer("server-" + testNumber, launchBrowser = false, password = Option("test"))
    chartServer.start()
    Thread.sleep(waitTime)

    val http = Http(s"http://$DEFAULT_INTERFACE:$DEFAULT_PORT/chart/update")
    val httpAuth = http.auth("user", "test")
    httpAuth.asString shouldBe 'success

    chartServer.stop()
  }

}



