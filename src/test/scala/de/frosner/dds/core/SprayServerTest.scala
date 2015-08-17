package de.frosner.dds.core

import java.util.Date

import de.frosner.dds.core.SprayServer._
import de.frosner.dds.servables.c3
import de.frosner.dds.servables.composite.DummyServable
import de.frosner.dds.servables.tabular.Table
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import spray.json._

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

  "A spray server" should s"be available on $DEFAULT_INTERFACE:$DEFAULT_PORT when started with default settings" in {
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

  "/servables" should "list no servables if none are served" in {
    val chartServer = SprayServer.withoutLaunchingBrowser("server-" + testNumber)
    chartServer.start()
    Thread.sleep(waitTime)

    Http(s"http://$DEFAULT_INTERFACE:$DEFAULT_PORT/servables").asString.body shouldBe JsArray().compactPrint

    chartServer.stop()
  }

  it should "list all servables that have been served so far" in {
    val chartServer = SprayServer.withoutLaunchingBrowser("server-" + testNumber)
    chartServer.start()
    Thread.sleep(waitTime)

    val dummyServable1 = DummyServable(1)
    val dummyServable2 = Table(List("1"), List(List(1)))
    val now = new Date()
    chartServer.serve(dummyServable1)
    chartServer.serve(dummyServable2)

    val answer = Http(s"http://$DEFAULT_INTERFACE:$DEFAULT_PORT/servables").asString.body

    val answerArray = JsonParser(answer).asInstanceOf[JsArray]
    val answerArrayElements = answerArray.elements
    answerArrayElements.size shouldBe 2
    val Vector(firstServable, secondServable) = answerArrayElements

    val firstServableFields = firstServable.asJsObject.fields
    firstServableFields("id") shouldBe JsNumber(0)
    new Date(firstServableFields("time").asInstanceOf[JsNumber].value.longValue()).getTime / 1000 shouldBe now.getTime / 1000
    firstServableFields("type") shouldBe JsString(dummyServable1.servableType)

    val secondServableFields = secondServable.asJsObject.fields
    secondServableFields("id") shouldBe JsNumber(1)
    new Date(secondServableFields("time").asInstanceOf[JsNumber].value.longValue()).getTime / 1000 shouldBe now.getTime / 1000
    secondServableFields("type") shouldBe JsString(dummyServable2.servableType)

    chartServer.stop()
  }

  it should "not save a history if started with the corresponding parameter" in {
    // TODO
  }

  "/servables/<id>" should "return the servable with the given id" in {
    val chartServer = SprayServer.withoutLaunchingBrowser("server-" + testNumber)
    chartServer.start()
    Thread.sleep(waitTime)

    val dummyServable1 = DummyServable(1)
    val dummyServable2 = Table(List("1"), List(List(1)))
    val now = new Date()
    chartServer.serve(dummyServable1)
    chartServer.serve(dummyServable2)

    val answer0 = Http(s"http://$DEFAULT_INTERFACE:$DEFAULT_PORT/servables/0").asString.body
    JsonParser(answer0).asJsObject shouldBe JsObject(
      ("id", JsNumber(0)),
      ("servable", dummyServable1.toJson)
    )

    val answer1 = Http(s"http://$DEFAULT_INTERFACE:$DEFAULT_PORT/servables/1").asString.body
    JsonParser(answer1).asJsObject shouldBe JsObject(
      ("id", JsNumber(1)),
      ("servable", dummyServable2.toJson)
    )

    chartServer.stop()
  }

  it should "fail if the given id does not exist" in {
    val chartServer = SprayServer.withoutLaunchingBrowser("server-" + testNumber)
    chartServer.start()
    Thread.sleep(waitTime)

    val dummyServable1 = DummyServable(1)
    val dummyServable2 = Table(List("1"), List(List(1)))
    val now = new Date()
    chartServer.serve(dummyServable1)
    chartServer.serve(dummyServable2)

    Http(s"http://$DEFAULT_INTERFACE:$DEFAULT_PORT/servables/2").asString.isServerError shouldBe true

    chartServer.stop()
  }

  it should "fail if the given id is not an integer" in {
    val chartServer = SprayServer.withoutLaunchingBrowser("server-" + testNumber)
    chartServer.start()
    Thread.sleep(waitTime)

    Http(s"http://$DEFAULT_INTERFACE:$DEFAULT_PORT/servables/thisisnotanumber").asString.isCodeInRange(404, 404) shouldBe true

    chartServer.stop()
  }

  "/servables/latest?current=<id>" should "return the latest servable if the current id is not the latest one" in {
    val chartServer = SprayServer.withoutLaunchingBrowser("server-" + testNumber)
    chartServer.start()
    Thread.sleep(waitTime)

    val dummyServable1 = DummyServable(1)
    val dummyServable2 = Table(List("1"), List(List(1)))
    val now = new Date()
    chartServer.serve(dummyServable1)
    chartServer.serve(dummyServable2)

    val answer = Http(s"http://$DEFAULT_INTERFACE:$DEFAULT_PORT/servables/latest?current=0").asString.body
    JsonParser(answer) shouldBe JsObject(
      ("id", JsNumber(1)),
      ("servable", dummyServable2.toJson)
    )

    chartServer.stop()
  }

  it should "return no servable if the current id is the latest one" in {
    val chartServer = SprayServer.withoutLaunchingBrowser("server-" + testNumber)
    chartServer.start()
    Thread.sleep(waitTime)

    val dummyServable1 = DummyServable(1)
    val dummyServable2 = Table(List("1"), List(List(1)))
    val now = new Date()
    chartServer.serve(dummyServable1)
    chartServer.serve(dummyServable2)

    val answer = Http(s"http://$DEFAULT_INTERFACE:$DEFAULT_PORT/servables/latest?current=1").asString.body
    JsonParser(answer) shouldBe JsObject()

    chartServer.stop()
  }

  it should "return the latest servable if there is no current id specified" in {
    val chartServer = SprayServer.withoutLaunchingBrowser("server-" + testNumber)
    chartServer.start()
    Thread.sleep(waitTime)

    val dummyServable1 = DummyServable(1)
    val dummyServable2 = Table(List("1"), List(List(1)))
    val now = new Date()
    chartServer.serve(dummyServable1)
    chartServer.serve(dummyServable2)

    val answer = Http(s"http://$DEFAULT_INTERFACE:$DEFAULT_PORT/servables/latest").asString.body
    JsonParser(answer) shouldBe JsObject(
      ("id", JsNumber(1)),
      ("servable", dummyServable2.toJson)
    )

    chartServer.stop()
  }

  it should "return no servable if there is no current id specified but also nothing to serve" in {
    val chartServer = SprayServer.withoutLaunchingBrowser("server-" + testNumber)
    chartServer.start()
    Thread.sleep(waitTime)

    val answer = Http(s"http://$DEFAULT_INTERFACE:$DEFAULT_PORT/servables/latest").asString.body
    JsonParser(answer) shouldBe JsObject()

    chartServer.stop()
  }

  it should "return an error if the given current id is not an integer" in {
    val chartServer = SprayServer.withoutLaunchingBrowser("server-" + testNumber)
    chartServer.start()
    Thread.sleep(waitTime)

    val answer = Http(s"http://$DEFAULT_INTERFACE:$DEFAULT_PORT/servables/latest?current=bla").asString
    answer.isServerError shouldBe true

    chartServer.stop()
  }

  "A secured chart server" should "require HTTP authentication for the index page" in {
    val chartServer = SprayServer("server-" + testNumber, launchBrowser = false, password = Option("test"))
    chartServer.start()
    Thread.sleep(waitTime)

    Http(s"http://$DEFAULT_INTERFACE:$DEFAULT_PORT").asString.isCodeInRange(401, 401) shouldBe true

    chartServer.stop()
  }

  it should "require HTTP authentication for the REST API" in {
    val chartServer = SprayServer("server-" + testNumber, launchBrowser = false, password = Option("test"))
    chartServer.start()
    Thread.sleep(waitTime)

    Http(s"http://$DEFAULT_INTERFACE:$DEFAULT_PORT/servables").asString.isCodeInRange(401, 401) shouldBe true
    Http(s"http://$DEFAULT_INTERFACE:$DEFAULT_PORT/servables/latest").asString.isCodeInRange(401, 401) shouldBe true

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

    val http = Http(s"http://$DEFAULT_INTERFACE:$DEFAULT_PORT/servables")
    val httpAuth = http.auth("", "wrongPass")
    httpAuth.asString.isCodeInRange(401, 401) shouldBe true

    chartServer.stop()
  }

  it should "allow authentication attempts with correct passwords" in {
    val chartServer = SprayServer("server-" + testNumber, launchBrowser = false, password = Option("test"))
    chartServer.start()
    Thread.sleep(waitTime)

    val http = Http(s"http://$DEFAULT_INTERFACE:$DEFAULT_PORT/servables")
    val httpAuth = http.auth("", "test")
    httpAuth.asString shouldBe 'success

    chartServer.stop()
  }

  it should "allow authentication attempts with correct passwords and ignore any entered user name" in {
    val chartServer = SprayServer("server-" + testNumber, launchBrowser = false, password = Option("test"))
    chartServer.start()
    Thread.sleep(waitTime)

    val http = Http(s"http://$DEFAULT_INTERFACE:$DEFAULT_PORT/servables")
    val httpAuth = http.auth("user", "test")
    httpAuth.asString shouldBe 'success

    chartServer.stop()
  }

}



