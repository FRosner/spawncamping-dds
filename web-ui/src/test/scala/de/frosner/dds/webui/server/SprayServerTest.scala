package de.frosner.dds.webui.server

import java.util.Date

import de.frosner.dds.servables.{Servable, Composite, Blank}
import de.frosner.dds.webui.servables.ServableJsonProtocol
import de.frosner.dds.webui.server.SprayServer._
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import spray.json._

import scala.io.Source
import scalaj.http.Http

// TODO Remove Akka log stuff through some config that is probably not packaged properly
class SprayServerTest extends FlatSpec with Matchers with BeforeAndAfter {

  private val waitTime = 4000
  private var testNumber = 0
  private val hostName = Source.fromInputStream(Runtime.getRuntime().exec("hostname").getInputStream).mkString.trim

  private def initDoTearDownWith(server: SprayServer)(toDo: SprayServer => Unit) = {
    try {
      server.init()
      toDo(server)
    } finally {
      server.tearDown()
    }
  }

  private def sprayServerWithoutLaunchingBrowser(name: String) = SprayServer(
    name = name,
    interface = SprayServer.DEFAULT_INTERFACE,
    port = SprayServer.DEFAULT_PORT,
    launchBrowser = false
  )
  
  before {
    Thread.sleep(waitTime/2)
    testNumber += 1
  }

  after {
    Thread.sleep(waitTime)
  }

  "A spray server" should s"be available on $DEFAULT_INTERFACE:$DEFAULT_PORT when started with default settings" in {
    val server = sprayServerWithoutLaunchingBrowser("server-" + testNumber)
    initDoTearDownWith(server) { server =>
      Thread.sleep(waitTime)
      Http(s"http://$DEFAULT_INTERFACE:$DEFAULT_PORT").asString shouldBe 'success
    }
  }

  it should "be available on the given interface and port as specified" in {
    assume(hostName != "", "Hostname could not be found. This test requires the 'hostname' command to be present.")
    val port = 25331
    val customServer = SprayServer(
      "custom-server-" + testNumber, interface = hostName, port = port, launchBrowser = false
    )
    initDoTearDownWith(customServer) { server =>
      Thread.sleep(waitTime)
      Http(s"http://$hostName:$port").asString shouldBe 'success
    }
  }

  it should "display a meaningful error message when socket is already in use" in {
    val firstServer = SprayServer(
      "first-server-" + testNumber, interface = "localhost", port = 23456, launchBrowser = false
    )
    val secondServer = SprayServer(
      "second-server-" + testNumber, interface = "localhost", port = 23456, launchBrowser = false
    )
    initDoTearDownWith(firstServer) { firstServer =>
      initDoTearDownWith(secondServer) { secondServer =>
        // see nice error message :P
      }
    }
  }

  "/servables" should "list no servables if none are served" in {
    val server = sprayServerWithoutLaunchingBrowser("server-" + testNumber)
    initDoTearDownWith(server) { server =>
      Thread.sleep(waitTime)
      Http(s"http://$DEFAULT_INTERFACE:$DEFAULT_PORT/servables").asString.body shouldBe JsArray().compactPrint
    }
  }

  it should "list all servables that have been served so far" in {
    val server = sprayServerWithoutLaunchingBrowser("server-" + testNumber)
    initDoTearDownWith(server) { server =>
      Thread.sleep(waitTime)

      val blankServable1 = Blank
      val blankServable2 = Blank
      val now = new Date()
      server.serve(blankServable1)
      server.serve(blankServable2)

      val answer = Http(s"http://${server.interface}:${server.port}/servables").asString.body

      val answerArray = JsonParser(answer).asInstanceOf[JsArray]
      val answerArrayElements = answerArray.elements
      answerArrayElements.size shouldBe 2
      val Vector(firstServable, secondServable) = answerArrayElements

      val firstServableFields = firstServable.asJsObject.fields
      firstServableFields("id") shouldBe JsNumber(0)
      new Date(firstServableFields("time").asInstanceOf[JsNumber].value.longValue()).getTime / 1000 shouldBe now.getTime / 1000
      firstServableFields("title") shouldBe JsString(Blank.title)

      val secondServableFields = secondServable.asJsObject.fields
      secondServableFields("id") shouldBe JsNumber(1)
      new Date(secondServableFields("time").asInstanceOf[JsNumber].value.longValue()).getTime / 1000 shouldBe now.getTime / 1000
      secondServableFields("title") shouldBe JsString(Blank.title)
    }
  }

  it should "not save a history if started with the corresponding parameter" in {
    val server = new SprayServer(
      name = "server-" + testNumber,
      interface = DEFAULT_INTERFACE,
      port = DEFAULT_PORT,
      launchBrowser = false,
      enableHistory = false
    )
    initDoTearDownWith(server) { server =>
      Thread.sleep(waitTime)

      val blankServable = Blank
      val compositeServable = Composite("c", Seq.empty)
      val now = new Date()
      server.serve(blankServable)
      server.serve(compositeServable)

      val answer = Http(s"http://${server.interface}:${server.port}/servables").asString.body

      val answerArray = JsonParser(answer).asInstanceOf[JsArray]
      val answerArrayElements = answerArray.elements
      answerArrayElements.size shouldBe 1
      val Vector(firstServable) = answerArrayElements

      val firstServableFields = firstServable.asJsObject.fields
      firstServableFields("id") shouldBe JsNumber(0)
      new Date(firstServableFields("time").asInstanceOf[JsNumber].value.longValue()).getTime / 1000 shouldBe now.getTime / 1000
      firstServableFields("title") shouldBe JsString(compositeServable.title)
    }
  }

  "/servables/<id>" should "return the servable with the given id" in {
    val server = sprayServerWithoutLaunchingBrowser("server-" + testNumber)
    initDoTearDownWith(server) { server =>
      Thread.sleep(waitTime)

      val blankServable: Servable = Blank
      val compositeServable: Servable = Composite("c", Seq.empty)
      val now = new Date()
      server.serve(blankServable)
      server.serve(compositeServable)

      val answer0 = Http(s"http://${server.interface}:${server.port}/servables/0").asString.body
      JsonParser(answer0).asJsObject shouldBe JsObject(
        ("id", JsNumber(0)),
        ("servable", blankServable.toJson(ServableJsonProtocol.ServableJsonFormat))
      )

      val answer1 = Http(s"http://${server.interface}:${server.port}/servables/1").asString.body
      JsonParser(answer1).asJsObject shouldBe JsObject(
        ("id", JsNumber(1)),
        ("servable", compositeServable.toJson(ServableJsonProtocol.ServableJsonFormat))
      )
    }
  }

  it should "fail if the given id does not exist" in {
    val server = sprayServerWithoutLaunchingBrowser("server-" + testNumber)
    initDoTearDownWith(server) { server =>
      Thread.sleep(waitTime)

      val blankServable = Blank
      val compositeServable = Composite("c", Seq.empty)
      val now = new Date()
      server.serve(blankServable)
      server.serve(compositeServable)

      Http(s"http://${server.interface}:${server.port}/servables/2").asString.isServerError shouldBe true
    }
  }

  it should "fail if the given id is not an integer" in {
    val server = sprayServerWithoutLaunchingBrowser("server-" + testNumber)
    initDoTearDownWith(server) { server =>
      Thread.sleep(waitTime)

      Http(s"http://${server.interface}:${server.port}/servables/thisisnotanumber").asString.isCodeInRange(404, 404) shouldBe true
    }
  }

  "/servables/latest?current=<id>" should "return the latest servable if the current id is not the latest one" in {
    val server = sprayServerWithoutLaunchingBrowser("server-" + testNumber)
    initDoTearDownWith(server) { server =>
      Thread.sleep(waitTime)

      val blankServable: Servable = Blank
      val compositeServable: Servable = Composite("c", Seq.empty)
      val now = new Date()
      server.serve(blankServable)
      server.serve(compositeServable)

      val answer = Http(s"http://${server.interface}:${server.port}/servables/latest?current=0").asString.body
      JsonParser(answer) shouldBe JsObject(
        ("id", JsNumber(1)),
        ("servable", compositeServable.toJson(ServableJsonProtocol.ServableJsonFormat))
      )
    }
  }

  it should "return no servable if the current id is the latest one" in {
    val server = sprayServerWithoutLaunchingBrowser("server-" + testNumber)
    initDoTearDownWith(server) { server =>
      Thread.sleep(waitTime)

      val blankServable = Blank
      val compositeServable = Composite("c", Seq.empty)
      val now = new Date()
      server.serve(blankServable)
      server.serve(compositeServable)

      val answer = Http(s"http://${server.interface}:${server.port}/servables/latest?current=1").asString.body
      JsonParser(answer) shouldBe JsObject()
    }
  }

  it should "return the latest servable if there is no current id specified" in {
    val server = sprayServerWithoutLaunchingBrowser("server-" + testNumber)
    initDoTearDownWith(server) { server =>
      Thread.sleep(waitTime)

      val blankServable: Servable = Blank
      val compositeServable: Servable = Composite("c", Seq.empty)
      val now = new Date()
      server.serve(blankServable)
      server.serve(compositeServable)

      val answer = Http(s"http://${server.interface}:${server.port}/servables/latest").asString.body
      JsonParser(answer) shouldBe JsObject(
        ("id", JsNumber(1)),
        ("servable", compositeServable.toJson(ServableJsonProtocol.ServableJsonFormat))
      )
    }
  }

  it should "return no servable if there is no current id specified but also nothing to serve" in {
    val server = sprayServerWithoutLaunchingBrowser("server-" + testNumber)
    initDoTearDownWith(server) { server =>
      Thread.sleep(waitTime)

      val answer = Http(s"http://${server.interface}:${server.port}/servables/latest").asString.body
      JsonParser(answer) shouldBe JsObject()
    }
  }

  it should "return an error if the given current id is not an integer" in {
    val server = sprayServerWithoutLaunchingBrowser("server-" + testNumber)
    initDoTearDownWith(server) { server =>
      Thread.sleep(waitTime)

      val answer = Http(s"http://${server.interface}:${server.port}/servables/latest?current=bla").asString
      answer.isServerError shouldBe true
    }
  }

  "A secured server" should "require HTTP authentication for the index page" in {
    val server = new SprayServer(
      name = "server-" + testNumber,
      interface = DEFAULT_INTERFACE,
      port = DEFAULT_PORT,
      launchBrowser = false,
      enableHistory = false,
      password = Some("password")
    )
    initDoTearDownWith(server) { server =>
      Thread.sleep(waitTime)

      Http(s"http://${server.interface}:${server.port}").asString.isCodeInRange(401, 401) shouldBe true
    }
  }

  it should "require HTTP authentication for the REST API" in {
    val server = new SprayServer(
      name = "server-" + testNumber,
      interface = DEFAULT_INTERFACE,
      port = DEFAULT_PORT,
      launchBrowser = false,
      enableHistory = false,
      password = Some("password")
    )
    initDoTearDownWith(server) { server =>
      Thread.sleep(waitTime)

      Http(s"http://${server.interface}:${server.port}/servables").asString.isCodeInRange(401, 401) shouldBe true
      Http(s"http://${server.interface}:${server.port}/servables/latest").asString.isCodeInRange(401, 401) shouldBe true
    }
  }

  it should "require HTTP authentication for the resource directory" in {
    val server = new SprayServer(
      name = "server-" + testNumber,
      interface = DEFAULT_INTERFACE,
      port = DEFAULT_PORT,
      launchBrowser = false,
      enableHistory = false,
      password = Some("password")
    )
    initDoTearDownWith(server) { server =>
      Thread.sleep(waitTime)

      Http(s"http://${server.interface}:${server.port}/ui/").asString.isCodeInRange(401, 401) shouldBe true
    }
  }

  it should "reject authentication attempts with wrong passwords" in {
    val server = new SprayServer(
      name = "server-" + testNumber,
      interface = DEFAULT_INTERFACE,
      port = DEFAULT_PORT,
      launchBrowser = false,
      enableHistory = false,
      password = Some("password")
    )
    initDoTearDownWith(server) { server =>
      Thread.sleep(waitTime)

      val http = Http(s"http://${server.interface}:${server.port}/servables")
      val httpAuth = http.auth("", "wrongPass")
      httpAuth.asString.isCodeInRange(401, 401) shouldBe true
    }
  }

  it should "allow authentication attempts with correct passwords" in {
    val server = new SprayServer(
      name = "server-" + testNumber,
      interface = DEFAULT_INTERFACE,
      port = DEFAULT_PORT,
      launchBrowser = false,
      enableHistory = false
    )
    initDoTearDownWith(server) { server =>
      Thread.sleep(waitTime)

      val http = Http(s"http://${server.interface}:${server.port}/servables")
      val httpAuth = http.auth("", "test")
      httpAuth.asString shouldBe 'success
    }
  }

  it should "allow authentication attempts with correct passwords and ignore any entered user name" in {
    val server = new SprayServer(
      name = "server-" + testNumber,
      interface = DEFAULT_INTERFACE,
      port = DEFAULT_PORT,
      launchBrowser = false,
      enableHistory = false
    )
    initDoTearDownWith(server) { server =>
      Thread.sleep(waitTime)

      val http = Http(s"http://${server.interface}:${server.port}/servables")
      val httpAuth = http.auth("user", "test")
      httpAuth.asString shouldBe 'success
    }
  }

}
