package de.frosner.dds.webui.server

import java.awt.Desktop
import java.net.URI
import java.util.Date

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import de.frosner.dds.core.{DDS, Server}
import de.frosner.dds.servables.Servable
import de.frosner.dds.webui.servables.ServableJsonProtocol
import de.frosner.replhelper.Help
import spray.json._
import spray.routing.authentication._
import spray.routing.directives.AuthMagnet
import spray.routing.{Route, SimpleRoutingApp}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Try

/**
 * [[de.frosner.dds.core.Server]] based on spray-can HTTP server. If multiple servers shall be used, they need to have different names.
 *
 * @param name of the server
 * @param launchBrowser indicating whether a browser window pointing to the web UI should be launched
 *                      when the server is started
 * @param interface to bind the server to
 * @param port to bind the server to
 */
case class SprayServer(name: String,
                       launchBrowser: Boolean = true,
                       interface: String,
                       port: Int,
                       password: Option[String] = Option.empty,
                       enableHistory: Boolean = true)
  extends SimpleRoutingApp with Server {

  private var servables: Seq[(Servable, Date)] = Vector.empty

  private implicit val system = ActorSystem(name + "-system", {
    val conf = ConfigFactory.parseResources("dds.typesafe-conf")
    conf.resolve()
  })

  private val actorName = "chart-server-" + name + "-actor"

  private def withAuthentication(innerRoute: Route) =
    if (password.isDefined) {
      authenticate(AuthMagnet.fromContextAuthenticator(
        new BasicHttpAuthenticator(
          "DDS Web UI has been password protected",
          (userPass: Option[UserPass]) => Future(
            if (userPass.exists(_.pass == password.get)) Some(true)
            else None
          )
        )
      ))(authenticated => innerRoute)
    } else {
      innerRoute
    }

  def init(): Unit = {
    val tryToConnectToSocket = Try(scalaj.http.Http(s"http://$interface:$port").asString)
    if (tryToConnectToSocket.isSuccess) {
      println(s"""$interface:$port is already in use. Server started already? Another server blocking the socket?""")
      println()
      DDS.help("start")
    } else {
      println(s"""Starting server on $interface:$port""")
      if (password.isDefined) println(s"""Basic HTTP authentication enabled (password = ${password.get}). """ +
        s"""Password will be transmitted unencrypted. Do not reuse it somewhere else!""")
      val server = startServer(interface, port, actorName) {
        path("") {
          withAuthentication {
            getFromResource("ui/index.html")
          }
        } ~
          path("servables") {
            withAuthentication {
              get {
                complete {
                  val servableObjects = servables.zipWithIndex.map{
                    case ((servable, date), index) => JsObject(Map(
                      ("id", JsNumber(index)),
                      ("title", JsString(servable.title)), // TODO I changed this from type to title (check JS)
                      ("time", JsNumber(date.getTime))
                    ))
                  }
                  JsArray(servableObjects.toVector).compactPrint
                }
              }
            }
          } ~
          path("servables" / IntNumber) { id =>
            withAuthentication {
              get {
                if (id < servables.size)
                  complete {
                    val (servable, date) = servables(id)
                    SprayServer.wrapIdAndServable(id, servable)
                  }
                else
                  failWith(new IllegalArgumentException(
                    s"There is no servable with id $id. Id needs to be within (0, ${servables.size})"
                  ))
              }
            }
          } ~
          (path("servables" / "latest") & parameter('current ?)) { currentServableIdParameter =>
            withAuthentication {
              get {
                complete {
                  val currentServableIndex = currentServableIdParameter.map(_.toInt)
                  val lastServableIndex = servables.size - 1
                  if (lastServableIndex >= 0 && currentServableIndex.forall(_ != lastServableIndex)) {
                    val (servable, date) = servables(lastServableIndex)
                    SprayServer.wrapIdAndServable(lastServableIndex, servable)
                  } else {
                    "{}"
                  }
                }
              }
            }
          } ~
          pathPrefix("ui") {
            withAuthentication {
              getFromResourceDirectory("ui")
            }
          }
      }

      Thread.sleep(1000)
      if (launchBrowser && Desktop.isDesktopSupported()) {
        println("Opening browser")
        Desktop.getDesktop().browse(new URI( s"""http://$interface:$port/"""))
      }
    }
  }

  def tearDown(): Unit = {
    println("Stopping server")
    servables = Seq.empty
    system.scheduler.scheduleOnce(1.milli)(system.shutdown())(system.dispatcher)
  }

  def serve(servable: Servable) = {
    val toAdd = (servable, new Date())
    if (enableHistory)
      servables = servables :+ toAdd
    else
      servables = Vector(toAdd)
  }

}

object SprayServer {

  val DEFAULT_INTERFACE = "localhost"
  val DEFAULT_PORT = 23080

  private var serverNumber = 0

  private var serverInstance = Option.empty[SprayServer]

  @Help(
    category = "Web UI",
    shortDescription = "Starts the DDS Web UI",
    longDescription = "Starts the DDS Web UI bound to the default interface and port. You can stop it by calling stop()."
  )
  def start(): Unit = start(DEFAULT_INTERFACE, DEFAULT_PORT)

  @Help(
    category = "Web UI",
    shortDescription = "Starts the DDS Web UI bound to the given interface and port with an optional authentication mechanism",
    longDescription = "Starts the DDS Web UI bound to the given interface and port. You can also specify a password " +
      "which will be used for a simple HTTP authentication. Note however, that this is transmitting the password " +
      "unencrypted and you should not reuse this password somewhere else. You can stop it by calling stop().",
    parameters = "interface: String, port: Int, (optional) password: String"
  )
  def start(interface: String, port: Int, password: String = null): Unit =
    DDS.setServer(SprayServer(
      name = "dds-" + serverNumber,
      interface = interface,
      port = port,
      launchBrowser = true,
      password = Option(password)
    ))

  @Help(
    category = "Web UI",
    shortDescription = "Stops the DDS Web UI",
    longDescription = "Stops the DDS Web UI. You can restart it again by calling start()."
  )
  def stop(): Unit = DDS.unsetServer()

  private def wrapIdAndServable(id: Int, servable: Servable): String = {
    JsObject(
      ("servable", servable.toJson(ServableJsonProtocol.ServableJsonFormat)),
      ("id", JsNumber(id))
    ).compactPrint
  }

}
