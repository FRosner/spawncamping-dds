package de.frosner.dds.core

import java.awt.Desktop
import java.net.URI
import java.util.Date

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import spray.json._
import spray.routing.SimpleRoutingApp
import spray.routing.authentication._
import spray.routing.Route
import spray.routing.directives.AuthMagnet
import scala.concurrent.ExecutionContext
import ExecutionContext.Implicits.global

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Try

/**
 * [[Server]] based on spray-can HTTP server. If multiple servers shall be used, they need to have different names.
 *
 * @param name of the server
 * @param launchBrowser indicating whether a browser window pointing to the web UI should be launched
 *                      when the server is started
 * @param interface to bind the server to
 * @param port to bind the server to
 */
case class SprayServer(name: String,
                       launchBrowser: Boolean = true,
                       interface: String = SprayServer.DEFAULT_INTERFACE,
                       port: Int = SprayServer.DEFAULT_PORT,
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
          "DDS has been password protected",
          (userPass: Option[UserPass]) => Future(
            if (userPass.exists(_.pass == password.get)) Some(true)
            else None
          )
        )
      ))(authenticated => innerRoute)
    } else {
      innerRoute
    }
  
  def start() = {
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
                    ("type", JsString(servable.servableType)),
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

  def stop() = {
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

  /**
   * Create a server instance bound to default port and interface, without opening a browser window.
   *
   * @param name of the server
   * @return A server bound to default port and interface.
   */
  def withoutLaunchingBrowser(name: String) = SprayServer(name, launchBrowser = false)

  private def wrapIdAndServable(id: Int, servable: Servable): String = {
    JsObject(
      ("servable", servable.toJson),
      ("id", JsNumber(id))
    ).compactPrint
  }

}
