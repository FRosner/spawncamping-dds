package de.frosner.dds.core

import java.awt.Desktop
import java.net.URI

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import de.frosner.dds.html.{LockButton, Index, Watermark}
import de.frosner.dds.js.{C3, D3, JQuery, Main, _}
import de.frosner.dds.servables.tabular.Table
import spray.http.MediaTypes._
import spray.routing.SimpleRoutingApp

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
                       launchBrowser: Boolean,
                       interface: String = SprayServer.DEFAULT_INTERFACE,
                       port: Int = SprayServer.DEFAULT_PORT)
  extends SimpleRoutingApp with Server {

  private var servable: Option[Servable] = Option.empty

  private implicit val system = ActorSystem(name + "-system", {
    val conf = ConfigFactory.parseResources("dds.typesafe-conf")
    conf.resolve()
  })

  private val actorName = "chart-server-" + name + "-actor"
  
  def start() = {
    val tryToConnectToSocket = Try(scalaj.http.Http(s"http://$interface:$port").asString)
    if (tryToConnectToSocket.isSuccess) {
      println(s"""$interface:$port is already in use. Server started already? Another server blocking the socket?""")
      println()
      DDS.help("start")
    } else {
      println(s"""Starting server on $interface:$port""")
      val server = startServer(interface, port, actorName) {
        path("") {
          get {
            respondWithMediaType(`text/html`) {
              complete(Index.html)
            }
          }
        } ~
          path("img" / "watermark.svg") {
            get {
              respondWithMediaType(`image/svg+xml`) {
                complete(Watermark.svg)
              }
            }
          } ~
          path("img" / "lock.png") {
            get {
              respondWithMediaType(`image/png`) {
                complete(LockButton.png)
              }
            }
          } ~
          path("lib" / "d3.js") {
            get {
              respondWithMediaType(`application/javascript`) {
                complete(D3.js)
              }
            }
          } ~
          path("lib" / "c3.js") {
            get {
              respondWithMediaType(`application/javascript`) {
                complete(C3.js)
              }
            }
          } ~
          path("css" / "c3.css") {
            get {
              respondWithMediaType(`text/css`) {
                complete(C3.css)
              }
            }
          } ~
          path("lib" / "d3.parcoords.js") {
            get {
              respondWithMediaType(`application/javascript`) {
                complete(PC.js)
              }
            }
          } ~
          path("css" / "d3.parcoords.css") {
            get {
              respondWithMediaType(`text/css`) {
                complete(PC.css)
              }
            }
          } ~
          path("css" / "table.css") {
            get {
              respondWithMediaType(`text/css`) {
                complete(Table.css)
              }
            }
          } ~
          path("css" / "index.css") {
            get {
              respondWithMediaType(`text/css`) {
                complete(Index.css)
              }
            }
          } ~
          path("lib" / "jquery.js") {
            get {
              respondWithMediaType(`application/javascript`) {
                complete(JQuery.js)
              }
            }
          } ~
          path("app" / "main.js") {
            get {
              respondWithMediaType(`application/javascript`) {
                complete(Main.js)
              }
            }
          } ~
          path("chart" / "update") {
            get {
              complete {
                val response = servable.map(_.toJsonString).getOrElse("{}")
                servable = Option.empty
                response
              }
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
    servable = Option.empty
    system.scheduler.scheduleOnce(1.milli)(system.shutdown())(system.dispatcher)
  }

  def serve(servable: Servable) = {
    this.servable = Option(servable)
  }

}

object SprayServer {

  val DEFAULT_INTERFACE = "localhost"
  val DEFAULT_PORT = 8080

  /**
   * Create a server instance bound to default port and interface, and open a browser window once the server is started.
   *
   * @param name of the server
   * @return A server bound to default port and interface.
   */
  def apply(name: String): SprayServer = SprayServer(name, true)

  /**
   * Create a server instance bound to default port and interface, without opening a browser window.
   *
   * @param name of the server
   * @return A server bound to default port and interface.
   */
  def withoutLaunchingBrowser(name: String) = SprayServer(name, false)

}
