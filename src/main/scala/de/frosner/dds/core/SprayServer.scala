package de.frosner.dds.core

import java.awt.Desktop
import java.net.URI

import akka.actor.ActorSystem
import com.typesafe.config.{ConfigFactory, ConfigUtil, Config}
import de.frosner.dds.core.SprayServer._
import de.frosner.dds.html.{Watermark, Index}
import de.frosner.dds.js.{C3, D3, JQuery, Main}
import de.frosner.dds.servables.tabular.Table
import spray.http.HttpHeaders.`Content-Type`
import spray.http.{ContentType, HttpCharsets, MediaTypes}
import spray.http.MediaTypes._
import spray.routing.SimpleRoutingApp

import scala.concurrent.duration._

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
    val conf = ConfigFactory.parseResources("dds-akka.conf")
    conf.resolve()
  })

  private val actorName = "chart-server-" + name + "-actor"
  
  def start() = {
    println(s"""Starting server on $interface:$port""")
    val server = startServer(interface, port, actorName) {
      path("") {
        get {
          respondWithMediaType(`text/html`) {
            complete(Index.html)
          }
        }
      } ~
      path("img" / "watermark.svg"){
        get {
          respondWithMediaType(`image/svg+xml`) {
            complete(Watermark.svg)
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
      path("css" / "table.css") {
        get {
          respondWithMediaType(`text/css`) {
            complete(Table.css)
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

//  val `image/svg+xml` = ContentType(MediaTypes.`image/svg+xml`, HttpCharsets.`UTF-8`)

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
