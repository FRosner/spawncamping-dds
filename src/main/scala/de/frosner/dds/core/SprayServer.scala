package de.frosner.dds.core

import java.awt.Desktop
import java.net.URI

import akka.actor.ActorSystem
import de.frosner.dds.chart.Table
import de.frosner.dds.html.Index
import de.frosner.dds.js.{C3, D3, JQuery, Main}
import spray.routing.SimpleRoutingApp

import scala.concurrent.duration._

case class SprayServer(name: String,
                            launchBrowser: Boolean,
                            interface: String = SprayServer.DEFAULT_INTERFACE,
                            port: Int = SprayServer.DEFAULT_PORT)
  extends SimpleRoutingApp with Server {

  private var servable: Option[Servable] = Option.empty

  private implicit val system = ActorSystem(name + "-system")

  private val actorName = "chart-server-" + name + "-actor"
  
  def start() = {
    println(s"""Starting server on $interface:$port""")
    val server = startServer(interface, port, actorName) {
      path(""){                  get{ complete{ Index.html } } } ~
      path("lib" / "d3.js"){     get{ complete{ D3.js } } } ~
      path("lib" / "c3.js"){     get{ complete{ C3.js } } } ~
      path("css" / "c3.css"){    get{ complete{ C3.css } } } ~
      path("css" / "table.css"){ get{ complete{ Table.css } } } ~
      path("lib" / "jquery.js"){ get{ complete{ JQuery.js } } } ~
      path("app" / "main.js"){   get{ complete{ Main.js } } } ~
      path("chart" / "update"){  get{ complete{
        val response = servable.map(_.toJsonString).getOrElse("{}")
        servable = Option.empty
        response
      } } }
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

  val DEFAULT_INTERFACE = "localhost"
  val DEFAULT_PORT = 8080

  def apply(name: String): SprayServer = SprayServer(name, true)

  def withoutLaunchingBrowser(name: String) = SprayServer(name, false)

}
