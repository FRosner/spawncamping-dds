package de.frosner.dds.core

import java.awt.Desktop
import java.net.URI

import akka.actor.ActorSystem
import de.frosner.dds.chart.Chart
import de.frosner.dds.html.Index
import de.frosner.dds.js.{Main, JQuery, C3, D3}
import spray.routing.SimpleRoutingApp
import scala.concurrent.duration._

object ChartServer extends SimpleRoutingApp {

  private var chart: Option[Chart] = Option.empty

  // package private var to allow multiple systems in unit test environment
  private[core] implicit var system = ActorSystem("dds-system")

  // only for testing
  private[core] var isInTestMode = false
  private[core] var actorName = "chart-server-actor"

  val interface = "localhost"
  val port = 8080

  def start() = {
    println(s"""Starting server on $interface:$port""")
    val server = startServer(interface, port, actorName) {
      path(""){                  get{ complete{ Index.html } } } ~
      path("lib" / "d3.js"){     get{ complete{ D3.js } } } ~
      path("lib" / "c3.js"){     get{ complete{ C3.js } } } ~
      path("css" / "c3.css"){    get{ complete{ C3.css } } } ~
      path("lib" / "jquery.js"){ get{ complete{ JQuery.js } } } ~
      path("app" / "main.js"){   get{ complete{ Main.js } } } ~
      path("chart" / "update"){  get{ complete{
        val response = chart.map(_.toJsonString).getOrElse("{}")
        chart = Option.empty
        response
      } } }
    }

    Thread.sleep(1000)
    if (!isInTestMode && Desktop.isDesktopSupported()) {
      println("Opening browser")
      Desktop.getDesktop().browse(new URI( s"""http://$interface:$port/"""))
    }
  }

  def stop() = {
    println("Stopping server")
    chart = Option.empty
    system.scheduler.scheduleOnce(1.milli)(system.shutdown())(system.dispatcher)
  }

  def serve(chart: Chart) = {
    this.chart = Option(chart)
  }

}
