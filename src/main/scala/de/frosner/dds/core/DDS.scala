package de.frosner.dds.core

import java.awt.Desktop
import java.net.URI
import java.util.Date

import akka.actor.ActorSystem
import de.frosner.dds.chart.Chart
import de.frosner.dds.html.Index
import de.frosner.dds.js._
import spray.json.JsObject
import spray.routing.SimpleRoutingApp

object DDS extends SimpleRoutingApp {
  
  implicit val system = ActorSystem("dds-system")

  val interface = "localhost"
  val port = 8080

  var chart: Option[Chart] = Option.empty
  
  def start() = {
    println(s"""Starting server on $interface:$port""")
    val server = startServer(interface, port) {
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
    println("Opening browser")
    if (Desktop.isDesktopSupported()) {
      Desktop.getDesktop().browse(new URI( s"""http://$interface:$port/"""))
    }
  }

  def plot(chart: Chart) = {
    this.chart = Option(chart)
  }

}
