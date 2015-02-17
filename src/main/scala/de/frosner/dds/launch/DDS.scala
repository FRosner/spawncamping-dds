package de.frosner.dds.launch

import java.awt.Desktop
import java.net.URI

import akka.actor.ActorSystem
import de.frosner.dds.html.Index
import de.frosner.dds.js.{JQuery, C3, D3, Chart}
import spray.routing.SimpleRoutingApp

object DDS extends SimpleRoutingApp {
  
  implicit val system = ActorSystem("dds-system")

  val interface = "localhost"
  val port = 8080

  var chartIsUpdated = 0
  
  def start() = {
    println(s"""Starting server on $interface:$port""")
    val server = startServer(interface, port) {
      path("hello"){             get{ complete{ Index.html } } } ~
      path("lib" / "d3.js"){     get{ complete{ D3.js } } } ~
      path("lib" / "c3.js"){     get{ complete{ C3.js } } } ~
      path("css" / "c3.css"){    get{ complete{ C3.css } } } ~
      path("lib" / "jquery.js"){ get{ complete{ JQuery.js } } } ~
      path("app" / "chart.js"){  get{ complete{ Chart.js } } } ~
      path("chart" / "update"){  get{ complete{
        val response = chartIsUpdated.toString
        chartIsUpdated = 0
        response
      } } }
    }
    println("Opening browser")
    if (Desktop.isDesktopSupported()) {
      Desktop.getDesktop().browse(new URI( s"""http://$interface:$port/hello"""))
    }
  }

  def plot() = chartIsUpdated = 1

}
