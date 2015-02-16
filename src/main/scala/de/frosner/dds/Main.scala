package de.frosner.dds

import java.awt.Desktop
import java.net.URI

import akka.actor.ActorSystem
import spray.routing.SimpleRoutingApp

object Main extends App with SimpleRoutingApp {

  implicit val system = ActorSystem("dds-system")

  val server = startServer(interface = "localhost", port = 8080) {
    path("hello") {
      get {
        complete {
          <h1>Say hello to spray</h1>
        }
      }
    }
  }

  if (Desktop.isDesktopSupported()) {
    Desktop.getDesktop().browse(new URI("http://localhost:8080/hello"))
  }

}
