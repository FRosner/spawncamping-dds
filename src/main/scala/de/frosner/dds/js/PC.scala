package de.frosner.dds.js

import de.frosner.dds.util.StringResource

object PC {

  lazy val js = StringResource.read("/js/d3.parcoords.js")
  lazy val css = StringResource.read("/css/d3.parcoords.css")

}
