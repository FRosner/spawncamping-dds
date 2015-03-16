package de.frosner.dds.js

import de.frosner.dds.util.StringResource

object PC {

  lazy val js = StringResource.read("/ui/lib/d3.parcoords.js")
  lazy val css = StringResource.read("/ui/css/d3.parcoords.css")

}
