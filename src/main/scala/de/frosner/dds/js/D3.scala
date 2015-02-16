package de.frosner.dds.js

import de.frosner.dds.util.StringResource

import scala.io.Source

object D3 {

  lazy val js = StringResource.read("/js/d3.v3.min.js")

}
