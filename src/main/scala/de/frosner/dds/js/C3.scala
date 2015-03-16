package de.frosner.dds.js

import de.frosner.dds.util.StringResource

object C3 {

  lazy val js = StringResource.read("/ui/lib/c3.min.js")
  lazy val css = StringResource.read("/ui/css/c3.css")

}
