package de.frosner.dds.util

import scala.io.Source

object StringResource {

  def read(resourceLocation: String) =
    Source.fromInputStream(this.getClass.getResourceAsStream(resourceLocation)).mkString

}
