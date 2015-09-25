package de.frosner.dds.core

import de.frosner.dds.servables.Servable
import Server.TransformedServable

trait Server {

  def init(): Unit

  def tearDown(): Unit

  def serve(servable: Servable): TransformedServable

}

object Server {

  type TransformedServable = Any

}
