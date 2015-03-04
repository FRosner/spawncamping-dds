package de.frosner.dds.core

trait Server {

  def start()

  def stop()

  def serve(servable: Servable)

}
