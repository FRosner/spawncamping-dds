package de.frosner.dds.core

trait ChartServer {

  def start()

  def stop()

  def serve(servable: Servable)

}
