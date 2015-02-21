package de.frosner.dds.core

import de.frosner.dds.chart.{Stats, Chart}

trait ChartServer {

  def start()

  def stop()

  def serve(servable: Servable)

}
