package de.frosner.dds.core

import de.frosner.dds.chart.Chart

trait ChartServer {

  def start()

  def stop()

  def serve(chart: Chart)

}
