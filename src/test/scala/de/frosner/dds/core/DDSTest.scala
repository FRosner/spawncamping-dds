package de.frosner.dds.core

import de.frosner.dds.chart.Chart
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfter, Matchers, FlatSpec}

class DDSTest extends FlatSpec with Matchers with MockFactory with BeforeAndAfter {

  private var server: ChartServer = _

  before {
    server = stub[ChartServer]
  }

  after {
    DDS.resetServer()
  }

  "DDS" should "start the chart server when start() is executed" in {
    DDS.start(server)
    (server.start _).verify().once()
  }

  it should "tear the server down when stop() is executed" in {
    DDS.start(server)
    DDS.stop()
    (server.stop _).verify().once()
  }

  it should "not start another server if one is started already" in {
    DDS.start(server)
    DDS.start(server)
    (server.start _).verify().once()
  }

  it should "do nothing when stopped the second time" in {
    DDS.start(server)
    DDS.stop()
    DDS.stop()
    (server.stop _).verify().once()
  }

}
