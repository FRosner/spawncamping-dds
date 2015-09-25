package de.frosner.dds.datasets

import java.sql.Timestamp

case class FlightsRow(flightDate: Timestamp,
                      carrier: String,
                      tailNumber: Option[String],
                      flightNumber: String,
                      originAirport: String,
                      destinationAirport: String,
                      crsDepartureTime: Timestamp,
                      departureTime: Option[Timestamp],
                      departureDelay: Option[Double],
                      wheelsOffTime: Option[Timestamp],
                      wheelsOnTime: Option[Timestamp],
                      crsArrivalTime: Timestamp,
                      arrivalTime: Option[Timestamp],
                      arrivalDelay: Option[Double],
                      airTime: Option[Double],
                      carrierDelay: Option[Double],
                      weatherDelay: Option[Double],
                      nasDelay: Option[Double],
                      securityDelay: Option[Double],
                      lateAircraftDelay: Option[Double]) {
  // override equals because default implementation using pattern matching does not work in Spark REPL
  override def equals(thatAny: Any): Boolean = {
    if (thatAny.isInstanceOf[FlightsRow]) {
      val that = thatAny.asInstanceOf[FlightsRow]
      this.flightDate == that.flightDate &&
        this.carrier == that.carrier &&
        this.tailNumber == that.tailNumber &&
        this.flightNumber == that.flightNumber &&
        this.originAirport == that.originAirport &&
        this.destinationAirport == that.destinationAirport &&
        this.crsDepartureTime == that.crsDepartureTime &&
        this.departureTime == that.departureTime &&
        this.departureDelay == that.departureDelay &&
        this.wheelsOffTime == that.wheelsOffTime &&
        this.wheelsOnTime == that.wheelsOnTime &&
        this.crsArrivalTime == that.crsArrivalTime &&
        this.arrivalTime == that.arrivalTime &&
        this.arrivalDelay == that.arrivalDelay &&
        this.airTime == that.airTime &&
        this.carrierDelay == that.carrierDelay &&
        this.weatherDelay == that.weatherDelay &&
        this.nasDelay == that.nasDelay &&
        this.securityDelay == that.securityDelay &&
        this.lateAircraftDelay == that.lateAircraftDelay
    } else {
      false
    }
  }
}
