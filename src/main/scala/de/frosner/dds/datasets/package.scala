package de.frosner.dds

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.types._
import org.apache.spark.graphx._

import scala.io.Source
import scala.util.Try

package object datasets {

  private def readCsvWithHeader(location: String) = {
    val raw = Source.fromInputStream(this.getClass.getResourceAsStream(location)).getLines().toSeq
    val (Seq(rawHead), rawBody) = raw.splitAt(1)
    (rawHead, rawBody)
  }

  private lazy val readGolf = readCsvWithHeader("/data/golf.csv")

  def golf(implicit sc: SparkContext): RDD[GolfRow] = {
    val (rawHead, rawBody) = readGolf
    sc.parallelize(rawBody).map(line => {
      val split = line.split(",", -1)
      GolfRow(
        outlook = split(0),
        temperature = split(1).toDouble,
        humidity = split(2).toDouble,
        wind = split(3).toBoolean,
        play = split(4) == "yes"
      )
    })
  }

  private lazy val readNetwork = readCsvWithHeader("/data/peer_to_peer_network.csv")

  def gnutella(implicit sc: SparkContext): Graph[Int, String] = {
    val (rawHead, rawBody) = readNetwork

    val edgeRdd = sc.parallelize(rawBody).map(line => {
     val split = line.replaceAll("\"", "").split(",", -1)
      Edge(split(0).toLong, split(1).toLong, "")
    })

    val allVertices = rawBody.flatMap(line => {
      val leftId = line.replaceAll("\"", "").split(",", -1)(0).toInt
      val rightId = line.replaceAll("\"", "").split(",", -1)(1).toInt
      List((leftId.toLong, leftId), (rightId.toLong, rightId))
    }).toSet.toSeq
    val vertexRdd = sc.parallelize(allVertices)

    Graph(vertexRdd, edgeRdd)
  }

  def golf(implicit sc: SparkContext, sql: SQLContext): DataFrame = {
    val (rawHead, rawBody) = readGolf
    val schema = rawHead.split(",", -1).map(columnName => StructField(
      name = columnName,
      dataType = columnName match {
        case "Outlook" => StringType
        case "Temperature" => DoubleType
        case "Humidity" => DoubleType
        case "Wind" => BooleanType
        case "Play" => BooleanType
      },
      nullable = false
    ))
    val data = sc.parallelize(rawBody).map(line => {
      val split = line.split(",", -1)
      Row(split(0), split(1).toDouble, split(2).toDouble, split(3).toBoolean, split(4) == "yes")
    })
    sql.createDataFrame(data, StructType(schema))
  }

  lazy val readFlights = readCsvWithHeader("/data/flights.csv")

  private lazy val flightDateFormat = new SimpleDateFormat("yyyy-MM-dd")
  private lazy val hourMinuteDateFormat = new SimpleDateFormat("HHmm")

  def flights(implicit sc: SparkContext): RDD[FlightsRow] = {
    val (rawHead, rawBody) = readFlights
    sc.parallelize(rawBody.map(line => {
      val split = line.split(",", -1).map(_.replace("\"", ""))
      FlightsRow(
        flightDate = flightDateFormat.parse(split(0)),
        carrier = split(1),
        tailNumber = if (split(2).isEmpty) Option.empty else Option(split(2)),
        flightNumber = split(3),
        originAirport = split(4),
        destinationAirport = split(5),
        crsDepartureTime = hourMinuteDateFormat.parse(split(6)),
        departureTime = Try(hourMinuteDateFormat.parse(split(7))).toOption,
        departureDelay = Try(split(8).toDouble).toOption,
        wheelsOffTime = Try(hourMinuteDateFormat.parse(split(9))).toOption,
        wheelsOnTime = Try(hourMinuteDateFormat.parse(split(10))).toOption,
        crsArrivalTime = hourMinuteDateFormat.parse(split(11)),
        arrivalTime = Try(hourMinuteDateFormat.parse(split(12))).toOption,
        arrivalDelay = Try(split(13).toDouble).toOption,
        airTime = Try(split(14).toDouble).toOption,
        carrierDelay = Try(split(15).toDouble).toOption,
        weatherDelay = Try(split(16).toDouble).toOption,
        nasDelay = Try(split(17).toDouble).toOption,
        securityDelay = Try(split(18).toDouble).toOption,
        lateAircraftDelay = Try(split(19).toDouble).toOption
      )
    }), 100)
  }

  def flights(implicit sc: SparkContext, sql: SQLContext): DataFrame = {
    val (rawHead, rawBody) = readFlights
    val schema = rawHead.split(",").map(_.replace("\"", "")).map(columnName => columnName match {
      case "FL_DATE" => StructField("Flight Date", DateType, false)
      case "UNIQUE_CARRIER" => StructField("Carrier", StringType, false)
      case "TAIL_NUM" => StructField("Tail Number", StringType, true)
      case "FL_NUM" => StructField("Flight Number", StringType, false)
      case "ORIGIN_AIRPORT_ID" => StructField("Origin Airport", StringType, false)
      case "DEST_AIRPORT_ID" => StructField("Destination Airport", StringType, false)
      case "CRS_DEP_TIME" => StructField("CRS Departure Time", DateType, false)
      case "DEP_TIME" => StructField("Departure Time", DateType, true)
      case "DEP_DELAY_NEW" => StructField("Departure Delay", DoubleType, true)
      case "WHEELS_OFF" => StructField("Wheels-Off Time", DateType, true)
      case "WHEELS_ON" => StructField("Wheels-On Time", DateType, true)
      case "CRS_ARR_TIME" => StructField("CRS Arrival Time", DateType, false)
      case "ARR_TIME" => StructField("Arrival Time", DateType, true)
      case "ARR_DELAY_NEW" => StructField("Arrival Delay", DoubleType, true)
      case "AIR_TIME" => StructField("Air Time", DoubleType, true)
      case "CARRIER_DELAY" => StructField("Carrier Delay", DoubleType, true)
      case "WEATHER_DELAY" => StructField("Weather Delay", DoubleType, true)
      case "NAS_DELAY" => StructField("NAS Delay", DoubleType, true)
      case "SECURITY_DELAY" => StructField("Security Delay", DoubleType, true)
      case "LATE_AIRCRAFT_DELAY" => StructField("Late Aircraft Delay", DoubleType, true)
    })
    val data = sc.parallelize(rawBody.map(line => {
      val split = line.split(",", -1).map(_.replace("\"", ""))
      Row(
        flightDateFormat.parse(split(0)),
        split(1),
        if (split(2).isEmpty) null else split(2),
        split(3),
        split(4),
        split(5),
        hourMinuteDateFormat.parse(split(6)),
        Try(hourMinuteDateFormat.parse(split(7))).toOption.getOrElse(null),
        Try(split(8).toDouble).toOption.getOrElse(null),
        Try(hourMinuteDateFormat.parse(split(9))).toOption.getOrElse(null),
        Try(hourMinuteDateFormat.parse(split(10))).toOption.getOrElse(null),
        hourMinuteDateFormat.parse(split(11)),
        Try(hourMinuteDateFormat.parse(split(12))).toOption.getOrElse(null),
        Try(split(13).toDouble).toOption.getOrElse(null),
        Try(split(14).toDouble).toOption.getOrElse(null),
        Try(split(15).toDouble).toOption.getOrElse(null),
        Try(split(16).toDouble).toOption.getOrElse(null),
        Try(split(17).toDouble).toOption.getOrElse(null),
        Try(split(18).toDouble).toOption.getOrElse(null),
        Try(split(19).toDouble).toOption.getOrElse(null)
      )
    }), 100)
    sql.createDataFrame(data, StructType(schema))
  }

}

case class GolfRow(outlook: String,
                   temperature: Double,
                   humidity: Double,
                   wind: Boolean,
                   play: Boolean) {
  // override equals because default implementation using pattern matching does not work in Spark REPL
  override def equals(thatAny: Any): Boolean = {
    if (thatAny.isInstanceOf[GolfRow]) {
      val that = thatAny.asInstanceOf[GolfRow]
      this.outlook == that.outlook &&
        this.temperature == that.temperature &&
        this.humidity == that.humidity &&
        this.wind == that.wind &&
        this.play == that.play
    } else {
      false
    }
  }
}

case class FlightsRow(flightDate: Date,
                      carrier: String,
                      tailNumber: Option[String],
                      flightNumber: String,
                      originAirport: String,
                      destinationAirport: String,
                      crsDepartureTime: Date,
                      departureTime: Option[Date],
                      departureDelay: Option[Double],
                      wheelsOffTime: Option[Date],
                      wheelsOnTime: Option[Date],
                      crsArrivalTime: Date,
                      arrivalTime: Option[Date],
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

