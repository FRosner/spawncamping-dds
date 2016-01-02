package de.frosner.dds

import java.text.SimpleDateFormat

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

  private lazy val readGolf = readCsvWithHeader("/de/frosner/dds/datasets/golf.csv")

  def golf(sc: SparkContext): RDD[GolfRow] = {
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

  private lazy val readNetwork = readCsvWithHeader("/de/frosner/dds/datasets/enron.csv")

  def enron(sc: SparkContext): Graph[Int, String] = {
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

  def golf(sql: SQLContext): DataFrame = {
    import sql.implicits._
    golf(sql.sparkContext).toDF
  }

  lazy val readFlights = readCsvWithHeader("/de/frosner/dds/datasets/flights.csv")

  private lazy val flightDateFormat = new SimpleDateFormat("yyyy-MM-dd")
  private lazy val hourMinuteDateFormat = new SimpleDateFormat("HHmm")

  def flights(sc: SparkContext): RDD[FlightsRow] = {
    val (rawHead, rawBody) = readFlights
    sc.parallelize(rawBody.map(line => {
      val split = line.split(",", -1).map(_.replace("\"", ""))
      FlightsRow(
        flightDate = new java.sql.Timestamp(flightDateFormat.parse(split(0)).getTime),
        carrier = split(1),
        tailNumber = if (split(2).isEmpty) Option.empty else Option(split(2)),
        flightNumber = split(3),
        originAirport = split(4),
        destinationAirport = split(5),
        crsDepartureTime = new java.sql.Timestamp(hourMinuteDateFormat.parse(split(6)).getTime),
        departureTime = Try(new java.sql.Timestamp(hourMinuteDateFormat.parse(split(7)).getTime)).toOption,
        departureDelay = Try(split(8).toDouble).toOption,
        wheelsOffTime = Try(new java.sql.Timestamp(hourMinuteDateFormat.parse(split(9)).getTime)).toOption,
        wheelsOnTime = Try(new java.sql.Timestamp(hourMinuteDateFormat.parse(split(10)).getTime)).toOption,
        crsArrivalTime = new java.sql.Timestamp(hourMinuteDateFormat.parse(split(11)).getTime),
        arrivalTime = Try(new java.sql.Timestamp(hourMinuteDateFormat.parse(split(12)).getTime)).toOption,
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

  def flights(sql: SQLContext): DataFrame = {
    import sql.implicits._
    flights(sql.sparkContext).toDF
  }

}





