package de.frosner.dds.datasets

import java.util.{GregorianCalendar, Calendar}

import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}

import scala.collection.mutable.ArrayBuffer

class DatasetsTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  private var sc: SparkContext = _
  private var sql: SQLContext = _

  override def beforeAll() = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName(this.getClass.toString)
    sc = new SparkContext(conf)
    sql = new SQLContext(sc)
  }

  override def afterAll() = {
    sc.stop()
  }

  "Golf CSV SchemaRDD" should "have the correct schema" in {
    golf(sql).schema shouldBe StructType(List(
      StructField("outlook", StringType, true),
      StructField("temperature", DoubleType, false),
      StructField("humidity", DoubleType, false),
      StructField("wind", BooleanType, false),
      StructField("play", BooleanType, false)
    ))
  }

  it should "have the correct data" in {
    golf(sql).collect shouldBe Array(
      Row("sunny",85,85,false,false),
      Row("sunny",80,90,true,false),
      Row("overcast",83,78,false,true),
      Row("rain",70,96,false,true),
      Row("rain",68,80,false,true),
      Row("rain",65,70,true,false),
      Row("overcast",64,65,true,true),
      Row("sunny",72,95,false,false),
      Row("sunny",69,70,false,true),
      Row("rain",75,80,false,true),
      Row("sunny",75,70,true,true),
      Row("overcast",72,90,true,true),
      Row("overcast",81,75,false,true),
      Row("rain",71,80,true,false)
    )
  }

  "Golf CSV case class RDD" should "have the correct data" in {
    golf(sc).collect shouldBe Array(
      GolfRow("sunny",85,85,false,false),
      GolfRow("sunny",80,90,true,false),
      GolfRow("overcast",83,78,false,true),
      GolfRow("rain",70,96,false,true),
      GolfRow("rain",68,80,false,true),
      GolfRow("rain",65,70,true,false),
      GolfRow("overcast",64,65,true,true),
      GolfRow("sunny",72,95,false,false),
      GolfRow("sunny",69,70,false,true),
      GolfRow("rain",75,80,false,true),
      GolfRow("sunny",75,70,true,true),
      GolfRow("overcast",72,90,true,true),
      GolfRow("overcast",81,75,false,true),
      GolfRow("rain",71,80,true,false)
    )
  }

  "Flights CSV case class RDD" should "have the correct data" in {
    val flightsArray = flights(sc).collect
    flightsArray.head shouldBe FlightsRow(
      flightDate = new java.sql.Timestamp(new GregorianCalendar(2015, Calendar.JANUARY, 1).getTimeInMillis),
      carrier = "AA",
      tailNumber = Option("N3KEAA"),
      flightNumber = "41",
      originAirport = "13930",
      destinationAirport = "14747",
      crsDepartureTime = new java.sql.Timestamp(new GregorianCalendar(1970, Calendar.JANUARY, 1, 20, 55).getTimeInMillis),
      departureTime = Option(new java.sql.Timestamp(new GregorianCalendar(1970, Calendar.JANUARY, 1, 20, 54).getTimeInMillis)),
      departureDelay = Option(0d),
      wheelsOffTime = Option(new java.sql.Timestamp(new GregorianCalendar(1970, Calendar.JANUARY, 1, 21, 10).getTimeInMillis)),
      wheelsOnTime = Option(new java.sql.Timestamp(new GregorianCalendar(1970, Calendar.JANUARY, 1, 23, 6).getTimeInMillis)),
      crsArrivalTime = new java.sql.Timestamp(new GregorianCalendar(1970, Calendar.JANUARY, 1, 23, 25).getTimeInMillis),
      arrivalTime = Option(new java.sql.Timestamp(new GregorianCalendar(1970, Calendar.JANUARY, 1, 23, 47).getTimeInMillis)),
      arrivalDelay = Option(22d),
      airTime = Option(236d),
      carrierDelay = Option(0d),
      weatherDelay = Option(0d),
      nasDelay = Option(22d),
      securityDelay = Option(0d),
      lateAircraftDelay = Option(0d)
    )
    flightsArray(979) shouldBe FlightsRow(
      flightDate = new java.sql.Timestamp(new GregorianCalendar(2015, Calendar.JANUARY, 2).getTimeInMillis),
      carrier = "DL",
      tailNumber = Option("N549US"),
      flightNumber = "1398",
      originAirport = "12892",
      destinationAirport = "14747",
      crsDepartureTime = new java.sql.Timestamp(new GregorianCalendar(1970, Calendar.JANUARY, 1, 8, 45).getTimeInMillis),
      departureTime = Option.empty,
      departureDelay = Option.empty,
      wheelsOffTime = Option.empty,
      wheelsOnTime = Option.empty,
      crsArrivalTime = new java.sql.Timestamp(new GregorianCalendar(1970, Calendar.JANUARY, 1, 11, 37).getTimeInMillis),
      arrivalTime = Option.empty,
      arrivalDelay = Option.empty,
      airTime = Option.empty,
      carrierDelay = Option.empty,
      weatherDelay = Option.empty,
      nasDelay = Option.empty,
      securityDelay = Option.empty,
      lateAircraftDelay = Option.empty
    )
    flightsArray.size shouldBe 18441
  }

  "Flights CSV SchemaRDD" should "have the correct schema" in {
    flights(sql).schema shouldBe StructType(ArrayBuffer(
      StructField("flightDate", TimestampType, true),
      StructField("carrier", StringType, true),
      StructField("tailNumber", StringType, true),
      StructField("flightNumber", StringType, true),
      StructField("originAirport", StringType, true),
      StructField("destinationAirport", StringType, true),
      StructField("crsDepartureTime", TimestampType, true),
      StructField("departureTime", TimestampType, true),
      StructField("departureDelay", DoubleType, true),
      StructField("wheelsOffTime", TimestampType, true),
      StructField("wheelsOnTime", TimestampType, true),
      StructField("crsArrivalTime", TimestampType, true),
      StructField("arrivalTime", TimestampType, true),
      StructField("arrivalDelay", DoubleType, true),
      StructField("airTime", DoubleType, true),
      StructField("carrierDelay", DoubleType, true),
      StructField("weatherDelay", DoubleType, true),
      StructField("nasDelay", DoubleType, true),
      StructField("securityDelay", DoubleType, true),
      StructField("lateAircraftDelay", DoubleType, true)
    ))
  }

  it should "have the correct data" in {
    val flightsArray = flights(sql).collect
    flightsArray.head shouldBe Row(
      new java.sql.Timestamp(new GregorianCalendar(2015, Calendar.JANUARY, 1).getTimeInMillis),
      "AA",
      "N3KEAA",
      "41",
      "13930",
      "14747",
      new java.sql.Timestamp(new GregorianCalendar(1970, Calendar.JANUARY, 1, 20, 55).getTimeInMillis),
      new java.sql.Timestamp(new GregorianCalendar(1970, Calendar.JANUARY, 1, 20, 54).getTimeInMillis),
      0d,
      new java.sql.Timestamp(new GregorianCalendar(1970, Calendar.JANUARY, 1, 21, 10).getTimeInMillis),
      new java.sql.Timestamp(new GregorianCalendar(1970, Calendar.JANUARY, 1, 23, 6).getTimeInMillis),
      new java.sql.Timestamp(new GregorianCalendar(1970, Calendar.JANUARY, 1, 23, 25).getTimeInMillis),
      new java.sql.Timestamp(new GregorianCalendar(1970, Calendar.JANUARY, 1, 23, 47).getTimeInMillis),
      22d,
      236d,
      0d,
      0d,
      22d,
      0d,
      0d
    )
    flightsArray(979) shouldBe Row(
      new java.sql.Timestamp(new GregorianCalendar(2015, Calendar.JANUARY, 2).getTimeInMillis),
      "DL",
      "N549US",
      "1398",
      "12892",
      "14747",
      new java.sql.Timestamp(new GregorianCalendar(1970, Calendar.JANUARY, 1, 8, 45).getTimeInMillis),
      null,
      null,
      null,
      null,
      new java.sql.Timestamp(new GregorianCalendar(1970, Calendar.JANUARY, 1, 11, 37).getTimeInMillis),
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null
    )
    flightsArray.size shouldBe 18441
  }

  "Enron email communication network CSV case class RDD" should "have the correct data" in {
    val network = enron(sc)
    network.edges.count() shouldBe 367662

    val edgesFrom6 = network.triplets.filter(triplet =>
      triplet.srcId == 6L
    )
    edgesFrom6.count() shouldBe 9

    val destinationsFrom6 = edgesFrom6.map(triplet =>
      triplet.dstId
    )
    destinationsFrom6.collect().toSet shouldBe Set(1L, 3L, 7L, 50L, 74L, 308L, 878L, 910L, 10606L)
  }
}
