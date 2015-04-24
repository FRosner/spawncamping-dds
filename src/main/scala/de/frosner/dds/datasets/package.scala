package de.frosner.dds

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SchemaRDD, SQLContext}
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.catalyst.types._

import scala.io.Source

package object datasets {

  private lazy val readGolf = {
    val raw = Source.fromInputStream(this.getClass.getResourceAsStream("/data/golf.csv")).getLines().toSeq
    val (Seq(rawHead), rawBody) = raw.splitAt(1)
    (rawHead, rawBody)
  }

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

  def golf(implicit sc: SparkContext, sql: SQLContext): SchemaRDD = {
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
    sql.applySchema(data, StructType(schema))
  }

}

case class GolfRow(outlook: String, temperature: Double, humidity: Double, wind: Boolean, play: Boolean) {
  // override equals because default implementation using pattern matching does not work in Spark REPL
  override def equals(thatAny: Any): Boolean = {
    if (thatAny.isInstanceOf[GolfRow]) {
      val that = thatAny.asInstanceOf[GolfRow]
      this.outlook == that.outlook && this.temperature == that.temperature && this.humidity == that.humidity &&
        this.wind == that.wind && this.play == that.play
    } else {
      false
    }
  }
}
