package de.frosner.dds

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SchemaRDD, SQLContext}
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.catalyst.types._

import scala.io.Source

package object datasets {

  def golf(implicit sc: SparkContext, sql: SQLContext): SchemaRDD = {
    val raw = Source.fromInputStream(this.getClass.getResourceAsStream("/data/golf.csv")).getLines().toSeq
    val (Seq(rawHead), rawBody) = raw.splitAt(1)
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
