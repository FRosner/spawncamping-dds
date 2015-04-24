package de.frosner.dds.datasets

import de.frosner.dds.GolfRow
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}

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
    golf(sc, sql).schema shouldBe StructType(List(
      StructField("Outlook", StringType, false),
      StructField("Temperature", DoubleType, false),
      StructField("Humidity", DoubleType, false),
      StructField("Wind", BooleanType, false),
      StructField("Play", BooleanType, false)
    ))
  }

  it should "have the correct data" in {
    golf(sc, sql).collect shouldBe Array(
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

}
