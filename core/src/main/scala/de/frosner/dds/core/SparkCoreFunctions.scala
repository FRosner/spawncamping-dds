package de.frosner.dds.core

import de.frosner.dds.servables.{KeyValueSequence, Histogram, Servable}
import de.frosner.dds.util.ServableUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.util.StatCounter

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

object SparkCoreFunctions {

  private[core] def createBar[V: ClassTag](values: RDD[V], seriesName: String, title: String): Option[Servable] = {
    val (distinctValues, distinctCounts) =
      values.map((_, 1)).reduceByKey(_ + _).collect.sortBy{ case (value, count) => count }.reverse.unzip
    ScalaFunctions.createBar(distinctCounts.map(_.toDouble), distinctValues.map(_.toString), seriesName, title)
  }

  private[core] def createPie[V: ClassTag](values: RDD[V], title: String): Option[Servable] = {
    val keyCounts = values.map((_, 1)).reduceByKey(_ + _)
    val collectedKeyCounts = keyCounts.map{ case (segment, count) => (segment.toString, count.toDouble)}.collect
    ScalaFunctions.createPie(collectedKeyCounts, title)
  }

  private[core] def createHistogram[N: ClassTag](values: RDD[N], numBuckets: Option[Int], title: String)
                                                (implicit num: Numeric[N]): Option[Servable] = {
    if (numBuckets.isDefined && numBuckets.get < 2) {
      println("Number of buckets must be greater than or equal to 2")
      Option.empty
    } else {
      val localNumBuckets = if (numBuckets.isEmpty) ServableUtils.optimalNumberOfBins(values.count) else numBuckets.get
      val tryHist = util.Try(values.map(v => num.toDouble(v)).histogram(localNumBuckets))
      if (tryHist.isSuccess) {
        val (buckets, frequencies) = tryHist.get
        ScalaFunctions.createHistogram(buckets, frequencies, title)
      } else {
        println("Could not create histogram: " + tryHist.failed.get)
        Option.empty
      }
    }
  }

  private[core] def createHistogram[N1, N2](values: RDD[N1], buckets: Seq[N2], title: String)
                                           (implicit num1: Numeric[N1], num2: Numeric[N2]): Option[Servable] = {
    val doubleBuckets = buckets.map(num2.toDouble(_)).toArray
    val frequencies = values.map(v => num1.toLong(v)).histogram(doubleBuckets, false)
    ScalaFunctions.createHistogram(
      bins = doubleBuckets,
      frequencies = frequencies,
      title = title
    )
  }

  private[core] def createPieGroups[K, N](groupValues: RDD[(K, Iterable[N])], title: String)
                                         (reduceFunction: (N, N) => N)
                                         (implicit num: Numeric[N]): Option[Servable] = {
    ScalaFunctions.createPie(groupValues.map { case (key, values) => {
      (key.toString, num.toDouble(values.reduce(reduceFunction)))
    } }.collect, title)
  }

  private[core] def createGroupAndPie[K: ClassTag, N: ClassTag](toBeGroupedValues: RDD[(K, N)], title: String)
                                                               (reduceFunction: (N, N) => N)
                                                               (implicit num: Numeric[N]): Option[Servable] = {
    val keyValuePairs = toBeGroupedValues.reduceByKey(reduceFunction).map {
      case (key, value) => (key.toString, num.toDouble(value))
    }.collect
    val sortedKeyValuePairs = keyValuePairs.sortBy { case (value, count) => count }
    ScalaFunctions.createPie(keyValuePairs, title)
  }

  private[core] def createShow[V](rdd: RDD[V], sampleSize: Int, title: String)
                                 (implicit tag: TypeTag[V]): Option[Servable] = {
    ScalaFunctions.createShow(rdd.take(sampleSize), title)(tag)
  }

  private[core] def createMedian[N: ClassTag](values: RDD[N], title: String)
                                             (implicit num: Numeric[N]): Option[Servable] = {
    val sorted = values.sortBy(identity).zipWithIndex().map{
      case (v, idx) => (idx, v)
    }
    val count = sorted.count
    if (count > 0) {
      val median: Double = if (count % 2 == 0) {
        val r = count / 2
        val l = r - 1
        num.toDouble(num.plus(sorted.lookup(l).head, sorted.lookup(r).head)) * 0.5
      } else {
        num.toDouble(sorted.lookup(count / 2).head)
      }
      ScalaFunctions.createTable(StructType(List(StructField("median", DoubleType, false))), List(Row(median)), title)
    } else {
      println("Median is not defined on an empty RDD!")
      Option.empty
    }
  }

  private[core] def createSummarize[N: ClassTag](values: RDD[N], title: String)
                                                (implicit num: Numeric[N] = null): Option[Servable] = {
    if (num != null) {
      Option(ServableUtils.statCounterToKeyValueSequence(values.stats(), title))
    } else {
      val cardinality = values.distinct.count
      if (cardinality > 0) {
        val valueCounts = values.map((_, 1)).reduceByKey(_ + _)
        val (mode, modeCount) = valueCounts.max()(Ordering.by { case (value, count) => count})
        ScalaFunctions.createKeyValuePairs(
          List(
            ("Mode", mode),
            ("Cardinality", cardinality)
          ), title
        )
      } else {
        println("Summarize function requires a non-empty RDD!")
        Option.empty
      }
    }
  }

  private[core] def createSummarizeGroups[K, N](groupValues: RDD[(K, Iterable[N])], title: String)
                                               (implicit num: Numeric[N]): Option[Servable] = {
    val statCounters = groupValues.map{ case (key, values) =>
      (key, StatCounter(values.map(num.toDouble(_))))
    }.map{ case (key, stat) =>
      (key.toString, stat)
    }.collect
    val (labels, stats) = statCounters.unzip
    Option(ServableUtils.statCountersToTable(labels, stats, title))
  }

  private[core] def createGroupAndSummarize[K: ClassTag, N: ClassTag](toBeGroupedValues: RDD[(K, N)], title: String)
                                                                     (implicit num: Numeric[N]): Option[Servable] = {
    createSummarizeGroups(toBeGroupedValues.groupByKey(), title)
  }

}
