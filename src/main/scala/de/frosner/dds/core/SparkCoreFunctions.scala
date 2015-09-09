package de.frosner.dds.core

import de.frosner.dds.servables.histogram.Histogram
import de.frosner.dds.servables.tabular.{Table, KeyValueSequence}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.util.StatCounter

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

object SparkCoreFunctions {

  private[core] def createBar[V: ClassTag](values: RDD[V], title: String): Option[Servable] = {
    val (distinctValues, distinctCounts) =
      values.map((_, 1)).reduceByKey(_ + _).collect.sortBy{ case (value, count) => count }.reverse.unzip
    ScalaFunctions.createBar(distinctCounts, distinctValues.map(_.toString), Some(title))
  }

  private[core] def createPie[V: ClassTag](values: RDD[V]): Option[Servable] = {
    ScalaFunctions.createPie(values.map((_, 1)).reduceByKey(_ + _).collect)
  }

  private[core] def createHistogram[N: ClassTag](values: RDD[N], numBuckets: Option[Int])
                                                (implicit num: Numeric[N]): Option[Servable] = {
    if (numBuckets.isDefined && numBuckets.get < 2) {
      println("Number of buckets must be greater than or equal to 2")
      Option.empty
    } else {
      val localNumBuckets = if (numBuckets.isEmpty) Histogram.optimalNumberOfBins(values.count) else numBuckets.get
      val tryHist = util.Try(values.map(v => num.toDouble(v)).histogram(localNumBuckets))
      if (tryHist.isSuccess) {
        val (buckets, frequencies) = tryHist.get
        ScalaFunctions.createHistogram(buckets, frequencies)
      } else {
        println("Could not create histogram: " + tryHist.failed.get)
        Option.empty
      }
    }
  }

  private[core] def createHistogram[N1: ClassTag, N2: ClassTag](values: RDD[N1], buckets: Seq[N2])
                                                               (implicit num1: Numeric[N1], num2: Numeric[N2]): Option[Servable] = {
    val frequencies = values.map(v => num1.toLong(v)).histogram(buckets.map(b => num2.toDouble(b)).toArray, false)
    ScalaFunctions.createHistogram(buckets, frequencies)
  }

  private[core] def createPieGroups[K, N](groupValues: RDD[(K, Iterable[N])])
                                   (reduceFunction: (N, N) => N)
                                   (implicit num: Numeric[N]): Option[Servable] = {
    ScalaFunctions.createPie(groupValues.map { case (key, values) => (key, values.reduce(reduceFunction)) }.collect)
  }

  private[core] def createGroupAndPie[K: ClassTag, N: ClassTag](toBeGroupedValues: RDD[(K, N)])
                                                               (reduceFunction: (N, N) => N)
                                                               (implicit num: Numeric[N]): Option[Servable] = {
    ScalaFunctions.createPie(toBeGroupedValues.reduceByKey(reduceFunction).collect.sortBy { case (value, count) => count })
  }

  private[core] def createShow[V](rdd: RDD[V], sampleSize: Int)(implicit tag: TypeTag[V]): Option[Servable] = {
    val vType = tag.tpe
    if (vType <:< typeOf[Row]) {
      // RDD of rows but w/o a schema
      val values = rdd.take(sampleSize).map(_.asInstanceOf[Row].toSeq)
      ScalaFunctions.createTable((1 to values.head.size).map(_.toString), values)
    } else {
      // Normal RDD
      ScalaFunctions.createShow(rdd.take(sampleSize))(tag)
    }
  }

  private[core] def createMedian[N: ClassTag](values: RDD[N])(implicit num: Numeric[N]): Option[Servable] = {
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
      ScalaFunctions.createTable(List("median"), List(List(median)))
    } else {
      println("Median is not defined on an empty RDD!")
      Option.empty
    }
  }

  private[core] def createSummarize[N: ClassTag](values: RDD[N], title: String = Servable.DEFAULT_TITLE)
                                                (implicit num: Numeric[N] = null): Option[Servable] = {
    if (num != null) {
      Option(KeyValueSequence.fromStatCounter(values.stats(), title))
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

  private[core] def createSummarizeGroups[K, N](groupValues: RDD[(K, Iterable[N])])
                                         (implicit num: Numeric[N]): Option[Servable] = {
    val statCounters = groupValues.map{ case (key, values) =>
      (key, StatCounter(values.map(num.toDouble(_))))
    }.map{ case (key, stat) =>
      (key.toString, stat)
    }.collect
    val (labels, stats) = statCounters.unzip
    Option(Table.fromStatCounters(labels, stats))
  }

  private[core] def createGroupAndSummarize[K: ClassTag, N: ClassTag](toBeGroupedValues: RDD[(K, N)])
                                                                     (implicit num: Numeric[N]): Option[Servable] = {
    createSummarizeGroups(toBeGroupedValues.groupByKey())
  }

}
