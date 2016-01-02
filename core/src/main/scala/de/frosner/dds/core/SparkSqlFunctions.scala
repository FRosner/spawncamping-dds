package de.frosner.dds.core

import de.frosner.dds.analytics.{DateColumnStatisticsAggregator, ColumnsStatisticsAggregator, MutualInformationAggregator, CorrelationAggregator}
import de.frosner.dds.servables.{Composite, Blank, Servable}
import de.frosner.dds.util.{ServableUtils, DataFrameUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, FloatType, IntegerType, DoubleType}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import de.frosner.dds.util.DataFrameUtils._

object SparkSqlFunctions {

  private[core] def createBar(dataFrame: DataFrame, nullValue: Any = null): Option[Servable] = {
    requireSingleColumned(dataFrame, "bar") {
      val field = dataFrame.schema.fields.head
      val fieldName = field.name
      val title = s"Bar chart of ${field.name}"
      val rdd = dataFrame.rdd
      (field.nullable) match {
        case true => SparkCoreFunctions.createBar(rdd.map(row => if (nullValue == null) {
            if (row.isNullAt(0)) Option.empty[Any] else Option(row(0))
          } else {
            if (row.isNullAt(0)) nullValue else row(0)
          }
        ), fieldName, title)
        case false => SparkCoreFunctions.createBar(rdd.map(row => row(0)), fieldName, title)
      }
    }
  }

  private[core] def createPie(dataFrame: DataFrame, nullValue: Any = null): Option[Servable] = {
    requireSingleColumned(dataFrame, "pie") {
      val field = dataFrame.schema.fields.head
      val title = s"Pie chart of ${field.name}"
      val rdd = dataFrame.rdd
      (field.nullable) match {
        case true => SparkCoreFunctions.createPie(rdd.map(row => if (nullValue == null) {
            if (row.isNullAt(0)) Option.empty[Any] else Option(row(0))
          } else {
            if (row.isNullAt(0)) nullValue else row(0)
          }
        ), title)
        case false => SparkCoreFunctions.createPie(rdd.map(row => row(0)), title)
      }
    }
  }

  private[core] def createHistogram[N: ClassTag](dataFrame: DataFrame, buckets: Seq[N])
                                                (implicit num: Numeric[N]): Option[Servable] = {
    createSomethingOnNumericColumn(dataFrame, "Histogram") {
      (doubleRdd, title) => SparkCoreFunctions.createHistogram(doubleRdd, buckets.map(num.toDouble(_)), title)
    }
  }

  private[core] def createHistogram(dataFrame: DataFrame, numBuckets: Option[Int]): Option[Servable] = {
    createSomethingOnNumericColumn(dataFrame, "Histogram") {
      (doubleRdd, title) => SparkCoreFunctions.createHistogram(doubleRdd, numBuckets, title)
    }
  }

  private[core] def createShow(dataFrame: DataFrame,
                               sampleSize: Int): Option[Servable] = {
    val schema = dataFrame.schema
    val fields = schema.fields
    val title = s"""Sample of ${DataFrameUtils.dfToString(dataFrame)}"""
    val values = dataFrame.take(sampleSize)
    ScalaFunctions.createTable(schema, values, title)
  }

  private[core] def createCorrelation(dataFrame: DataFrame): Option[Servable] = {
    def showError = println("Correlation only supported for RDDs with multiple numerical columns.")
    val schema = dataFrame.schema
    val fields = schema.fields
    val title = s"Correlation of ${DataFrameUtils.dfToString(dataFrame)}"
    if (fields.size >= 2) {
      val numericalFields = fields.zipWithIndex.filter{ case (field, idx) => {
        val dataType = field.dataType
        dataType == DoubleType || dataType == FloatType || dataType == IntegerType || dataType == LongType
      }}
      val numericalFieldIndexes = numericalFields.map{ case (field, idx) => idx }.toSet
      if (numericalFields.length >= 2) {
        val corrAgg = dataFrame.rdd.aggregate(new CorrelationAggregator(numericalFields.length)) (
          (agg, row) => {
            val numericalCells = row.toSeq.zipWithIndex.filter{ case (element, idx) => numericalFieldIndexes.contains(idx) }
            val numericalValues = numericalCells.zip(numericalFields).map {
              case ((element, elementIdx), (elementType, typeIdx)) =>
                require(elementIdx == typeIdx, s"Element index ($elementIdx) did not equal type index ($typeIdx)")
                val dataType = elementType.dataType
                if (element == null)
                  Option.empty[Double]
                else
                  Option[Double](
                    if (dataType == DoubleType) element.asInstanceOf[Double]
                    else if (dataType == FloatType) element.asInstanceOf[Float].toDouble
                    else if (dataType == IntegerType) element.asInstanceOf[Int].toDouble
                    else if (dataType == LongType) element.asInstanceOf[Long].toDouble
                    else element.toString.toDouble // fall back, should not happen
                  )
            }
            agg.iterate(numericalValues)
          },
          (agg1, agg2) => agg1.merge(agg2)
        )
        var corrMatrix: mutable.Seq[mutable.Seq[Double]] = new ArrayBuffer(corrAgg.numColumns) ++
          List.fill(corrAgg.numColumns)(new ArrayBuffer[Double](corrAgg.numColumns) ++
            List.fill(corrAgg.numColumns)(0d))
        for (((i, j), corr) <- corrAgg.pearsonCorrelations) {
          corrMatrix(i)(j) = corr
        }
        val fieldNames = numericalFields.map{ case (field, idx) => field.name }
        ScalaFunctions.createHeatmap(
          values = corrMatrix,
          rowNames = fieldNames,
          colNames = fieldNames,
          zColorZeroes = Seq(-1d, 0d, 1d),
          title = title
        )
      } else {
        showError
        Option.empty
      }
    } else {
      showError
      Option.empty
    }
  }

  private[core] def createMutualInformation(dataFrame: DataFrame,
                                            normalization: String = MutualInformationAggregator.DEFAULT_NORMALIZATION): Option[Servable] = {
    import MutualInformationAggregator.{isValidNormalization, DEFAULT_NORMALIZATION, METRIC_NORMALIZATION, NO_NORMALIZATION}
    def showError = println("Mutual information only supported for RDDs with at least one column.")

    // bin all numerical fields by first converting them to double and then perform equal-width binning using sturges
    val schema = dataFrame.schema
    val fields = schema.fields
    val numericalFields = fields.zipWithIndex.filter{ case (field, idx) => isNumeric(field.dataType) }
    val dfWithAllNumericColumnsAsDouble = dataFrame.select(
      fields.map(field => {
        if (isNumeric(field.dataType)) {
          new Column(field.name).cast(DoubleType).as(field.name)
        } else {
          new Column(field.name)
        }
      }):_*
    )
    val convertNanToNull = udf((d: Double) => if (d.isNaN) null.asInstanceOf[Double] else d)
    val (numericMinMaxCountIndexes, numericMinMaxCountColumns) = fields.zipWithIndex.flatMap{case (field, idx) => {
      import org.apache.spark.sql.functions._
      val currentColumn = new Column(field.name)
      val currentColumnWithoutNaN = convertNanToNull(currentColumn) // otherwise the max and min are NaN
      if (isNumeric(field.dataType)) {
        List(
          ((idx, 0), max(currentColumnWithoutNaN).as(field.name + "_max")),
          ((idx, 1), min(currentColumnWithoutNaN).as(field.name + "_min")),
          ((idx, 2), count(currentColumn).as(field.name + "_count"))
        )
      } else {
        List.empty
      }
    }}.unzip
    val numericMinMaxCountValues = dfWithAllNumericColumnsAsDouble.select(numericMinMaxCountColumns:_*).collect
    val numericMinMaxCountValuesMap = numericMinMaxCountIndexes.zip(numericMinMaxCountValues.head.toSeq).toMap
    val dfWithBinnedDoubleValues = dfWithAllNumericColumnsAsDouble.select(
      fields.zipWithIndex.map{ case (field, idx) => {
        if (isNumeric(field.dataType)) {
          val max = numericMinMaxCountValuesMap((idx, 0)).asInstanceOf[Double]
          val min = numericMinMaxCountValuesMap((idx, 1)).asInstanceOf[Double]
          val count = numericMinMaxCountValuesMap((idx, 2)).asInstanceOf[Long]
          val optimalNumBins = ServableUtils.optimalNumberOfBins(count)
          DataFrameUtils.binDoubleUdf(optimalNumBins, min, max)(new Column(field.name))
        } else {
          new Column(field.name)
        }
      }}:_*
    )

    // compute mutual information matrix
    val binnedFields = dfWithBinnedDoubleValues.schema.fields
    if (fields.size >= 1) {
      val miAgg = dfWithBinnedDoubleValues.rdd.aggregate(new MutualInformationAggregator(binnedFields.size)) (
        (agg, row) => agg.iterate(row.toSeq),
        (agg1, agg2) => agg1.merge(agg2)
      )
      var mutualInformationMatrix: mutable.Seq[mutable.Seq[Double]] = new ArrayBuffer(miAgg.numColumns) ++
        List.fill(miAgg.numColumns)(
          new ArrayBuffer[Double](miAgg.numColumns) ++ List.fill(miAgg.numColumns)(0d)
        )

      val actualNormalization = if (isValidNormalization(normalization)) {
        normalization
      } else {
        println(s"""Not a valid normalization method: $normalization. Falling back to $DEFAULT_NORMALIZATION.""")
        DEFAULT_NORMALIZATION
      }

      val title = s"Mutual Information ($actualNormalization) of ${DataFrameUtils.dfToString(dataFrame)}"

      for (((i, j), mi) <- actualNormalization match {
        case METRIC_NORMALIZATION => miAgg.mutualInformationMetric
        case NO_NORMALIZATION => miAgg.mutualInformation
      }) {
        mutualInformationMatrix(i)(j) = mi
      }

      val fieldNames = fields.map(_.name)
      ScalaFunctions.createHeatmap(
        values = mutualInformationMatrix,
        rowNames = fieldNames,
        colNames = fieldNames,
        zColorZeroes = Seq(0d),
        title = title
      )
    } else {
      showError
      Option.empty
    }
  }

  private[core] def createMedian(dataFrame: DataFrame): Option[Servable] = {
    createSomethingOnNumericColumn(dataFrame, "Median") {
      (doubleRdd, title) => SparkCoreFunctions.createMedian(doubleRdd, title)
    }
  }

  private[core] def createSummarize(dataFrame: DataFrame): Option[Servable] = {
    val title = s"Summary of ${DataFrameUtils.dfToString(dataFrame)}"
    val columnStatistics = dataFrame.rdd.aggregate(ColumnsStatisticsAggregator(dataFrame.schema))(
      (agg, row) => agg.iterate(row),
      (agg1, agg2) => agg1.merge(agg2)
    )

    val numericColumnStatistics = columnStatistics.numericColumns
    val numericFields = getNumericFields(dataFrame)
    val numericServables = for ((index, field) <- numericFields) yield {
      val hist = createHistogram(dataFrame.select(new Column(field.name)), Option(10))
      val (agg, _) = numericColumnStatistics(index)
      val table = ScalaFunctions.createKeyValuePairs(
        title = s"Summary statistics of ${field.name}",
        pairs = List(
          ("Total Count", agg.totalCount),
          ("Missing Count", agg.missingCount),
          ("Non-Missing Count", agg.nonMissingCount),
          ("Sum", agg.sum),
          ("Min", agg.min),
          ("Max", agg.max),
          ("Mean", agg.mean),
          ("Stdev", agg.stdev),
          ("Variance", agg.variance)
        )
      )
      Option((index, List(table.getOrElse(Blank), hist.getOrElse(Blank))))
    }

    val dateColumnStatistics = columnStatistics.dateColumns
    val dateFields = getDateFields(dataFrame)
    val dateServables = for ((index, field) <- dateFields) yield {
      val (agg, _) = dateColumnStatistics(index)
      val (years, yearFrequencies) = agg.yearFrequencies.toList.sortBy(_._1).map { case (year, count) => {
        (DateColumnStatisticsAggregator.calendarYearToString(year), count.toDouble)
      }}.unzip
      val yearBar = ScalaFunctions.createBar(yearFrequencies, years, field.name, s"Years in ${field.name}")
      val (months, monthFrequencies) = agg.monthFrequencies.toList.sortBy(_._1).map { case (month, count) => {
        (DateColumnStatisticsAggregator.calendarMonthToString(month), count.toDouble)
      }}.unzip
      val monthBar = ScalaFunctions.createBar(monthFrequencies, months, field.name, s"Months in ${field.name}")
      val (days, dayFrequencies) = agg.dayOfWeekFrequencies.toList.sortBy(_._1).map { case (day, count) => {
        (DateColumnStatisticsAggregator.calendarDayToString(day), count.toDouble)
      }}.unzip
      val dayBar = ScalaFunctions.createBar(dayFrequencies, days, field.name, s"Days in ${field.name}")
      val table = ScalaFunctions.createKeyValuePairs(List(
        ("Total Count", agg.totalCount),
        ("Missing Count", agg.missingCount),
        ("Non-Missing Count", agg.nonMissingCount),
        ("Top Year", agg.topYear match { case (year, count) => (DateColumnStatisticsAggregator.calendarYearToString(year), count) }),
        ("Top Month", agg.topMonth match { case (month, count) => (DateColumnStatisticsAggregator.calendarMonthToString(month), count) }),
        ("Top Day", agg.topDayOfWeek match { case (day, count) => (DateColumnStatisticsAggregator.calendarDayToString(day), count) })
      ), s"Summary statistics of ${field.name}")
      if (table.isDefined && yearBar.isDefined && monthBar.isDefined && dayBar.isDefined) {
        Option((index, List(table.get, yearBar.get, monthBar.get, dayBar.get)))
      } else {
        Option.empty
      }
    }

    val nominalColumnStatistics = columnStatistics.nominalColumns
    val nominalFields = getNominalFields(dataFrame)
    val nominalServables = for ((index, field) <- nominalFields) yield {
      val groupCounts = dataFrame.groupBy(new Column(field.name)).count.map(row =>
        (if (row.isNullAt(0)) "NULL" else row.get(0).toString, row.getLong(1))
      )
      val cardinality = groupCounts.count
      val orderedCounts = groupCounts.sortBy(x => x._2, ascending = false)
      val mode = orderedCounts.first
      val barTitle = s"Bar of ${field.name}"
      val barPlot = if (cardinality <= 10) {
        val (values, counts) = orderedCounts.collect.unzip
        ScalaFunctions.createBar(counts.map(_.toDouble), values, field.name, barTitle)
      } else {
        val (top10Values, top10Counts) = orderedCounts.take(10).unzip
        val top10CountsSum = top10Counts.sum
        val totalCountsSum = orderedCounts.map { case (value, counts) => counts }.reduce(_ + _)
        val otherCount = totalCountsSum - top10CountsSum
        ScalaFunctions.createBar(
          values = top10Counts.map(_.toDouble) ++ List(otherCount.toDouble),
          categories = top10Values ++ List("..."),
          seriesName = field.name,
          title = barTitle
        )
      }
      val (agg, _) = nominalColumnStatistics(index)
      val table = ScalaFunctions.createKeyValuePairs(List(
        ("Total Count", agg.totalCount),
        ("Missing Count", agg.missingCount),
        ("Non-Missing Count", agg.nonMissingCount),
        ("Mode", mode),
        ("Cardinality", cardinality)
      ), s"Summary statistics of ${field.name}")
      if (table.isDefined && barPlot.isDefined) {
        Option((index, List(table.get, barPlot.get)))
      } else {
        Option.empty
      }
    }

    if (numericServables.forall(_.isDefined) && dateServables.forall(_.isDefined) && nominalServables.forall(_.isDefined)) {
      val allServables = numericServables.map(_.get) ++ dateServables.map(_.get) ++ nominalServables.map(_.get)
      val sortedServables = allServables.toSeq.sortBy(_._1)
      Option(Composite(title, sortedServables.map { case (index, servables) => servables }))
    } else {
      println("Failed to create summary statistics")
      Option.empty
    }
  }

  private[core] def createDashboard(dataFrame: DataFrame): Option[Servable] = {
    val rdd = dataFrame.rdd
    val fields = dataFrame.schema.fields
    val title = s"Dashboard of ${DataFrameUtils.dfToString(dataFrame)}"

    def toCell(maybeServable: Option[Servable]) = maybeServable.map(servable => List(servable)).getOrElse(List.empty)
    Option(Composite(title, List(
      toCell(createShow(dataFrame, DDS.DEFAULT_SHOW_SAMPLE_SIZE)),
      toCell(createCorrelation(dataFrame)) ++ toCell(createMutualInformation(dataFrame)),
      toCell(createSummarize(dataFrame))
    )))
  }

  private def createSomethingOnNumericColumn(dataFrame: DataFrame, name: String)
                                            (creator: (RDD[Double], String) => Option[Servable]): Option[Servable] = {
    requireSingleColumned(dataFrame, name) {
      val field = dataFrame.schema.fields.head
      val title = s"$name on ${field.name}"
      if (isNumeric(field.dataType)) {
        val rdd = dataFrame.select(new Column(field.name).cast(DoubleType)).rdd
        val doubleRdd = if (field.nullable)
          rdd.flatMap(row => if (row.isNullAt(0)) Option.empty[Double] else Option(row.getDouble(0)))
        else
          rdd.map(row => row.getDouble(0))
        creator(doubleRdd, title)
      } else {
        println(s"$name only supported for numerical columns.")
        Option.empty
      }
    }
  }

}
