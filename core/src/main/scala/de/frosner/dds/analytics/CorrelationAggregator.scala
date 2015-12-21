package de.frosner.dds.analytics

import scala.collection.mutable

class CorrelationAggregator(val numColumns: Int) extends Serializable {

  require(numColumns > 0, "You need to pass a positive number of columns to use the aggregator.")

  private[analytics] var aggregators: mutable.Map[(Int, Int), (NumericColumnStatisticsAggregator, NumericColumnStatisticsAggregator)] =
    initializeMapWith(numColumns)((new NumericColumnStatisticsAggregator(), new NumericColumnStatisticsAggregator()))

  private[analytics] var runningCov: mutable.Map[(Int, Int), (Double)] = initializeMapWith(numColumns)(0d)

  def iterateWithoutNulls(columns: Seq[Double]): CorrelationAggregator = {
    iterate(columns.map(d => Option(d)))
  }

  def iterate(columns: Seq[Option[Double]]): CorrelationAggregator = {
    require(columns.size == numColumns)
    val columnsWithIndex = columns.zipWithIndex
    for ((column1, idx1) <- columnsWithIndex; (column2, idx2) <- columnsWithIndex; if idx1 < idx2)
      if (column1.isDefined && column2.isDefined) {
        val value1 = column1.get
        val value2 = column2.get

        val (agg1, agg2) = aggregators(idx1, idx2)
        val prevCount = agg1.nonMissingCount.toDouble
        val (prevMean1, prevMean2) = (agg1.mean, agg2.mean)
        agg1.iterate(Option(value1))
        agg2.iterate(Option(value2))
        val newCount = agg1.nonMissingCount.toDouble
        val prevCov = runningCov((idx1, idx2))
        val newCov = if (prevCount != 0d)
            (prevCov * prevCount + prevCount / newCount * (value1 - prevMean1) * (value2 - prevMean2)) / newCount
          else
            0d
        runningCov.update((idx1, idx2), newCov)
      }
    this
  }

  def merge(intermediateAggregator: CorrelationAggregator): CorrelationAggregator = {
    require(numColumns == intermediateAggregator.numColumns)

    for (((i, j), (agg1, agg2)) <- aggregators) {
      val (intermediateAgg1, intermediateAgg2) = intermediateAggregator.aggregators((i, j))
      val count = agg1.nonMissingCount.toDouble
      val intermediateCount = intermediateAgg1.nonMissingCount.toDouble
      val coMoment = runningCov((i, j)) * count
      val intermediateCoMoment = intermediateAggregator.runningCov((i, j)) * intermediateCount
      val mergedCov = if (agg1.totalCount == 0) {
        intermediateCoMoment / intermediateCount
      } else if (intermediateAgg1.totalCount == 0) {
        coMoment / count
      } else {
        val totalCount = (agg1.nonMissingCount + intermediateAgg1.nonMissingCount).toDouble
        (coMoment + intermediateCoMoment + (agg1.mean - intermediateAgg1.mean) * (agg2.mean - intermediateAgg2.mean) *
          (agg1.nonMissingCount * intermediateAgg1.nonMissingCount / totalCount)) / totalCount
      }
      runningCov.update((i, j), mergedCov)
      aggregators.update((i, j), (agg1.merge(intermediateAgg1), agg2.merge(intermediateAgg2)))
    }
    this
  }

  private def means: Map[(Int, Int), (Double, Double)] = aggregators.map{
    case ((i, j), (aggI, aggJ)) => (i, j) -> (aggI.mean, aggJ.mean)
  }.toMap

  def pearsonCorrelations: Map[(Int, Int), Double] = {
    val curMeans = means
    val halfOfTheMatrix = aggregators.map{ case ((idx1, idx2), (agg1, agg2)) => {
      ((idx1, idx2), runningCov((idx1, idx2)) / (agg1.stdev * agg2.stdev))
    }}
    val diagonalOfTheMatrix = (0 to numColumns - 1).map(idx => ((idx, idx), 1d))
    val otherHalfOfTheMatrix = halfOfTheMatrix.map { case ((idx1, idx2), correlation) => ((idx2, idx1), correlation) }
    (halfOfTheMatrix ++ diagonalOfTheMatrix ++ otherHalfOfTheMatrix).toMap
  }

  private def initializeMapWith[V](size: Int)(zeroValue: => V) =
    mutable.HashMap.empty[(Int, Int), V].++(
      for (i <- 0 to size - 1; j <- 0 to size - 1; if i < j) yield ((i, j), zeroValue)
    )

}
