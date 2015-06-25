package de.frosner.dds.analytics

import scala.math.Integral

class NumericColumnStatisticsAggregator[N](implicit num: Numeric[N]) extends Serializable {

  private[analytics] var totalCount = 0l
  private[analytics] var missingCount = 0l
  private[analytics] var sum = num.zero
  private[analytics] var sumOfSquares = num.zero

  def iterate(value: Option[N]): NumericColumnStatisticsAggregator[N] = {
    totalCount = totalCount + 1
    if (value.isEmpty) {
      missingCount = missingCount + 1
    } else {
      val actualValue = value.get
      sum = num.plus(sum, actualValue)
      sumOfSquares = num.plus(sumOfSquares, num.times(actualValue, actualValue))
    }
    this
  }

  def merge(intermediateAggregator: NumericColumnStatisticsAggregator[N]): NumericColumnStatisticsAggregator[N] = {
    totalCount = totalCount + intermediateAggregator.totalCount
    missingCount = missingCount + intermediateAggregator.missingCount
    sum = num.plus(sum, intermediateAggregator.sum)
    sumOfSquares = num.plus(sumOfSquares, intermediateAggregator.sumOfSquares)
    this
  }

}
