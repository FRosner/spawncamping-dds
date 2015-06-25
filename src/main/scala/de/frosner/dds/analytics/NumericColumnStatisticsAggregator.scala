package de.frosner.dds.analytics

class NumericColumnStatisticsAggregator[N](implicit num: Numeric[N]) extends Serializable {

  private[analytics] var totalCount = 0l
  private[analytics] var missingCount = 0l
  private[analytics] var min = Option.empty[N]
  private[analytics] var max = Option.empty[N]
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
      min = if (min.isDefined) Option(num.min(min.get, actualValue)) else Option(actualValue)
      max = if (max.isDefined) Option(num.max(max.get, actualValue)) else Option(actualValue)
    }
    this
  }

  def merge(intermediateAggregator: NumericColumnStatisticsAggregator[N]): NumericColumnStatisticsAggregator[N] = {
    totalCount = totalCount + intermediateAggregator.totalCount
    missingCount = missingCount + intermediateAggregator.missingCount
    sum = num.plus(sum, intermediateAggregator.sum)
    sumOfSquares = num.plus(sumOfSquares, intermediateAggregator.sumOfSquares)
    if (min.isEmpty)
      min = intermediateAggregator.min
    else if (intermediateAggregator.min.isDefined)
      min = min.map(num.min(_, intermediateAggregator.min.get))
    if (max.isEmpty)
      max = intermediateAggregator.max
    else if (intermediateAggregator.max.isDefined)
      max = max.map(num.max(_, intermediateAggregator.max.get))
    this
  }

}
