package de.frosner.dds.analytics

class NumericColumnStatisticsAggregator extends Serializable {

  private var counts: NominalColumnStatisticsAggregator = new NominalColumnStatisticsAggregator()
  private var runningMin = Double.PositiveInfinity
  private var runningMax = Double.NegativeInfinity
  private var runningSum = 0d
  private var runningMean = 0d
  private var runningMeanSquareDif = 0d

  def iterate(value: Option[Double]): NumericColumnStatisticsAggregator = {
    counts = counts.iterate(value)
    if (value.isDefined) {
      val actualValue = value.get
      val delta = actualValue - runningMean
      runningSum = runningSum + actualValue
      runningMean = runningMean + delta / nonMissingCount
      runningMeanSquareDif = runningMeanSquareDif + delta * (actualValue - runningMean)
      runningMin = Math.min(runningMin, actualValue)
      runningMax = Math.max(runningMax, actualValue)
    }
    this
  }

  def merge(that: NumericColumnStatisticsAggregator): NumericColumnStatisticsAggregator = {
    runningSum = runningSum + that.runningSum
    val delta = that.runningMean - runningMean
    runningMin = Math.min(runningMin, that.runningMin)
    runningMax = Math.max(runningMax, that.runningMax)
    runningMean = (runningMean * nonMissingCount + that.runningMean * that.nonMissingCount) /
      (nonMissingCount + that.nonMissingCount)
    runningMeanSquareDif = runningMeanSquareDif + that.runningMeanSquareDif +
      (delta * delta * nonMissingCount * that.nonMissingCount) /
      (nonMissingCount + that.nonMissingCount)
    counts = counts.merge(that.counts)
    this
  }

  def totalCount = counts.totalCount

  def missingCount = counts.missingCount

  def nonMissingCount = counts.nonMissingCount

  def sum = runningSum

  def min = runningMin

  def max = runningMax

  def mean = if (nonMissingCount > 0) runningMean else Double.NaN

  def variance = if (nonMissingCount > 1) runningMeanSquareDif / nonMissingCount else Double.NaN

  def stdev = Math.sqrt(variance)

}
