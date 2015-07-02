package de.frosner.dds.analytics

class NominalColumnStatisticsAggregator extends Serializable {

  private[analytics] var runningTotalCount = 0l
  private[analytics] var runningMissingCount = 0l

  def iterate(value: Option[Any]): NominalColumnStatisticsAggregator = {
    runningTotalCount = runningTotalCount + 1
    if (value.isEmpty) {
      runningMissingCount = runningMissingCount + 1
    }
    this
  }

  def merge(intermediateAggregator: NominalColumnStatisticsAggregator): NominalColumnStatisticsAggregator = {
    runningTotalCount = runningTotalCount + intermediateAggregator.runningTotalCount
    runningMissingCount = runningMissingCount + intermediateAggregator.runningMissingCount
    this
  }

  def totalCount = runningTotalCount

  def missingCount = runningMissingCount

  def nonMissingCount = totalCount - missingCount

}
