package de.frosner.dds.analytics

import scala.collection.mutable

class MutualInformationAggregator(val numColumns: Int) extends Serializable {

  require(numColumns > 0, "You need to pass a positive number of columns to use the aggregator.")

  private[analytics] var columnCounts: mutable.ArrayBuffer[mutable.Map[Any, Long]] =
    mutable.ArrayBuffer.empty ++ List.fill(numColumns)(mutable.HashMap.empty[Any, Long])

  private[analytics] var crossColumnCounts: mutable.Map[(Int, Int), mutable.Map[(Any, Any), Long]] = {
    var pXY = mutable.HashMap.empty[(Int, Int), mutable.Map[(Any, Any), Long]]
    for (i <- 0 to numColumns - 1; j <- 0 to numColumns - 1; if i <= j) {
      pXY.put((i, j), mutable.HashMap.empty)
    }
    pXY
  }

  def iterate(columns: Seq[Any]): MutualInformationAggregator = {
    require(columns.size == numColumns)
    for ((value, counts) <- columns.zip(columnCounts)) {
      counts.get(value) match {
        case Some(count) => counts.update(value, count + 1)
        case None => counts.put(value, 1)
      }
    }
    val columnsWithIndex = columns.zipWithIndex
    for ((value1, idx1) <- columnsWithIndex; (value2, idx2) <- columnsWithIndex; if idx1 <= idx2) {
      val crossCounts = crossColumnCounts((idx1, idx2))
      crossCounts.get((value1, value2)) match {
        case Some(crossCount) => crossCounts.update((value1, value2), crossCount + 1)
        case None => crossCounts.put((value1, value2), 1)
      }
    }
    this
  }

  def merge(intermediateAggregator: MutualInformationAggregator): MutualInformationAggregator = {
    require(numColumns == intermediateAggregator.numColumns)
    for ((counts, intermediateCounts) <- columnCounts.zip(intermediateAggregator.columnCounts);
         (intermediateValue, intermediateCount) <- intermediateCounts) {
      counts.get(intermediateValue) match {
        case Some(count) => counts.update(intermediateValue, count + intermediateCount)
        case None => counts.put(intermediateValue, intermediateCount)
      }
    }
    for (((idx1, idx2), intermediateCrossCounts) <- intermediateAggregator.crossColumnCounts;
         crossCounts = crossColumnCounts((idx1, idx2));
         ((value1, value2), intermediateCrossCount) <- intermediateCrossCounts) {
      crossCounts.get((value1, value2)) match {
        case Some(crossCount) => crossCounts.update((value1, value2), crossCount + intermediateCrossCount)
        case None => crossCounts.put((value1, value2), intermediateCrossCount)
      }
    }
    this
  }

  def mutualInformation: Map[(Int, Int), Double] = {
    val totalCount = columnCounts.head.map{ case (key, value) => value }.sum.toDouble
    val columnProbabilities = columnCounts.map(counts => {
      counts.mapValues(_ / totalCount)
    })
    val mutualColumnInformation = crossColumnCounts.map{ case ((idx1, idx2), crossCounts) => {
      val crossInformationCounts = crossCounts.map{ case ((value1, value2), crossCount) => {
        val column1Probabilities = columnProbabilities(idx1)
        val column2Probabilities = columnProbabilities(idx2)
        val crossProbability = crossCount / totalCount
        val crossInformation = crossProbability *
          (Math.log(crossProbability) - Math.log(column1Probabilities(value1)) - Math.log(column2Probabilities(value2)))
        ((value1, value2), crossInformation)
      }}
      val mutualInformation = crossInformationCounts.map{ case (key, value) => value }.sum
      ((idx1, idx2), mutualInformation)
    }}
    val otherHalfMutualColumnInfo = for (((idx1, idx2), mutualInformation) <- mutualColumnInformation; if idx1 != idx2)
       yield ((idx2, idx1), mutualInformation)
    (mutualColumnInformation ++ otherHalfMutualColumnInfo).toMap
  }

  def mutualInformationMetric: Map[(Int, Int), Double] = {
    val mutualInformationMatrix = mutualInformation
    for (((i, j), mi) <- mutualInformationMatrix)
      yield ((i, j), (mi / Math.max(mutualInformationMatrix(i,i), mutualInformationMatrix(j,j))))
  }

}

object MutualInformationAggregator {
  val NO_NORMALIZATION = "none"
  val METRIC_NORMALIZATION = "metric"
  val DEFAULT_NORMALIZATION = METRIC_NORMALIZATION

  def isValidNormalization(normalization: String) = Set(NO_NORMALIZATION, METRIC_NORMALIZATION).contains(normalization)
}
