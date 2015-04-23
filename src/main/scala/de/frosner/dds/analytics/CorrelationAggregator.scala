package de.frosner.dds.analytics

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable

class CorrelationAggregator(val numColumns: Int) extends Serializable {

  require(numColumns > 0, "You need to pass a positive number of columns to use the aggregator.")

  private[analytics] var count: Int = 0
  private[analytics] var sums: mutable.Seq[Double] = new ArrayBuffer(numColumns) ++ List.fill(numColumns)(0d)
  private[analytics] var sumsOfSquares: mutable.Seq[Double] = new ArrayBuffer(numColumns) ++ List.fill(numColumns)(0d)
  private[analytics] var sumsOfPairs: mutable.Map[(Int, Int), Double] = mutable.HashMap.empty[(Int, Int), Double].++(
    for (i <- 0 to numColumns - 1; j <- 0 to numColumns - 1; if i < j) yield ((i, j), 0d))

  def iterate(columns: Seq[Double]): CorrelationAggregator = {
    require(columns.size == numColumns)
    val columnsWithIndex = columns.zipWithIndex
    for ((column, idx) <- columnsWithIndex) {
      sums(idx) += column
      sumsOfSquares(idx) += column * column
    }
    for ((column1, idx1) <- columnsWithIndex; (column2, idx2) <- columnsWithIndex; if idx1 < idx2)
      sumsOfPairs(idx1, idx2) += column1 * column2
    count += 1
    this
  }

  def merge(intermediateAggregator: CorrelationAggregator): CorrelationAggregator = {
    require(numColumns == intermediateAggregator.numColumns)
    for ((intermediateSum, idx) <- intermediateAggregator.sums.zipWithIndex)
      sums(idx) += intermediateSum
    for ((intermediateSumOfSquares, idx) <- intermediateAggregator.sumsOfSquares.zipWithIndex)
      sumsOfSquares(idx) += intermediateSumOfSquares
    for (((i, j), sum) <- intermediateAggregator.sumsOfPairs)
      sumsOfPairs(i, j) += sum
    count += intermediateAggregator.count
    this
  }

  def means: Seq[Double] = sums.map(_ / count)

  def pearsonCorrelations: Map[(Int, Int), Double] = {
    val curMeans = means
    val halfOfTheMatrix = sumsOfPairs.map { case ((idx1, idx2), sumOfPairs) => ((idx1, idx2),
        (sumOfPairs - count * curMeans(idx1) * curMeans(idx2)) /
          (Math.sqrt(sumsOfSquares(idx1) - count * curMeans(idx1) * curMeans(idx1)) *
            Math.sqrt(sumsOfSquares(idx2) - count * curMeans(idx2) * curMeans(idx2)))
        )
    }
    val diagonalOfTheMatrix = (0 to numColumns - 1).map(idx => ((idx, idx), 1d))
    val otherHalfOfTheMatrix = halfOfTheMatrix.map { case ((idx1, idx2), correlation) => ((idx2, idx1), correlation) }
    (halfOfTheMatrix ++ diagonalOfTheMatrix ++ otherHalfOfTheMatrix).toMap
  }

}
