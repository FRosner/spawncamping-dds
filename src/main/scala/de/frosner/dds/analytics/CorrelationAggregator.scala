package de.frosner.dds.analytics

import scala.collection.mutable

class CorrelationAggregator(val numColumns: Int) extends Serializable {

  require(numColumns > 0, "You need to pass a positive number of columns to use the aggregator.")

  private[analytics] var counts: mutable.Map[(Int, Int), Int] =
    initializeMapWith(numColumns)(0)
  private[analytics] var sums: mutable.Map[(Int, Int), (Double, Double)] =
    initializeMapWith(numColumns)((0d, 0d))
  private[analytics] var sumsOfSquares: mutable.Map[(Int, Int), (Double, Double)] =
    initializeMapWith(numColumns)((0d, 0d))
  private[analytics] var sumsOfPairs: mutable.Map[(Int, Int), Double] =
    initializeMapWith(numColumns)(0d)

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
        val (sum1, sum2) = sums(idx1, idx2)
        sums.update((idx1, idx2), (sum1 + value1, sum2 + value2))
        val (sumOfSquare1, sumOfSquare2) = sumsOfSquares(idx1, idx2)
        sumsOfSquares.update((idx1, idx2), (sumOfSquare1 + value1 * value1, sumOfSquare2 + value2 * value2))
        sumsOfPairs(idx1, idx2) += value1 * value2
        val count = counts(idx1, idx2)
        counts.update((idx1, idx2), count + 1)
      }
    this
  }

  def merge(intermediateAggregator: CorrelationAggregator): CorrelationAggregator = {
    require(numColumns == intermediateAggregator.numColumns)
    for (((i, j), (sumI, sumJ)) <- sums) {
      val (intermediatSumI, intermediateSumJ) = intermediateAggregator.sums((i, j))
      sums.update((i, j), (sumI + intermediatSumI, sumJ + intermediateSumJ))
    }
    for (((i, j), (sumOfSquaresI, sumOfSquaresJ)) <- sumsOfSquares) {
      val (intermediateSumOfSquaresI, intermediateSumOfSquaresJ) = intermediateAggregator.sumsOfSquares((i, j))
      sumsOfSquares.update((i, j), (sumOfSquaresI + intermediateSumOfSquaresI, sumOfSquaresJ + intermediateSumOfSquaresJ))
    }
    for (((i, j), sum) <- intermediateAggregator.sumsOfPairs) {
      sumsOfPairs(i, j) += sum
    }
    for (((i, j), count) <- counts) {
      val intermediateCount = intermediateAggregator.counts((i, j))
      counts.update((i, j), count + intermediateCount)
    }
    this
  }

  private def means: Map[(Int, Int), (Double, Double)] = sums.map{ case ((i, j), (sumI, sumJ)) => {
    val count = counts((i, j))
    (i, j) -> (sumI / count, sumJ / count)
  }}.toMap

  def pearsonCorrelations: Map[(Int, Int), Double] = {
    val curMeans = means
    val halfOfTheMatrix = sumsOfPairs.map { case ((idx1, idx2), sumOfPairs) => {
      val (mean1, mean2) = curMeans((idx1, idx2))
      val (sumOfSquare1, sumOfSquare2) = sumsOfSquares((idx1, idx2))
      val count = counts(idx1, idx2)
      ((idx1, idx2),
        (sumOfPairs - count * mean1 * mean2) /
          (Math.sqrt(sumOfSquare1 - count * mean1 * mean1) *
            Math.sqrt(sumOfSquare2 - count * mean2 * mean2))
        )
    }}
    val diagonalOfTheMatrix = (0 to numColumns - 1).map(idx => ((idx, idx), 1d))
    val otherHalfOfTheMatrix = halfOfTheMatrix.map { case ((idx1, idx2), correlation) => ((idx2, idx1), correlation) }
    (halfOfTheMatrix ++ diagonalOfTheMatrix ++ otherHalfOfTheMatrix).toMap
  }

  private def initializeMapWith[V](size: Int)(zeroValue: V) =
    mutable.HashMap.empty[(Int, Int), V].++(
      for (i <- 0 to size - 1; j <- 0 to size - 1; if i < j) yield ((i, j), zeroValue)
    )

}
