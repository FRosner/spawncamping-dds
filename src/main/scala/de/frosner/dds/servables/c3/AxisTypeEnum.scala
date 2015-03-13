package de.frosner.dds.servables.c3

/**
 * Constants representing the types of C3 axes available.
 */
object AxisTypeEnum extends Enumeration {

  type AxisType = Value

  val Categorical = Value("category")
  val TimeSeries = Value("timeseries")
  val Indexed = Value("indexed")

}
