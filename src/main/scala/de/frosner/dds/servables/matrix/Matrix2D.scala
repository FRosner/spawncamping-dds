package de.frosner.dds.servables.matrix

import de.frosner.dds.core.Servable
import spray.json._

/**
 * Representation of a matrix. `entries(i)(j)` corresponds to the element in the `i`th row and `j`th column.
 */
case class Matrix2D(entries: Seq[Seq[Double]],
                    rowNames: Seq[String],
                    colNames: Seq[String],
                    title: String = Servable.DEFAULT_TITLE) extends Servable {

  override val servableType: String = "matrix"

  def contentAsJson: JsValue = JsObject(
    ("entries", JsArray(
      entries.map(row => JsArray(
        row.map(value => JsNumber(value)).toVector
      )).toVector
    )),
    ("rowNames", JsArray(rowNames.map(name => JsString(name)).toVector)),
    ("colNames", JsArray(colNames.map(name => JsString(name)).toVector))
  )

}
