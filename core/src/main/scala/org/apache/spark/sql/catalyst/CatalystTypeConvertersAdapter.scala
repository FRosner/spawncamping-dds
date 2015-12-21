package org.apache.spark.sql.catalyst

import org.apache.spark.sql.types.DataType

object CatalystTypeConvertersAdapter {

  def createToCatalystConverter(dataType: DataType): Any => Any =
    CatalystTypeConverters.createToCatalystConverter(dataType)

}
