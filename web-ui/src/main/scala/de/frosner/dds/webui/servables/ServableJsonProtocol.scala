package de.frosner.dds.webui.servables

import javax.xml.bind.DatatypeConverter

import de.frosner.dds.servables._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import spray.json._

object ServableJsonProtocol extends DefaultJsonProtocol {

  val TYPE_KEY = "type"
  val SERVABLE_KEY = "servable"
  val TITLE_KEY = "title"

  val BAR_CHART_TYPE = "bar"
  val PIE_CHART_TYPE = "pie"
  val HISTOGRAM_TYPE = "histogram"
  val TABLE_TYPE = "table"
  val HEATMAP_TYPE = "heatmap"
  val GRAPH_TYPE = "graph"
  val SCATTER_PLOT_TYPE = "scatter"
  val KEY_VALUE_SEQUENCE_TYPE = "keyValueSequence"
  val COMPOSITE_TYPE = "composite"
  val BLANK_TYPE = "blank"

  private val barChartFormat = DefaultJsonProtocol.jsonFormat4(BarChart)

  private val pieChartFormat = DefaultJsonProtocol.jsonFormat2(PieChart)

  private val histogramFormat = DefaultJsonProtocol.jsonFormat3(Histogram)

  // TODO column names are not only part of the schema, not of every row (row != JsObject) like before => JS code needs adjustments
  private object TableFormat extends RootJsonFormat[Table] {

    private val Array(titleName, schemaName, contentName) = extractFieldNames(classManifest[Table])
    private val columnNameName = "name"
    private val columnTypeName = "type"
    private val columnNullableName = "nullable"
    private val keyNameType = "key"
    private val valueNameType = "value"

    private def valueToJsValue(row: Row)(dataType: DataType, index: Int): JsValue = dataType match {
      case ByteType =>
        if (row.isNullAt(index)) JsNull else JsNumber(row.getByte(index))
      case ShortType =>
        if (row.isNullAt(index)) JsNull else JsNumber(row.getShort(index))
      case IntegerType =>
        if (row.isNullAt(index)) JsNull else JsNumber(row.getInt(index))
      case LongType =>
        if (row.isNullAt(index)) JsNull else JsNumber(row.getLong(index))
      case FloatType =>
        if (row.isNullAt(index)) JsNull else JsNumber(row.getFloat(index))
      case DoubleType =>
        if (row.isNullAt(index)) JsNull else JsNumber(row.getDouble(index))
      case decimalType: DecimalType =>
        if (row.isNullAt(index)) JsNull else JsNumber(row.getDecimal(index))
      case StringType =>
        if (row.isNullAt(index)) JsNull else JsString(row.getString(index))
      case BinaryType =>
        if (row.isNullAt(index)) JsNull else JsString(DatatypeConverter.printBase64Binary(row.getAs[Array[Byte]](index)))
      case BooleanType =>
        if (row.isNullAt(index)) JsNull else JsBoolean(row.getBoolean(index))
      case TimestampType =>
        if (row.isNullAt(index)) JsNull else JsNumber(row.getAs[java.sql.Timestamp](index).getTime)
      case DateType =>
        if (row.isNullAt(index)) JsNull else JsNumber(row.getDate(index).getTime)
      case ArrayType(elementType, containsNull) =>
        if (row.isNullAt(index)) JsNull else {
          val innerSeq = row.getSeq[Any](index)
          val innerRow = Row.fromSeq(innerSeq)
          JsArray((0 until innerSeq.size).map(index => valueToJsValue(innerRow)(elementType, index)).toVector)
        }
      case MapType(keyType, valueType, valueContainsNull) =>
        if (row.isNullAt(index)) JsNull else {
          val (keys, values) = row.getMap(index).asInstanceOf[Map[Any, Any]].toSeq.unzip
          val (keysRow, valuesRow) = (Row.fromSeq(keys), Row.fromSeq(values))
          val keyValuePairs = for (i <- 0 until keys.size) yield {
            JsObject(
              (keyNameType, valueToJsValue(keysRow)(keyType, i)),
              (valueNameType, valueToJsValue(valuesRow)(valueType, i))
            )
          }
          JsArray(keyValuePairs.toVector)
        }
      case structType: StructType =>
        if (row.isNullAt(index)) JsNull else rowToJsArray(row.getStruct(index), structType)
      case _ =>
        if (row.isNullAt(index)) JsNull else JsString(row.get(index).toString)
    }

    private def rowToJsArray(row: Row, schema: StructType): JsArray = {
      JsArray(
        schema.fields.zipWithIndex.map { case (field, index) =>
          valueToJsValue(row)(field.dataType, index)
        }.toVector
      )
    }

    private def nullOrElse[T, U <: JsValue](jsValue: JsValue)(elseValue: U => T): T =
      if (jsValue == JsNull) null.asInstanceOf[T] else elseValue(jsValue.asInstanceOf[U])

    private def jsValueToValue(dataType: DataType, nullable: Boolean)(jsValue: JsValue): Any = (dataType, nullable) match {
      case (ByteType, true) =>
        nullOrElse[java.lang.Byte, JsNumber](jsValue)(jsValue => jsValue.value.toByteExact)
      case (ByteType, false) =>
        jsValue.asInstanceOf[JsNumber].value.toByteExact
      case (ShortType, true) =>
        nullOrElse[java.lang.Short, JsNumber](jsValue)(jsValue => jsValue.value.toShortExact)
      case (ShortType, false) =>
        jsValue.asInstanceOf[JsNumber].value.toShortExact
      case (IntegerType, true) =>
        nullOrElse[java.lang.Integer, JsNumber](jsValue)(jsValue => jsValue.value.toIntExact)
      case (IntegerType, false) =>
        jsValue.asInstanceOf[JsNumber].value.toIntExact
      case (LongType, true) =>
        nullOrElse[java.lang.Long, JsNumber](jsValue)(jsValue => jsValue.value.toLongExact)
      case (LongType, false) =>
        jsValue.asInstanceOf[JsNumber].value.toLongExact
      case (FloatType, true) =>
        nullOrElse[java.lang.Float, JsNumber](jsValue)(jsValue => jsValue.value.toFloat)
      case (FloatType, false) =>
        jsValue.asInstanceOf[JsNumber].value.toFloat
      case (DoubleType, true) =>
        nullOrElse[java.lang.Double, JsNumber](jsValue)(jsValue => jsValue.value.toDouble)
      case (DoubleType, false) =>
        jsValue.asInstanceOf[JsNumber].value.toDouble
      case (decimalType: DecimalType, _) =>
        nullOrElse[java.math.BigDecimal, JsNumber](jsValue)(jsValue => jsValue.value.bigDecimal)
      case (StringType, _) =>
        nullOrElse[UTF8String, JsString](jsValue)(jsValue => UTF8String(jsValue.value))
      case (BinaryType, _) =>
        nullOrElse[Array[Byte], JsString](jsValue)(jsValue => DatatypeConverter.parseBase64Binary(jsValue.value))
      case (BooleanType, true) =>
        nullOrElse[java.lang.Boolean, JsBoolean](jsValue)(jsValue => jsValue.value)
      case (BooleanType, false) =>
        jsValue.asInstanceOf[JsBoolean].value
      case (TimestampType, _) =>
        nullOrElse[java.sql.Timestamp, JsNumber](jsValue)(jsValue => new java.sql.Timestamp(jsValue.value.toLongExact))
      case (DateType, _) => // TODO internal represenation = Int? does it mean days or ms?
        nullOrElse[java.sql.Date, JsNumber](jsValue)(jsValue => new java.sql.Date(jsValue.value.toLongExact))
      case (ArrayType(elementType, containsNull), _) =>
        nullOrElse[Seq[Any], JsArray](jsValue)(jsValue => jsValue.elements.map(jsValueToValue(elementType, containsNull)))
      case (MapType(keyType, valueType, valueContainsNull), _) =>
        nullOrElse[Map[Any, Any], JsArray](jsValue)(jsValue =>
          jsValue.elements.map(keyValuePair => {
            val Seq(jsKey, jsValue) = keyValuePair.asJsObject.getFields(keyNameType, valueNameType)
            jsValueToValue(keyType, nullable = false)(jsKey) -> jsValueToValue(valueType, valueContainsNull)(jsValue)
          }).toMap
        )
      case (structType: StructType, _) =>
        nullOrElse[Row, JsArray](jsValue)(jsValue => jsArrayToRow(jsValue, structType))
      case _ =>
        nullOrElse[String, JsString](jsValue)(jsValue => jsValue.value)
    }

    private def jsArrayToRow(jsArray: JsArray, schema: StructType): Row = {
      Row.fromSeq(
        jsArray.elements.zip(schema.fields).map { case (jsValue, field) =>
            jsValueToValue(field.dataType, field.nullable)(jsValue)
        }
      )
    }

    def write(table: Table) = {
      val jsTitle = JsString(table.title)

      val jsFields = table.schema.fields.map(field => JsObject(
        columnNameName -> JsString(field.name),
        columnTypeName -> JsString(field.dataType.json),
        columnNullableName -> JsBoolean(field.nullable)
      ))
      val jsSchema = JsArray(jsFields.toVector)

      val jsContent = JsArray(
        table.content.map(rowToJsArray(_, table.schema)).toVector
      )

      JsObject(
        (titleName, jsTitle),
        (schemaName, jsSchema),
        (contentName, jsContent)
      )
    }

    def read(jsValue: JsValue) = jsValue match {
      case table: JsObject => {
        val jsTableFields = table.fields

        val title = jsTableFields(titleName).asInstanceOf[JsString].value

        val jsSchemaFields = jsTableFields(schemaName).asInstanceOf[JsArray].elements.map(_.asJsObject.fields)
        val schema = StructType(jsSchemaFields.map(fields => {
            StructField(
              name = fields(columnNameName).asInstanceOf[JsString].value,
              dataType = DataType.fromJson(fields(columnTypeName).asInstanceOf[JsString].value),
              nullable = fields(columnNullableName).asInstanceOf[JsBoolean].value
            )
          }))

        val rows = jsTableFields(contentName).asInstanceOf[JsArray].elements
        val content = rows.map(jsRow => jsArrayToRow(jsRow.asInstanceOf[JsArray], schema))

        Table(
          title = title,
          schema = schema,
          content = content
        )
      }
      case _ => deserializationError("Table expected")
    }

  }

  private val heatmapFormat = DefaultJsonProtocol.jsonFormat5(Heatmap)

  private val graphFormat = DefaultJsonProtocol.jsonFormat3(Graph)

  private object ScatterPlotFormat extends RootJsonFormat[ScatterPlot] {

    private val Array(titleName, pointsName, xIsNumericName, yIsNumericName) =
      extractFieldNames(classManifest[ScatterPlot])
    private val xName = "x"
    private val yName = "y"

    def write(scatter: ScatterPlot) = JsObject(
      (titleName, JsString(scatter.title)),
      (pointsName, JsArray(
        scatter.points.map { case (x, y) =>
          val jsX = if (scatter.xIsNumeric) JsNumber(x.asInstanceOf[Double]) else JsString(x.asInstanceOf[String])
          val jsY = if (scatter.yIsNumeric) JsNumber(y.asInstanceOf[Double]) else JsString(y.asInstanceOf[String])
          JsObject(
            (xName, jsX),
            (yName, jsY)
          )
        }.toVector
      )),
      (xIsNumericName, JsBoolean(scatter.xIsNumeric)),
      (yIsNumericName, JsBoolean(scatter.yIsNumeric))
    )

    def read(jsValue: JsValue) = jsValue match {
      case scatter: JsObject => {
        val jsScatterFields = scatter.fields

        val xIsNumeric = jsScatterFields(xIsNumericName).asInstanceOf[JsBoolean].value
        val yIsNumeric = jsScatterFields(yIsNumericName).asInstanceOf[JsBoolean].value

        ScatterPlot(
          title = jsScatterFields(titleName).asInstanceOf[JsString].value,
          points = jsScatterFields(pointsName).asInstanceOf[JsArray].elements.map( jsPoint => {
            val pointFields = jsPoint.asJsObject.fields
            val xField = pointFields(xName)
            val x = if (xIsNumeric) xField.asInstanceOf[JsNumber].value.toDouble else xField.asInstanceOf[JsString].value
            val yField = pointFields(yName)
            val y = if (yIsNumeric) yField.asInstanceOf[JsNumber].value.toDouble else yField.asInstanceOf[JsString].value
            (x, y)
          }),
          xIsNumeric = xIsNumeric,
          yIsNumeric = yIsNumeric
        )
      }
      case _ => deserializationError("ScatterPlot expected")
    }

  }

  // TODO I used OrderedMap for key -> value before, now I use an array of key-value pairs
  private object KeyValueSequenceFormat extends RootJsonFormat[KeyValueSequence] {

    private val Array(titleName, keyValuePairsName) = extractFieldNames(classManifest[KeyValueSequence])
    private val keyName = "key"
    private val valueName = "val"

    def write(keyValueSequence: KeyValueSequence) = JsObject(
      (titleName, JsString(keyValueSequence.title)),
      (keyValuePairsName, JsArray(
        keyValueSequence.keyValuePairs.map { case (key, value) => JsObject(
          (keyName, JsString(key)),
          (valueName, JsString(value.toString))
        )}.toVector
      ))
    )

    def read(jsValue: JsValue) = jsValue match {
      case keyValueSequence: JsObject => {
        val jsKeyValueSequenceFields = keyValueSequence.fields
        KeyValueSequence(
          title = jsKeyValueSequenceFields(titleName).asInstanceOf[JsString].value,
          keyValuePairs = jsKeyValueSequenceFields(keyValuePairsName).asInstanceOf[JsArray].elements.map(kv => {
            val kvFields = kv.asJsObject.fields
            val key = kvFields(keyName).asInstanceOf[JsString].value
            val value = kvFields(valueName).asInstanceOf[JsString].value
            (key, value)
          })
        )
      }
      case _ => deserializationError("KeyValueSequence expected")
    }

  }

  private object CompositeFormat extends RootJsonFormat[Composite] {

    private val Array(titleName, servablesName) = extractFieldNames(classManifest[Composite])

    def write(composite: Composite) = JsObject(
      (titleName, JsString(composite.title)),
      (servablesName, JsArray(
        composite.servables.map(row =>
          JsArray(row.map(_.toJson(ServableJsonFormat)).toVector)
        ).toVector
      ))
    )

    def read(jsValue: JsValue) = jsValue match {
      case composite: JsObject => {
        val jsCompositeFields = composite.fields
        Composite(
          title = jsCompositeFields(titleName).asInstanceOf[JsString].value,
          servables = jsCompositeFields(servablesName).asInstanceOf[JsArray].elements.map(row =>
            row.asInstanceOf[JsArray].elements.map(_.convertTo[Servable](ServableJsonFormat))
          )
        )
      }
      case _ => deserializationError("Composite expected")
    }

  }

  private object BlankFormat extends RootJsonFormat[Blank.type] {

    def write(blank: Blank.type) = JsObject()

    def read(jsValue: JsValue) = jsValue match {
      case jsObject: JsObject if jsObject.fields.isEmpty => Blank
      case _ => deserializationError("KeyValueSequence expected")
    }

  }

  object ServableJsonFormat extends RootJsonFormat[Servable] {

    def write(servable: Servable) = {
      servable match {
        case barChart: BarChart =>
          JsObject(
            (TYPE_KEY, JsString(BAR_CHART_TYPE)),
            (SERVABLE_KEY, barChart.toJson(barChartFormat))
          )
        case pieChart: PieChart =>
          JsObject(
            (TYPE_KEY, JsString(PIE_CHART_TYPE)),
            (SERVABLE_KEY, pieChart.toJson(pieChartFormat))
          )
        case histogram: Histogram =>
          JsObject(
            (TYPE_KEY, JsString(HISTOGRAM_TYPE)),
            (SERVABLE_KEY, histogram.toJson(histogramFormat))
          )
        case table: Table =>
          JsObject(
            (TYPE_KEY, JsString(TABLE_TYPE)),
            (SERVABLE_KEY, table.toJson(TableFormat))
          )
        case heatmap: Heatmap =>
          JsObject(
            (TYPE_KEY, JsString(HEATMAP_TYPE)),
            (SERVABLE_KEY, heatmap.toJson(heatmapFormat))
          )
        case graph: Graph =>
          JsObject(
            (TYPE_KEY, JsString(GRAPH_TYPE)),
            (SERVABLE_KEY, graph.toJson(graphFormat))
          )
        case scatterPlot: ScatterPlot =>
          JsObject(
            (TYPE_KEY, JsString(SCATTER_PLOT_TYPE)),
            (SERVABLE_KEY, scatterPlot.toJson(ScatterPlotFormat))
          )
        case keyValueSequence: KeyValueSequence =>
          JsObject(
            (TYPE_KEY, JsString(KEY_VALUE_SEQUENCE_TYPE)),
            (SERVABLE_KEY, keyValueSequence.toJson(KeyValueSequenceFormat))
          )
        case composite: Composite => JsObject(
          (TYPE_KEY, JsString(COMPOSITE_TYPE)),
          (SERVABLE_KEY, composite.toJson(CompositeFormat))
        )
        case Blank =>
          JsObject(
            (TYPE_KEY, JsString(BLANK_TYPE)),
            (SERVABLE_KEY, Blank.toJson(BlankFormat))
          )
      }
    }

    def read(value: JsValue) = value match {
      case jsObject: JsObject if jsObject.fields.keySet == Set(TYPE_KEY, SERVABLE_KEY) => {
        val fields = jsObject.fields
        val servableJsObject = fields(SERVABLE_KEY).asJsObject
        fields(TYPE_KEY) match {
          case JsString(BAR_CHART_TYPE) => servableJsObject.convertTo[BarChart](barChartFormat)
          case JsString(PIE_CHART_TYPE) => servableJsObject.convertTo[PieChart](pieChartFormat)
          case JsString(HISTOGRAM_TYPE) => servableJsObject.convertTo[Histogram](histogramFormat)
          case JsString(TABLE_TYPE) => servableJsObject.convertTo[Table](TableFormat)
          case JsString(HEATMAP_TYPE) => servableJsObject.convertTo[Heatmap](heatmapFormat)
          case JsString(GRAPH_TYPE) => servableJsObject.convertTo[Graph](graphFormat)
          case JsString(SCATTER_PLOT_TYPE) => servableJsObject.convertTo[ScatterPlot](ScatterPlotFormat)
          case JsString(KEY_VALUE_SEQUENCE_TYPE) => servableJsObject.convertTo[KeyValueSequence](KeyValueSequenceFormat)
          case JsString(COMPOSITE_TYPE) => servableJsObject.convertTo[Composite](CompositeFormat)
          case JsString(BLANK_TYPE) => Blank
          case default => deserializationError(s"Unrecognized servable type $default")
        }
      }
      case default => deserializationError("Servable expected")
    }

  }

}
