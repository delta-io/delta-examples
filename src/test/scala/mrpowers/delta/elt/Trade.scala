package mrpowers.delta.elt

import mrpowers.delta.elt.helper.{ColumnHelper, TableHelper}
import org.apache.spark.sql.functions.{col, concat_ws, from_json, input_file_name, lit}
import org.apache.spark.sql.types.{DataTypes, DateType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

class Trade extends Elt() {

  override val tableName: String = "trade"

  override val partitions = List("ob_id")

  override val validConditionExpr: Column = TableHelper.createValidConditionExpr(schema)

  override val additionalDetailsExpr: Column = concat_ws(":", lit("Input File Name"), input_file_name())

  override val uniqueConditions = "x.trade_id = y.trade_id";

  private lazy val parsingSchema: StructType = StructType(
    Seq(
      StructField("trade_id", StringType, false),
      StructField("ts", LongType, false),
      StructField("ob_id", StringType, false),
      StructField("ask_member", StringType, false),
      StructField("bid_member", StringType, false),
      StructField("volume", ColumnHelper.decimalType, false),
      StructField("price", ColumnHelper.decimalType, false)
    ))

  private lazy val withColumnsSchema: StructType = StructType(
    Seq(
      StructField("turnover", ColumnHelper.decimalType, false),
    ))

  lazy val schema = new StructType(parsingSchema.fields ++ withColumnsSchema.fields)

  override def parseRawData(rawData: String): Column = {
    from_json(col(rawData), parsingSchema)
  }

  override def withAdditionalColumns(source: DataFrame): DataFrame = {
    source.transform(ColumnHelper.withTurnover())
  }
}
