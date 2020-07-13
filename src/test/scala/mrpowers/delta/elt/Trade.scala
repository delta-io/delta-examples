package mrpowers.delta.elt

import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{DataTypes, DateType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

class Trade(spark: SparkSession, streamingDF: DataFrame, additionalDetails: Column, rootPath: String) extends EltPipeline(spark: SparkSession,rootPath: String) {

  override val additionalDetailsExpr = additionalDetails

  override val partitionColumns: List[String] = List("trade_date", "ob_id")

  override val tableName: String = "trade"

  override val uniqueConditions = "x.batch_id = y.batch_id AND x.trade_id = y.trade_id";

  override val columnRenameMap = Map("TRADE_ID" -> "trade_id", "TIMESTAMP" -> "ts", "BROKER" -> "broker")

  override val schema = StructType(
    Seq(
      StructField("trade_id", StringType, false),
      StructField("trade_date", DateType, false),
      StructField("ts", LongType, false),
      StructField("ob_id", StringType, false),
      StructField("ask_member", StringType, false),
      StructField("sell_member", StringType, false),
      StructField("volume", TableHelper.decimalType, false),
      StructField("price", TableHelper.decimalType, false),
      StructField("turnover", TableHelper.decimalType, false),
      StructField("broker", StringType, true),
      StructField("batch_id", LongType, true) //Nullable to avoid validation error but it is set last in the write method
    ))

  override def parseRawData(rawData: String): Column = {
    from_json(col(rawData), schema)
  }

  override def withAdditionalColumns(source: DataFrame): DataFrame = {
    source.transform(ColumnHelper.withTurnover())
    source.transform(ColumnHelper.withTradeDate())
  }


  def start() {
    apply(streamingDF)
  }
}
