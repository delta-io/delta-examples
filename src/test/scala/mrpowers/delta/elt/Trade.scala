package mrpowers.delta.elt

import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{DateType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

class Trade(spark: SparkSession, streamingDF: DataFrame, additionalDetails: Column, rootPath: String) extends EltPipeline(spark: SparkSession,rootPath: String) {

  override val additionalDetailsExpr = additionalDetails

  override val partitionColumns: List[String] = List("trade_date", "ob_id")

  override val tableName: String = "trade"

  override val uniqueConditions = "x.delta_batch_id = y.delta_batch_id AND x.trade_id = y.trade_id";

  override val schema = StructType(
    Seq(
      StructField("delta_batch_id", LongType, false),
      StructField("trade_id", StringType, false),
      StructField("trade_date", DateType, false),
      StructField("ts", StringType, false),
      StructField("ob_id", StringType, false),
      StructField("ask_member", StringType, false),
      StructField("sell_member", StringType, false),
      StructField("volume", StringType, false),
      StructField("price", StringType, false),
      StructField("broker", StringType, true),
    ))

  override def parseRawData(rawData: String): Column = {
    from_json(col(rawData), schema)
  }

  def start() {
    apply(streamingDF)
  }
}
