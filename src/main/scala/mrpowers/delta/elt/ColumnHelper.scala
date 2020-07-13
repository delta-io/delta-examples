package mrpowers.delta.elt

import org.apache.spark.sql.functions.{col, when, to_date}
import org.apache.spark.sql.{Column, DataFrame, SparkSession, functions}

object ColumnHelper {

  def withTurnover()(df: DataFrame): DataFrame = {
    df.withColumn(
      "turnover",
      col("volume") * col("price")
    )
  }

  def withTradeDate()(df: DataFrame): DataFrame = {
     df.withColumn("trade_date",
       when(col("trade_date").isNotNull, col("trade_date")).
      otherwise(to_date(col("ts")./(1000).cast("timestamp"), "yyyyMMdd")))
  }

  def withColumnsRenamed(columnRenameMap: Map[String, String])(source: DataFrame): DataFrame = {
    columnRenameMap.foldLeft(source)((x, y) => x.withColumnRenamed(y._1, y._2))
  }
}
