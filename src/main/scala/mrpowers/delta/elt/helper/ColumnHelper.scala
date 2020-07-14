package mrpowers.delta.elt.helper

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DataTypes, DecimalType}

object ColumnHelper {

  val decimalType: DecimalType = DataTypes.createDecimalType(32, 9)

  def withTurnover()(df: DataFrame): DataFrame = {
    df.withColumn(
      "turnover",
      col("volume") * col("price")
    )
  }

  /* def withTradeDate(dateFormat: String)(df: DataFrame): DataFrame = {
      df.withColumn("trade_date",
        when(col("trade_date").isNotNull, col("trade_date")).
       otherwise(to_date(col("ts")./(1000).cast("timestamp"), dateFormat)))

  }*/

  def withColumnsRenamed(columnRenameMap: Map[String, String])(source: DataFrame): DataFrame = {
    columnRenameMap.foldLeft(source)((x, y) => x.withColumnRenamed(y._1, y._2))
  }
}
