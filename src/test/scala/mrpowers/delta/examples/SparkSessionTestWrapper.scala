package mrpowers.delta.examples

import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("spark session")
      .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
      .getOrCreate()
  }

}
