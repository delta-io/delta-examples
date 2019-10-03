package mrpowers.delta.examples

import io.delta.tables._
import org.apache.spark.sql.functions._

object UpdateATable extends SparkSessionWrapper {

  def createEventsDeltaLake(): Unit = {
val path = new java.io.File("./src/main/resources/event_data/").getCanonicalPath
val df = spark
  .read
  .option("header", "true")
  .option("charset", "UTF8")
  .csv(path)

val outputPath = new java.io.File("./tmp/event_delta_lake/").getCanonicalPath
df
  .repartition(1)
  .write
  .format("delta")
  .save(outputPath)
  }

  def displayEvents(): Unit = {
    val path = new java.io.File("./tmp/event_delta_lake/").getCanonicalPath
    val df = spark.read.format("delta").load(path)
    df.show()
  }

  def updateEventTypeSpelling(): Unit = {
val path = new java.io.File("./tmp/event_delta_lake/").getCanonicalPath
val deltaTable = DeltaTable.forPath(spark, path)

deltaTable.updateExpr(
  "eventType = 'clck'",
  Map("eventType" -> "'click'")
)

deltaTable.update(
  col("eventType") === "clck",
  Map("eventType" -> lit("click"))
)
  }

  def displayNewEventsParquetFile(): Unit = {
    val path = new java.io.File("./tmp/event_delta_lake/part-00000-bcb431ea-f9d1-4399-9da5-3abfe5178d32-c000.snappy.parquet").getCanonicalPath
    val df = spark.read.parquet(path)
    df.show()
  }


}
