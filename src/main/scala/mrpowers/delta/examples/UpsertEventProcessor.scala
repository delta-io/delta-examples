package mrpowers.delta.examples

object UpsertEventProcessor extends SparkSessionWrapper {

  def createUpsertEventsDeltaLake(): Unit = {
val path = new java.io.File("./src/main/resources/upsert_event_data/original_data.csv").getCanonicalPath
val df = spark
  .read
  .option("header", "true")
  .option("charset", "UTF8")
  .csv(path)

val outputPath = new java.io.File("./tmp/upsert_event_delta_lake/").getCanonicalPath
df
  .repartition(1)
  .write
  .format("delta")
  .save(outputPath)
  }

  def displayEvents(): Unit = {
val path = new java.io.File("./tmp/upsert_event_delta_lake/").getCanonicalPath
val df = spark.read.format("delta").load(path)
df.show(false)
  }

  def doTheUpsert(): Unit = {
val updatesPath = new java.io.File("./src/main/resources/upsert_event_data/mom_friendly_data.csv").getCanonicalPath
val updatesDF = spark
  .read
  .option("header", "true")
  .option("charset", "UTF8")
  .csv(updatesPath)

val path = new java.io.File("./tmp/upsert_event_delta_lake/").getCanonicalPath

import io.delta.tables._

DeltaTable.forPath(spark, path)
  .as("events")
  .merge(
    updatesDF.as("updates"),
    "events.eventId = updates.eventId"
  )
  .whenMatched
  .updateExpr(
    Map("data" -> "updates.data")
  )
  .whenNotMatched
  .insertExpr(
    Map(
      "date" -> "updates.date",
      "eventId" -> "updates.eventId",
      "data" -> "updates.data")
  )
  .execute()
  }

def displayEventParquetFile(filename: String): Unit = {
  val path = new java.io.File(s"./tmp/upsert_event_delta_lake/$filename.snappy.parquet").getCanonicalPath
  val df = spark.read.parquet(path)
  df.show(false)
}

}
