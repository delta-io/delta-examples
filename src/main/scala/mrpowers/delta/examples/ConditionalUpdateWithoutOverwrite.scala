package mrpowers.delta.examples

import io.delta.tables._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode

object ConditionalUpdateWithoutOverwrite extends SparkSessionWrapper {

val path: String = new java.io.File("./tmp/delta-table/").getCanonicalPath
val deltaTable = DeltaTable.forPath(spark, path)

def createInitialDataset(): Unit = {
  val data = spark.range(0, 5)
  data
    .write
    .format("delta")
    .mode(SaveMode.Overwrite)
    .save(path)

  deltaTable.toDF.show()
}

def addOneHundredToEvens(): Unit = {
  // Update every even value by adding 100 to it
  deltaTable.update(
    condition = expr("id % 2 == 0"),
    set = Map("id" -> expr("id + 100")))

  deltaTable.toDF.show()
}

def deleteEvenNumbers(): Unit = {
  // Delete every even value
  deltaTable.delete(condition = expr("id % 2 == 0"))

  deltaTable.toDF.show()
}

def upsertNewData(): Unit = {
  // Upsert (merge) new data
  val newData = spark.range(0, 20).toDF

  deltaTable.as("oldData")
    .merge(
      newData.as("newData"),
      "oldData.id = newData.id")
    .whenMatched
    .update(Map("id" -> col("newData.id")))
    .whenNotMatched
    .insert(Map("id" -> col("newData.id")))
    .execute()

  deltaTable.toDF.show()
}

}
