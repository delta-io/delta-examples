package mrpowers.delta.examples

import io.delta.tables._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._

object CatLakeCreator extends SparkSessionWrapper {

  def createCatDeltaLake(): Unit = {
val path = new java.io.File("./src/main/resources/cat_data/cats1.csv").getCanonicalPath
val df = spark
  .read
  .option("header", "true")
  .option("charset", "UTF8")
  .csv(path)

val outputPath = new java.io.File("./tmp/cat_delta_lake/").getCanonicalPath

df
  .repartition(1)
  .write
  .format("delta")
  .save(outputPath)

val path2 = new java.io.File("./src/main/resources/cat_data/cats2.csv").getCanonicalPath
val df2 = spark
  .read
  .option("header", "true")
  .option("charset", "UTF8")
  .csv(path2)

df2
  .repartition(1)
  .write
  .mode(SaveMode.Append)
  .format("delta")
  .save(outputPath)
  }

  def updateCatNameSpelling(): Unit = {
val path = new java.io.File("./tmp/cat_delta_lake/").getCanonicalPath
val deltaTable = DeltaTable.forPath(spark, path)

deltaTable.updateExpr(
  "cat_name = 'flffy'",
  Map("cat_name" -> "'fluffy'")
)

deltaTable.update(
  col("cat_name") === "flffy",
  Map("cat_name" -> lit("fluffy"))
)
}

}
