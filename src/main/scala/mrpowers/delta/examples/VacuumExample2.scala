package mrpowers.delta.examples

import org.apache.spark.sql.SaveMode

object VacuumExample2 extends SparkSessionWrapper {

  def createDogStore(): Unit = {
val path = new java.io.File("./src/main/resources/dog_data/dogs1.csv").getCanonicalPath
val df = spark
  .read
  .option("header", "true")
  .option("charset", "UTF8")
  .csv(path)

val outputPath = new java.io.File("./tmp/vacuum_example2/").getCanonicalPath
df
  .repartition(1)
  .write
  .format("delta")
  .save(outputPath)

val path2 = new java.io.File("./src/main/resources/dog_data/dogs2.csv").getCanonicalPath
val df2 = spark
  .read
  .option("header", "true")
  .option("charset", "UTF8")
  .csv(path2)

df2
  .repartition(1)
  .write
  .format("delta")
  .mode(SaveMode.Append)
  .save(outputPath)
  }

  def displayDogStore(): Unit = {
    val path = new java.io.File("./tmp/vacuum_example2/").getCanonicalPath
    val df = spark.read.format("delta").load(path)
    df.show()
  }

  def vacuumDogStore(): Unit = {
    val path = new java.io.File("./tmp/vacuum_example2/").getCanonicalPath
    import io.delta.tables._
    val deltaTable = DeltaTable.forPath(spark, path)
    deltaTable.vacuum(0.000001)
  }

}
