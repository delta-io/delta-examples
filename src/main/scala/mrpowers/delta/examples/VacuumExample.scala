package mrpowers.delta.examples

import org.apache.spark.sql.SaveMode

object VacuumExample extends SparkSessionWrapper {

  def createInitialPeopleStore(): Unit = {
    val path = new java.io.File("./src/main/resources/person_data/people1.csv").getCanonicalPath
    val df = spark
      .read
      .option("header", "true")
      .option("charset", "UTF8")
      .csv(path)

    val outputPath = new java.io.File("./tmp/vacuum_example/").getCanonicalPath
    df
      .repartition(1)
      .write
      .format("delta")
      .save(outputPath)
  }

  def displayPeopleStore(): Unit = {
    val path = new java.io.File("./tmp/vacuum_example/").getCanonicalPath
    val df = spark.read.format("delta").load(path)
    df.show()
  }

  def displayPeopleStorev0(): Unit = {
    val path = new java.io.File("./tmp/vacuum_example/").getCanonicalPath
    val df = spark.read.format("delta").option("versionAsOf", 0).load(path)
    df.show()
  }

  def overwritePeopleStore(): Unit = {
val path = new java.io.File("./src/main/resources/person_data/people2.csv").getCanonicalPath
val df = spark
  .read
  .option("header", "true")
  .option("charset", "UTF8")
  .csv(path)

val outputPath = new java.io.File("./tmp/vacuum_example/").getCanonicalPath
df
  .repartition(1)
  .write
  .format("delta")
  .mode(SaveMode.Overwrite)
  .save(outputPath)
  }

  def vacuumPeopleStore(): Unit = {
val path = new java.io.File("./tmp/vacuum_example/").getCanonicalPath
import io.delta.tables._
val deltaTable = DeltaTable.forPath(spark, path)
deltaTable.vacuum(0.000001)
  }

}
