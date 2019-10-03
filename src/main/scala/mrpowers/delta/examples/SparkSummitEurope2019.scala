package mrpowers.delta.examples

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions

object SparkSummitEurope2019 extends SparkSessionWrapper {

  def createTaxiDeltaLake(): Unit = {
    val path = new java.io.File("./src/main/resources/taxi_data/").getCanonicalPath
    val df = spark
      .read
      .option("header", "true")
      .option("charset", "UTF8")
      .csv(path)
    val outputPath = new java.io.File("./tmp/taxi_delta_lake/").getCanonicalPath
    df
      .repartition(5)
      .write
      .format("delta")
      .mode(SaveMode.Overwrite)
      .save(outputPath)
  }

  def compactTaxiDeltaLake(): Unit = {
    val path = new java.io.File("./tmp/taxi_delta_lake/").getCanonicalPath
    val df = spark
      .read
      .format("delta")
      .load(path)
    df
      .repartition(1)
      .write
      .format("delta")
      .mode("overwrite")
      .save(path)
  }

  def vacuumTaxiDeltaLake(): Unit = {
    val path = new java.io.File("./tmp/taxi_delta_lake/").getCanonicalPath
    import io.delta.tables._
    val deltaTable = DeltaTable.forPath(spark, path)
    deltaTable.vacuum(0.000001)
  }

//  def createPartitionedDeltaLake(): Unit = {
//    val path = new java.io.File("./src/main/resources/more_taxi_data/").getCanonicalPath
//    val df = spark
//      .read
//      .option("header", "true")
//      .option("charset", "UTF8")
//      .csv(path)
//      .withColumn("fare_type", when())
//  }

}
