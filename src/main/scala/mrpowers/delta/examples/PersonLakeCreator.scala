package mrpowers.delta.examples

import org.apache.spark.sql.SaveMode

object PersonLakeCreator extends SparkSessionWrapper {

  def createDeltaLake(saveMode: SaveMode): Unit = {
    val path = new java.io.File("./src/main/resources/person_data/").getCanonicalPath
    val df = spark
      .read
      .option("header", "true")
      .option("charset", "UTF8")
      .csv(path)

    val outputPath = new java.io.File("./tmp/person_delta_lake/").getCanonicalPath
    df
      .repartition(1)
      .write
      .format("delta")
      .mode(saveMode)
      .save(outputPath)
  }

}
