package mrpowers.delta.examples

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

object IncrementalDogUpdater extends SparkSessionWrapper {

  def update(): Unit = {
    val csvPath = new java.io.File("./tmp/dog_data_csv/").getCanonicalPath

    val schema = StructType(
      List(
        StructField("first_name", StringType, true),
        StructField("breed", StringType, true)
      )
    )

    val df = spark.readStream
      .schema(schema)
      .option("header", "true")
      .option("charset", "UTF8")
      .csv(csvPath)

    val checkpointPath = new java.io.File("./tmp/dog_data_checkpoint/").getCanonicalPath
    val parquetPath = new java.io.File("./tmp/dog_data_parquet/").getCanonicalPath

    df
      .writeStream
      .trigger(Trigger.Once)
      .format("parquet")
      .option("checkpointLocation", checkpointPath)
      .start(parquetPath)

    spark.read.parquet(parquetPath).show()
  }

  def showDogDataParquet(): Unit = {
    val parquetPath = new java.io.File("./tmp/dog_data_parquet/").getCanonicalPath
    spark.read.parquet(parquetPath).show()
  }

  // AGGREGATES

  // Manual

  def manuallyCountByName(): Unit = {
    val csvPath = new java.io.File("./tmp/dog_data_csv/dogs1.csv").getCanonicalPath
    val df = spark.read
      .option("header", "true")
      .option("charset", "UTF8")
      .csv(csvPath)

    val outputPath = new java.io.File("./tmp/dog_data_name_counts").getCanonicalPath
    df
      .groupBy("first_name")
      .count()
      .repartition(1)
      .write
      .parquet(outputPath)
  }

  def manuallyUpdateCountByName(): Unit = {
    val csvPath = new java.io.File("./tmp/dog_data_csv/dogs2.csv").getCanonicalPath
    val df = spark.read
      .option("header", "true")
      .option("charset", "UTF8")
      .csv(csvPath)

    val existingCountsPath = new java.io.File("./tmp/dog_data_name_counts").getCanonicalPath
    val tmpPath = new java.io.File("./tmp/dog_data_name_counts_tmp").getCanonicalPath
    val existingCountsDF = spark
      .read
      .parquet(existingCountsPath)

    df
      .union(existingCountsDF)
      .cache()
      .groupBy("first_name")
      .count()
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(tmpPath)

    spark
      .read
      .parquet(tmpPath)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(existingCountsPath)
  }

  def showDogDataNameCounts(): Unit = {
    val parquetPath = new java.io.File("./tmp/dog_data_name_counts/").getCanonicalPath
    spark.read.parquet(parquetPath).show()
  }

  def buildManualCountByName(): Unit = {
    manuallyCountByName()
    showDogDataNameCounts()
    manuallyUpdateCountByName()
    showDogDataNameCounts()
  }

  // Slightly less manual

  def dirExists(hdfsDirectory: String): Boolean = {
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val fs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    fs.exists(new org.apache.hadoop.fs.Path(hdfsDirectory))
  }

  def refactoredUpdateCountByName(csvPath: String): Unit = {
    val df = spark.read
      .option("header", "true")
      .option("charset", "UTF8")
      .csv(csvPath)

    val existingCountsPath = new java.io.File("./tmp/dog_data_name_counts").getCanonicalPath
    val tmpPath = new java.io.File("./tmp/dog_data_name_counts_tmp").getCanonicalPath

    val unionedDF = if(dirExists(existingCountsPath)) {
      spark
        .read
        .parquet(existingCountsPath)
        .union(df)
    } else {
      df
    }

    unionedDF
      .groupBy("first_name")
      .count()
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(tmpPath)

    spark
      .read
      .parquet(tmpPath)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(existingCountsPath)
  }

  def refactoredBuildManualCountByName(): Unit = {
    val csvPath1 = new java.io.File("./tmp/dog_data_csv/dogs1.csv").getCanonicalPath
    refactoredUpdateCountByName(csvPath1)
    showDogDataNameCounts()
    val csvPath2 = new java.io.File("./tmp/dog_data_csv/dogs2.csv").getCanonicalPath
    refactoredUpdateCountByName(csvPath2)
    showDogDataNameCounts()
  }

  // Structured Streaming + Trigger.Once

  def smartUpdateCountByName(csvPath: String): Unit = {
    val schema = StructType(
      List(
        StructField("first_name", StringType, true),
        StructField("breed", StringType, true)
      )
    )

    val df = spark.readStream
      .schema(schema)
      .option("header", "true")
      .option("charset", "UTF8")
      .csv(csvPath)

    val existingCountsPath = new java.io.File("./tmp/dog_data_name_counts").getCanonicalPath
    val tmpPath = new java.io.File("./tmp/dog_data_name_counts_tmp").getCanonicalPath
    val checkpointPath = new java.io.File("./tmp/dog_data_name_counts_checkpoint/").getCanonicalPath

    val unionedDF = if(dirExists(existingCountsPath)) {
      spark
        .read
        .parquet(existingCountsPath)
        .union(df)
    } else {
      df
    }

    unionedDF
      .groupBy("first_name")
      .count()
      .repartition(1)
      .writeStream
      .trigger(Trigger.Once)
      .format("parquet")
      .option("checkpointLocation", checkpointPath)
      .start(tmpPath)

    spark
      .read
      .parquet(tmpPath)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(existingCountsPath)

  }

}
