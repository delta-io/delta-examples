package mrpowers.delta.examples

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.scalatest.FunSpec
import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import com.github.mrpowers.spark.daria.utils.NioUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

class OverwriteSinglePartitionSpec extends FunSpec with SparkSessionTestWrapper with DataFrameComparer {

  it("overwrites a single partition") {

    val path = new java.io.File("./src/test/resources/ss_europe/").getCanonicalPath

val df = spark
  .read
  .option("header", "true")
  .option("charset", "UTF8")
  .csv(path)
  .withColumn("continent", lit(null).cast(StringType))

val deltaPath = new java.io.File("./tmp/country_partitioned_lake/").getCanonicalPath

//df
//  .repartition(col("country"))
//  .write
//  .partitionBy("country")
//  .format("delta")
//  .mode("overwrite")
//  .save(deltaPath)
//
//def withContinent()(df: DataFrame): DataFrame = {
//  df.withColumn(
//    "continent",
//    when(col("country") === "Russia", "Europe")
//      .when(col("country") === "China", "Asia")
//      .when(col("country") === "Argentina", "South America")
//  )
//}
//
//spark.read.format("delta").load(deltaPath)
//  .where(col("country") === "China")
//  .transform(withContinent())
//  .write
//  .format("delta")
//  .option("replaceWhere", "country = 'China'")
//  .mode("overwrite")
//  .save(deltaPath)

spark
  .read
  .format("delta")
  .load(deltaPath)
  .show(false)

//    NioUtils.removeAll(path)

  }

}
