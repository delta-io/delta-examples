package mrpowers.delta.examples

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.scalatest.FunSpec
import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import com.github.mrpowers.spark.daria.utils.NioUtils
import org.apache.spark.sql.types._

class TimeTravelSpec extends FunSpec with SparkSessionTestWrapper with DataFrameComparer {

  it("can time travel to an earlier version of the data") {

    val path: String = new java.io.File("./tmp/delta-table-tt/").getCanonicalPath

    val data = spark.range(0, 5)
    data.write.format("delta").mode("overwrite").save(path)

    val moreData = spark.range(20, 25)
    moreData.write.format("delta").mode("append").save(path)

    val expectedDF = spark.createDF(
      List(0L, 1L, 2L, 3L, 4L),
      List(("id", LongType, true))
    )

    val v0 = spark.read.format("delta").option("versionAsOf", 0).load(path)

    assertSmallDataFrameEquality(v0, expectedDF)

    NioUtils.removeAll(path)

  }

  it("creates a Delta Lake with one file") {
    val path = os.pwd/"src"/"test"/"resources"/"countries"/"countries1.csv"
    val df = spark.read.option("header", true).csv(path.toString())
    val outputPath = os.pwd/"tmp"/"andres_demo"
    df.write.format("delta").save(outputPath.toString())
  }

  it("incrementally updates the Delta lake") {
    val path = os.pwd/"src"/"test"/"resources"/"countries"/"countries2.csv"
    val df = spark.read.option("header", true).csv(path.toString())
    val outputPath = os.pwd/"tmp"/"andres_demo"
    df.write.format("delta").mode("append").save(outputPath.toString())
  }

  it("can display all the data in the lake") {
    val deltaPath = os.pwd/"tmp"/"andres_demo"
    val df = spark.read.format("delta").load(deltaPath.toString())
    df.show()
  }

  it("can time travel to the first version of the Delta Lake") {
    val deltaPath = os.pwd/"tmp"/"andres_demo"
    val df = spark.read
      .format("delta")
      .option("versionAsOf", 0)
      .load(deltaPath.toString())
    df.show()
  }

  it("deletes the Delta lake") {
    val deltaPath = os.pwd/"tmp"/"andres_demo"
    os.remove.all(deltaPath)
  }

}
