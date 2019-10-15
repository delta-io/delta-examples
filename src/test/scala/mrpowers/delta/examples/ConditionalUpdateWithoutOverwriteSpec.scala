package mrpowers.delta.examples

import org.scalatest.FunSpec
import io.delta.tables._
import mrpowers.delta.NioUtil
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import com.github.mrpowers.spark.fast.tests.{ColumnComparer, DataFrameComparer}

class ConditionalUpdateWithoutOverwriteSpec
    extends FunSpec
    with SparkSessionTestWrapper
    with DataFrameComparer {

  it("does the random assortment of conditional overwrite examples") {

    val path: String = new java.io.File("./tmp/delta-table-co/").getCanonicalPath

    // create initial dataset
    val data = spark.range(0, 5)
    data
      .write
      .format("delta")
      .mode("overwrite")
      .save(path)

    val deltaTable = DeltaTable.forPath(spark, path)

    // Update every even value by adding 100 to it
    deltaTable.update(
      condition = expr("id % 2 == 0"),
      set = Map("id" -> expr("id + 100"))
    )

    val expectedDF = spark.createDF(
      List(100L, 1L, 102L, 3L, 104L),
      List(("id", LongType, true))
    )

    val df = spark.read.format("delta").load(path)

    assertSmallDataFrameEquality(df, expectedDF)

    // Delete every even value
    deltaTable.delete(condition = expr("id % 2 == 0"))

    val postDeleteDF = spark.createDF(
      List(1L, 3L),
      List(("id", LongType, true))
    )

    val df2 = spark.read.format("delta").load(path)

    assertSmallDataFrameEquality(df2, postDeleteDF)

    // Upsert (merge) new data
    val newData = spark.range(500, 503).toDF

    deltaTable.as("oldData")
      .merge(
        newData.as("newData"),
        "oldData.id = newData.id")
      .whenMatched
      .update(Map("id" -> col("newData.id")))
      .whenNotMatched
      .insert(Map("id" -> col("newData.id")))
      .execute()

    val df3 = spark.read.format("delta").load(path)

    val postUpsertDF = spark.createDF(
      List(1L, 3L, 500L, 501L, 502L),
      List(("id", LongType, true))
    )

    assertSmallDataFrameEquality(df3, postUpsertDF, orderedComparison = false)

    NioUtil.removeAll(path)

  }

}
