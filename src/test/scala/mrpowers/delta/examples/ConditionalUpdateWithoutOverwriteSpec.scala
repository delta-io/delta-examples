package mrpowers.delta.examples

import org.scalatest.FunSpec
import io.delta.tables._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import com.github.mrpowers.spark.daria.utils.NioUtils
import com.github.mrpowers.spark.fast.tests.{ColumnComparer, DataFrameComparer}
import java.sql.Date
import java.text._

class ConditionalUpdateWithoutOverwriteSpec
    extends FunSpec
    with SparkSessionTestWrapper
    with DataFrameComparer {

  import spark.implicits._

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

    NioUtils.removeAll(path)

  }

  it("performs an upsert for a more standard dataset") {
    implicit def date(str: String): Date = Date.valueOf(str)

    val df = Seq(
      (1, "elon musk", "south africa", "pretoria", true, "1971-06-28", null),
      (2, "jeff bezos", "us", "albuquerque", true, "1964-01-12", null),
      (3, "bill gates", "us", "seattle", false, "1955-10-28", "1973-09-01")
    ).toDF("personId", "personName", "country", "region", "isCurrent", "effectiveDate", "endDate")

    df.show()

    val path = os.pwd/"tmp"/"tech_celebs"

    // create the Delta table
    df
      .write
      .format("delta")
      .mode("overwrite")
      .save(path.toString())

    //    val customersTable: DeltaTable =   // table with schema (customerId, address, current, effectiveDate, endDate)
    //      DeltaTable.forName("customers_delta_example")

    val updatesDF = Seq(
      (1, "elon musk", "canada", "montreal", "1989-06-01"),
      (4, "dhh", "us", "chicago", "2005-11-01")
    ).toDF("personId", "personName", "country", "region", "effectiveDate")

    updatesDF.show()

    val techCelebsTable = DeltaTable.forPath(spark, path.toString())

    // Rows to INSERT new geo of existing people
    val newGeoToInsert = updatesDF
      .as("updates")
      .join(techCelebsTable.toDF.as("tech_celebs"), "personId")
      .where("tech_celebs.isCurrent = true AND (updates.country <> tech_celebs.country OR updates.region <> tech_celebs.region)")
      .selectExpr("NULL as mergeKey", "updates.*")

    newGeoToInsert.show()

    // New people or
    val otherDF = updatesDF.selectExpr("personId as mergeKey", "*")

        otherDF.show()

    // Stage the update by unioning two sets of rows
    // 1. Rows that will be inserted in the `whenNotMatched` clause
    // 2. Rows that will either UPDATE the current addresses of existing customers or INSERT the new addresses of new customers
    val stagedUpdates = newGeoToInsert.union(otherDF)

    stagedUpdates.show()

        // Apply SCD Type 2 operation using merge
    techCelebsTable
      .as("tech_celebs")
      .merge(stagedUpdates.as("staged_updates"), "tech_celebs.personId = mergeKey")
      .whenMatched("tech_celebs.isCurrent = true AND (staged_updates.country <> tech_celebs.country OR staged_updates.region <> tech_celebs.region)")
      .updateExpr(Map( // Set current to false and endDate to source's effective date.
        "isCurrent" -> "false",
        "endDate" -> "staged_updates.effectiveDate"))
      .whenNotMatched()
      .insertExpr(Map(
        "personId" -> "staged_updates.personId",
        "personName" -> "staged_updates.personName",
        "country" -> "staged_updates.country",
        "region" -> "staged_updates.region",
        "isCurrent" -> "true",
        "effectiveDate" -> "staged_updates.effectiveDate", // Set current to true along with the new address and its effective date.
        "endDate" -> "null"))
      .execute()

    val resDF =  spark
      .read
      .format("delta")
      .load(path.toString())

    resDF.show()
  }

//  it("reads a bunch of files") {
//    val path = os.pwd/"tmp"/"tech_celebs"/"part-00000-2cc6a8d9-86ee-4292-a850-9f5e01918c0d-c000.snappy.parquet"
//    spark.read.parquet(path.toString()).show()

//    val path = os.pwd/"tmp"/"tech_celebs"/"part-00000-daa6c389-2894-4a6b-a012-618e830574c6-c000.snappy.parquet"
//    spark.read.parquet(path.toString()).show()

//    val path = os.pwd/"tmp"/"tech_celebs"/"part-00042-d38c2d50-7910-4658-b297-84c51cf4b196-c000.snappy.parquet"
//    spark.read.parquet(path.toString()).show()
//
//    val path = os.pwd/"tmp"/"tech_celebs"/"part-00043-acda103d-c5d7-4062-a6f8-0a112f4425f7-c000.snappy.parquet"
//    spark.read.parquet(path.toString()).show()

//    val path = os.pwd/"tmp"/"tech_celebs"/"part-00051-c5d71959-a214-44f9-97fc-a0b928a19393-c000.snappy.parquet"
//    spark.read.parquet(path.toString()).show()

//    val path = os.pwd/"tmp"/"tech_celebs"/"part-00102-41b139c3-a6cd-4a1a-814d-f509b84459a9-c000.snappy.parquet"
//    spark.read.parquet(path.toString()).show()

//    val path = os.pwd/"tmp"/"tech_celebs"/"part-00174-693e1b33-cbd1-4d93-b4fc-fbf4c7f00878-c000.snappy.parquet"
//    spark.read.parquet(path.toString()).show()
//  }

}
