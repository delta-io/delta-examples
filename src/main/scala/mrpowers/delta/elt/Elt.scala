package mrpowers.delta.elt

import com.github.mrpowers.spark.daria.sql.DataFrameExt.DataFrameMethods
import io.delta.tables.DeltaTable
import mrpowers.delta.elt.helper.TableHelper
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

trait Elt {

  private val parsingValidCol = col("parse_details.status") === "OK"
  private val parsingInvalidCol = col("parse_details.status") === "NOT_VALID"

  val tableName: String
  val partitions: List[String]
  val uniqueConditions: String

  val validConditionExpr: Column
  val additionalDetailsExpr: Column

  def prepare(source: DataFrame): DataFrame = source

  def parseRawData(rawData: String): Column

  def withAdditionalColumns(source: DataFrame): DataFrame = source

  final def apply(source: DataFrame, batchId: Long) {
    source.persist()

    source
      .transform(prepare)
      .select(struct("*") as 'origin)
      .transform(parse)
      .transform(withAdditionalColumns)
      .transform(withParseDetails)
      .transform(insertIntoDelta(batchId))

    compressFiles()

    source.unpersist()
  }

  final def parse(source: DataFrame): DataFrame = {
    source
      .withColumn(
        "extractedFields",
        parseRawData("origin.value")
      )
      .select(
        "extractedFields.*",
        "origin"
      )
  }

  final def withParseDetails(source: DataFrame): DataFrame = {
    source.withColumn(
      "parse_details",
      struct(
        when(validConditionExpr, lit("OK")).otherwise(lit("NOT_VALID")) as "status",
        additionalDetailsExpr as "info",
        current_timestamp() as "at"
      )
    )
  }

  private def insertIntoDelta(batchId: Long)(source: DataFrame): DataFrame = {
    source.withColumn("batch_id", typedLit(batchId))
    //Use the batch_id in tables to guarantee exactly-once

    insert(source, parsingValidCol, uniqueConditions, tableName)
    insert(source, parsingInvalidCol, "1 = 1", TableHelper.invalidRecordsTableName)
    source
  }


  private def insert(source: DataFrame, columnPredicate: Column, uniqueConditions: String, tableName: String) {
    val flattened = source.where(columnPredicate).flattenSchema("_").as("y")

    DeltaTable.forName(tableName).as("x")
      .merge(flattened, uniqueConditions)
      .whenNotMatched().insertAll()
      .execute()
  }

  private def compressFiles() {
    //Decompress in interval's here ?
    // val partitions: List[String]
    /* DeltaLogHelpers.partitionedLake1GbChunks

     spark.read
       .format("delta")
       .load(path)
       .where(partition)
       .repartition(numFilesPerPartition)
       .write
       .option("dataChange", "false")
       .format("delta")
       .mode("overwrite")
       .option("replaceWhere", partition)
       .save(path)*/
  }
}