package mrpowers.delta.elt

import com.github.mrpowers.spark.daria.sql.DataFrameExt.DataFrameMethods
import io.delta.tables.DeltaTable
import mrpowers.delta.elt.helper.TableHelper
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession, types}

trait Elt {

  private val parsingValidCol = col("parse_details.status") === "OK"
  private val parsingInvalidCol = col("parse_details.status") === "NOT_VALID"

  val tableName: String
  val uniqueConditions: String

  val validConditionExpr: Column
  val additionalDetailsExpr: Column

  def prepare(source: DataFrame): DataFrame = source
  def parseRawData(rawData: String): Column
  def withAdditionalColumns(source: DataFrame): DataFrame = source

  final def apply(source: DataFrame): DataFrame = {
    source
      .transform(prepare)
      .select(struct("*") as 'origin)
      .transform(parse)
      .transform(withAdditionalColumns)
      .transform(withParseDetails)
      .transform(writeToDelta)
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

  final def writeToDelta(source: DataFrame): DataFrame =  {
    source.writeStream
      .format("delta")
      .foreachBatch(insertToDelta _)
      .outputMode("append")
      .start()

    //Decompress in interval's here ?
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
    source
  }

  private def insertToDelta(batchDF: DataFrame, batchId: Long) {
    // batchDF.withColumn("batch_id", typedLit(batchId))

    batchDF.persist()

    insert(batchDF, parsingValidCol, uniqueConditions, tableName)
    insert(batchDF, parsingInvalidCol, "1 = 1", TableHelper.invalidRecordsTableName)

    batchDF.unpersist()
  }

  private def insert(batchDF: DataFrame, columnPredicate: Column, uniqueConditions: String, tableName: String) {
    DeltaTable.forName(tableName).as("x")
      .merge(batchDF.where(columnPredicate).flattenSchema("_").as("y"), uniqueConditions)
      .whenNotMatched().insertAll()
      .execute()
  }
}