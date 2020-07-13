package mrpowers.delta.elt

import com.github.mrpowers.spark.daria.sql.DataFrameExt.DataFrameMethods
import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

abstract class EltPipeline(spark: SparkSession, rootPath: String) {

  val tableName: String
  val schema: StructType
  val partitionColumns: List[String]
  val uniqueConditions: String
  val columnRenameMap: Map[String,String]

  lazy val validConditionExpr = TableHelper.createValidConditionExpr(schema)
  val additionalDetailsExpr: Column

  private val invalidRecordsTableName = "invalid_records"
  private var setup = false

  def prepare(source: DataFrame): DataFrame = source
  def parseRawData(rawData: String): Column

  def withAdditionalColumns(source: DataFrame): DataFrame = source

  final def apply(source: DataFrame): DataFrame = {
    ensureInitialized()

    source
      .transform(prepare)
      .select(struct("*") as 'origin)
      .transform(parse)
      .transform(withColumnsRenamed)
      .transform(withAdditionalColumns)
      .transform(withParseDetails)
      .transform(write)
  }

  private def ensureInitialized() {
    if (!setup) { //No need to synchronize here as we do CREATE IF NOT EXIST in TableHelper
      setup = true;
      TableHelper.createInvalidRecordsTable(rootPath)
      TableHelper.createSinkTable(spark, tableName, schema, partitionColumns, rootPath)
    }
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

  final def withColumnsRenamed(source: DataFrame): DataFrame = {
    source.transform(ColumnHelper.withColumnsRenamed(columnRenameMap))
  }

  final def withParseDetails(parsed: DataFrame): DataFrame = {
    parsed.withColumn(
      "parse_details",
      struct(
        when(validConditionExpr, lit("OK")).otherwise(lit("NOT_VALID")) as "status",
        additionalDetailsExpr as "info",
        current_timestamp() as "at"
      )
    )
  }

  final def write(source: DataFrame): DataFrame =  {
    source.writeStream
      .format("delta")
      .foreachBatch(insertToDelta _)
      .outputMode("append")
      .start()

    //Decompress in interval's if oss ?
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
    val valid = col("parse_details.status") === "OK"
    val invalid = col("parse_details.status") === "NOT_VALID"

    withBatchId(batchDF, batchId)

    batchDF.persist()

    insert(batchDF, valid, tableName)
    insert(batchDF, invalid, invalidRecordsTableName)

      /* Maybe write duplicates to invalid records table
       DeltaTable.forName(invalidRecordsTableName).as("x")
      .merge(batchDF.flattenSchema("-").as("y"), uniqueConditions)
      .whenMatched().updateAll()
      .execute()*/

    batchDF.unpersist()
  }

  private def withBatchId(batchDF: DataFrame, batchId: Long) = {
    if (batchDF.containsColumn("batch_id")) {
      batchDF.withColumn("batch_id", lit(batchId))
    }
  }

  private def insert(batchDF: DataFrame, columnPredicate: Column, tableName: String) {

    //print(batchDF.flattenSchema("_").writeStream.format("console").start())

    DeltaTable.forName(tableName).as("x")
      .merge(batchDF.where(columnPredicate).flattenSchema("_").as("y"), uniqueConditions)
      .whenNotMatched().insertAll()
      .execute()
  }
}