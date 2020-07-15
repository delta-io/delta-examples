package mrpowers.delta.elt.helper

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Column, SparkSession}

object TableHelper {

  final val invalidRecordsTableName = "invalid_records"

  def createTable(spark: SparkSession, tableName: String, schema: StructType, partitionColumns: List[String], rootPath: String) {
    val fields = getSchemaFieldsForDDL(schema)
    val partitions = partitionColumns.mkString("(", ",", ")")
    val location = createDeltaLocationStr(rootPath, tableName)

    spark.sql("DROP TABLE IF EXISTS "+ tableName)
    spark.sql(
      s"""CREATE TABLE $tableName $fields
        |  USING DELTA
        |    PARTITIONED BY $partitions
        |  LOCATION  $location""".stripMargin)
  }

  def createDeltaLocationStr(rootPath: String, folder: String): String ={
    var location = rootPath.replace("C:", "").replace("\\", "/")
    location = "'" + location +"/"+ folder +"'"
    location
  }

  private def getSchemaFieldsForDDL(schema: StructType) = {
    schema.fields
      .map(createField)
      .toList.mkString("(", ",\n ", ")")
  }

  private def createField(field: StructField) = {
    field.name + " " + field.dataType.sql + " " + (if (!field.nullable) "NOT NULL" else "")
  }

  def createValidConditionExpr(schema: StructType): Column = {
    var column = col("dummy")
    var first = true

    for (field <- schema.fields)
      if (!field.nullable)

        if (first) {
          column = col(field.name).isNotNull
          first = false
        } else
          column && col(field.name).isNotNull

    column
  }
}
