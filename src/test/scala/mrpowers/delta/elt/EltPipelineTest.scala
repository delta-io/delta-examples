package mrpowers.delta.elt


import java.io.File

import com.github.mrpowers.spark.daria.utils.NioUtils
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import mrpowers.delta.elt.helper.TableHelper
import mrpowers.delta.elt.helper.TableHelper.{createDeltaLocationStr, invalidRecordsTableName}
import mrpowers.delta.examples.SparkSessionTestWrapper
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.FunSpec

object EltPipelineTest extends FunSpec with SparkSessionTestWrapper with DataFrameComparer {

  private val rootPath: String = new java.io.File("./src/test/resources/elt/").getCanonicalPath

  def main(args: Array[String]): Unit = {

    /*val deltaLake = "/tmp/delta-lake"
    val checkpointLocation = "/tmp/delta-checkpointLocation"
    val path = s"$deltaLake/users"
    val partitionBy = "city"

    // The streaming query that writes out to Delta Lake
    val sq = users
      .writeStream
      .format("delta")
      .option("checkpointLocation", checkpointLocation)*/
    cleanUp

    createInvalidRecordsTable

    startTradeStream
      .writeStream
      .outputMode("append")
      .format("console")
      .start()


    spark.streams.awaitAnyTermination()
  }

  private def startTradeStream = {
    createTradeTable()
    val tradeDF = createTradeDF()
    new Trade(tradeDF).start()
  }

  private def cleanUp = {
   // new File(rootPath+"/invalid_records").delete()
   // new File(rootPath+"/trade/sink").delete()
  }

  def createInvalidRecordsTable {
    val location = TableHelper.createDeltaLocationStr(rootPath, invalidRecordsTableName+"/")

    spark.sql("DROP TABLE IF EXISTS "+ invalidRecordsTableName)

    spark.sql(
      s"""CREATE TABLE $invalidRecordsTableName (origin_value STRING NOT NULL)
         |  USING DELTA
         |  LOCATION $location""".stripMargin)
  }

  private def createTradeTable(): Unit ={
    TableHelper.createSinkTable(spark, "trade", new Trade(spark.emptyDataFrame).schema, List("ob_id"), rootPath)
  }

  private def createTradeDF(): DataFrame = {
    spark.readStream
      .format("text")
      .option("maxFilesPerTrigger", 1)
      .option("checkpointLocation", rootPath +"/trade/checkpoint")
      .load(rootPath + "/trade/source")
  }
}
