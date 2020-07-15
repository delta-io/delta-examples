package mrpowers.delta.elt

import com.github.mrpowers.spark.daria.utils.NioUtils
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import mrpowers.delta.elt.helper.TableHelper
import mrpowers.delta.examples.SparkSessionTestWrapper
import org.scalatest.FunSpec

class EltSpec extends FunSpec with SparkSessionTestWrapper with DataFrameComparer{

  val sourceRootPath = "./src/test/resources/elt/"
  val sinkRootPath = "./tmp/elt_delta_lake/"

  it("create folders") {
    createFolder(sinkRootPath)
    createFolder(sinkRootPath+"invalid_record")
    createFolder(sinkRootPath+"trade")
    createFolder(sinkRootPath+"checkpoint")
  }

  it("create tables") {
    createTables()
  }

  it("start trade processing and verify dataframe") {
    startTradeProcessing()
  }

  private def createFolder(path: String) {
    val file = new java.io.File(path)
    if (file.exists) {
      file.delete()
    }
    file.mkdir()
  }

  private def createTables() {
    createInvalidRecordTable
    TableHelper.createTable(spark, "trade", new Trade().schema, new Trade().partitions, sinkRootPath)
  }


  private def createInvalidRecordTable = {
    val invalidRecordsTable = TableHelper.invalidRecordsTableName
    val invalidRecordsLocation = "'" + sinkRootPath + "invalid_record" + "'"

    spark.sql(
      s"""CREATE TABLE $invalidRecordsTable (origin_value STRING NOT NULL)
         |  USING DELTA
         |  LOCATION $invalidRecordsLocation""".stripMargin)
  }


  private def startTradeProcessing() {
    val tradeDF = spark.readStream
      .format("text")
      .option("maxFilesPerTrigger", 1)
      .option("checkpointLocation", sinkRootPath + "checkpoint")
      .load(sourceRootPath + "/trade/source")

    new Trade().apply(tradeDF)
      .writeStream
      .outputMode("append")
      .format("console")
      .start()

    spark.streams.awaitAnyTermination()
  }

}
