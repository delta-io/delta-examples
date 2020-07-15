package mrpowers.delta.elt

import com.github.mrpowers.spark.daria.utils.NioUtils
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import io.delta.tables.DeltaTable
import mrpowers.delta.elt.helper.TableHelper
import mrpowers.delta.examples.SparkSessionTestWrapper
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.{OutputMode}
import org.scalatest.FunSpec


class EltTest extends FunSpec with SparkSessionTestWrapper with DataFrameComparer {

  val sourceRootPath = "./src/test/resources/elt/"
  val sinkRootPath = "./tmp/delta_lake_elt/"

  val tradeHandler = new Trade

  it("clean up") {
    val file = new java.io.File(sinkRootPath)
    if (file.exists) {
      NioUtils.removeUnder(file)
      NioUtils.removeAll(file)
    }
  }

  it("create folders") {
    createFolder(sinkRootPath)
    createFolder(sinkRootPath + "invalid_record")
    createFolder(sinkRootPath + "trade")
  }

  it("create tables") {
    createTables()
  }

  it("start trade processing") {
    startTradeProcessing()
  }

  it("Verify written delta files") {
    DeltaTable.forName(spark, "trade").toDF.show(10)
    DeltaTable.forName(spark, TableHelper.invalidRecordsTableName).toDF.show(10)
  //Todo verify frames
  }


  private def createFolder(path: String) {
    new java.io.File(path).mkdir()
  }

  private def createTables() {
    createInvalidRecordTable
    TableHelper.createTable(spark, "trade", tradeHandler.schema, tradeHandler.partitions, sinkRootPath)
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
    spark.readStream
      .format("text")
      .option("maxFilesPerTrigger", 1)
      .option("checkpointLocation", sinkRootPath + "checkpoint")
      .load(sourceRootPath + "/trade/source")
      .writeStream
      .outputMode(OutputMode.Append())
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        tradeHandler.apply(batchDF, batchId)
      }.start()
      .awaitTermination(60000)
  }
}
