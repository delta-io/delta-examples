package mrpowers.delta.elt


import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import mrpowers.delta.examples.SparkSessionTestWrapper
import org.apache.spark.sql.functions.{concat_ws, input_file_name, lit}
import org.scalatest.FunSpec

object EltPipelineTest extends FunSpec with SparkSessionTestWrapper with DataFrameComparer {

  val rootPath: String = new java.io.File("./src/test/resources/elt/").getCanonicalPath

  def main(args: Array[String]): Unit = {

    val streamingDF = spark.readStream.format("text").option("maxFilesPerTrigger", 1).load(rootPath+"/trade/source/")
    val additionalDetails = concat_ws(":", lit("Input File Name"), input_file_name())

    new Trade(spark, streamingDF, additionalDetails, rootPath).start()

  }
}
