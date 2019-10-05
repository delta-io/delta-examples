package mrpowers.delta.examples

import org.apache.spark.sql.SaveMode

object CreateATable extends SparkSessionWrapper {

  val path: String = new java.io.File("./tmp/delta-table/").getCanonicalPath

  def createTable(): Unit = {
    val data = spark.range(0, 5)
    data
      .write
      .format("delta")
      .mode(SaveMode.Overwrite)
      .save(path)
  }

  def readTable(): Unit = {
    val df = spark
      .read
      .format("delta")
      .load(path)
    df.show()
  }

}
