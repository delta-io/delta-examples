package mrpowers.delta.examples

object UpdateTableData extends SparkSessionWrapper {

  val path: String = new java.io.File("./tmp/delta-table/").getCanonicalPath

  def updateDeltaTable(): Unit = {
    val data = spark.range(5, 10)
    data
      .write
      .format("delta")
      .mode("overwrite")
      .save(path)
  }

}
