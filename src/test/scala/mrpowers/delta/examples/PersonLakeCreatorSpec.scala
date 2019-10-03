package mrpowers.delta.examples

import org.scalatest.FunSpec
import org.apache.spark.sql.SaveMode

class PersonLakeCreatorSpec extends FunSpec with SparkSessionTestWrapper {

  it("creates a delta lake") {
    val lakePath = new java.io.File("./tmp/person_delta_lake/").getCanonicalPath
    PersonLakeCreator.createDeltaLake(SaveMode.Overwrite)
    spark
      .read
      .format("delta")
      .load(lakePath)
      .show()
    PersonLakeCreator.createDeltaLake(SaveMode.Append)
    spark
      .read
      .format("delta")
      .load(lakePath)
      .show()
  }

}
