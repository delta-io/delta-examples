package mrpowers.delta.examples

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.scalatest.FunSpec
import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import com.github.mrpowers.spark.daria.utils.NioUtils
import org.apache.spark.sql.types._

class TimeTravelSpec extends FunSpec with SparkSessionTestWrapper with DataFrameComparer {

  it("can time travel to an earlier version of the data") {

    val path: String = new java.io.File("./tmp/delta-table-tt/").getCanonicalPath

    val data = spark.range(0, 5)
    data.write.format("delta").mode("overwrite").save(path)

    val moreData = spark.range(20, 25)
    moreData.write.format("delta").mode("append").save(path)

    val expectedDF = spark.createDF(
      List(0L, 1L, 2L, 3L, 4L),
      List(("id", LongType, true))
    )

    val v0 = spark.read.format("delta").option("versionAsOf", 0).load(path)

    assertSmallDataFrameEquality(v0, expectedDF)

    NioUtils.removeAll(path)

  }

}
