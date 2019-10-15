package mrpowers.delta.examples

import org.scalatest.FunSpec
import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import com.github.mrpowers.spark.daria.utils.NioUtils
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.types._

class CreateATableSpec extends FunSpec with SparkSessionTestWrapper with DataFrameComparer {

  it("creates a Delta table") {

    val path: String = new java.io.File("./tmp/delta-table/").getCanonicalPath

    val data = spark.range(0, 5)

    data
      .write
      .format("delta")
      .mode("overwrite")
      .save(path)

    val df = spark
      .read
      .format("delta")
      .load(path)

    val expectedDF = spark.createDF(
      List(0L, 1L, 2L, 3L, 4L),
      List(("id", LongType, true))
    )

    assertSmallDataFrameEquality(df, expectedDF)

    NioUtils.removeAll(path)

  }

}
