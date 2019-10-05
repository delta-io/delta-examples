# delta-examples

This repo provides working code for the examples covered in the [Delta Lake documentation](https://docs.delta.io/latest/index.html).

## [Create a table](https://docs.delta.io/latest/quick-start.html#create-a-table)

Let's use the `spark.range()` function to create a DataFrame of numbers and write out the DataFrame as a Delta lake.

```scala
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
```

Let's execute the code:

```
CreateATable.createTable()

CreateATable.readTable()

+---+
| id|
+---+
|  0|
|  1|
|  2|
|  3|
|  4|
+---+
```

Read the [Introduction to Delta Lake](https://mungingdata.com/delta-lake/introduction-time-travel/) blog post to learn more Delta lake basics.

