# delta-examples

This repo provides working code notebooks so you can execute code like the [Delta Lake documentation](https://docs.delta.io/latest/index.html) with Scala Spark and PySpark on your local machine.  It also provides [delta-rs](https://github.com/delta-io/delta-rs) examples.

Running Delta commands on your local machine and studying the files that are created is a great way to learn about how Delta Lake works.

## PySpark setup

You can install PySpark and Delta Lake by creating the `mr-delta` conda environment.

Create the environment with this command: `conda env create -f envs/pyspark-330-delta-210`.

Activate the environment with this command: `conda activate mr-delta`.

Then you can run `jupyter lab` and execute all the PySpark notebooks.

## delta-rs setup (Python bindings)

You can run the delta-rs notebooks that use the Python bindings by creating the `mr-delta-rs` conda environment.

Create the environment with this command: `conda env create -f envs/mr-delta-rs.yml`.

Activate the environment with this command: `conda activate mr-delta-rs`.

## Scala setup

TODO: Figure out how to use Courier and add.

## [Create a table](https://docs.delta.io/latest/quick-start.html#create-a-table) and [read data](https://docs.delta.io/latest/quick-start.html#read-data)

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

## [Update Table Data](https://docs.delta.io/latest/quick-start.html#update-table-data)

Here's some code to update a Delta table:

```scala
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
```

Let's run the code:

```aidl
UpdateTableData.updateDeltaTable()

CreateATable.readTable()

+---+
| id|
+---+
|  5|
|  6|
|  7|
|  8|
|  9|
+---+
```

## [Conditional updates without overwrite](https://docs.delta.io/latest/quick-start.html#conditional-update-without-overwrite)

Create the initial dataset:

```scala
val path: String = new java.io.File("./tmp/delta-table/").getCanonicalPath
val deltaTable = DeltaTable.forPath(spark, path)

def createInitialDataset(): Unit = {
  val data = spark.range(0, 5)
  data
    .write
    .format("delta")
    .mode(SaveMode.Overwrite)
    .save(path)

  deltaTable.toDF.show()
}
```

```aidl
createInitialDataset()

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

Add one hundred to the even numbers:

```scala
def addOneHundredToEvens(): Unit = {
  // Update every even value by adding 100 to it
  deltaTable.update(
    condition = expr("id % 2 == 0"),
    set = Map("id" -> expr("id + 100")))

  deltaTable.toDF.show()
}
```

```aidl
addOneHundredToEvens()

+---+
| id|
+---+
|100|
|  1|
|102|
|  3|
|104|
+---+
```

Delete the even numbers:

```scala
def deleteEvenNumbers(): Unit = {
  // Delete every even value
  deltaTable.delete(condition = expr("id % 2 == 0"))

  deltaTable.toDF.show()
}
```

```aidl
deleteEvenNumbers()

+---+
| id|
+---+
|  1|
|  3|
+---+
```

Upsert the new data:

```scala
def upsertNewData(): Unit = {
  // Upsert (merge) new data
  val newData = spark.range(0, 20).toDF

  deltaTable.as("oldData")
    .merge(
      newData.as("newData"),
      "oldData.id = newData.id")
    .whenMatched
    .update(Map("id" -> col("newData.id")))
    .whenNotMatched
    .insert(Map("id" -> col("newData.id")))
    .execute()

  deltaTable.toDF.show()
}
```

```aidl
upsertNewData()

+---+
| id|
+---+
| 11|
| 18|
| 10|
|  5|
|  8|
| 13|
| 14|
| 19|
|  4|
| 17|
| 15|
| 12|
|  7|
| 16|
|  0|
|  1|
|  6|
|  3|
|  9|
|  2|
+---+
```

## [Time Travel](https://docs.delta.io/latest/quick-start.html#read-older-versions-of-data-using-time-travel)

Let's create a Delta data lake and then update it with some additional data.

```scala
val path: String = new java.io.File("./tmp/delta-table-tt/").getCanonicalPath

val data = spark.range(0, 5)
data.write.format("delta").mode("overwrite").save(path)

val moreData = spark.range(20, 25)
moreData.write.format("delta").mode("overwrite").save(path)
```

The Delta lake will currently contain data from the original load and the incremental update:

```
spark.read.format("delta").load(path).show()

+---+
| id|
+---+
| 20|
| 21|
| 22|
| 23|
| 24|
|  0|
|  1|
|  2|
|  3|
|  4|
+---+
```

We can time travel back to "version 0" of the Delta lake (after the initial data load) and see the initial state of the Delta lake.

```
spark.read.format("delta").option("versionAsOf", 0).load(path).show()

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

