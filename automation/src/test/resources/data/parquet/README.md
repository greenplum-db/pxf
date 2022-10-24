# Generate Parquet Timestamp List Using Spark for Testing

Since Hive doesn't support Timestamp for Parquet List type, we generate parquet timestamp array dataset using Spark to test our read functionality.

```shell
import org.apache.spark.sql.types._
# prepare the timestamp array dataset in string type
scala> val df = Seq(
    (1,List("2022-10-05 11:30:00","2022-10-06 12:30:00","2022-10-07 13:30:00")),
    (2, List("2022-10-05 11:30:00","2022-10-05 11:30:00","2022-10-07 13:30:00")),
    (3, List(null, "2022-10-05 11:30:00", "2022-10-05 11:30:00")),
    (4, List(null)),
    (5, List()),
    (6, null)
).toDF("id", "tm_arr")
  
# convert from array<String> to array<Timestamp>
scala> val df2=df.withColumn("tm_arr", expr("transform(tm_arr, x -> to_timestamp(x))"))
scala> df2.printSchema()
root
 |-- id: integer (nullable = false)
 |-- tm_arr: array (nullable = true)
 |    |-- element: timestamp (containsNull = true)
 # write data into a single parquet file
scala> df2.repartition(1).write.parquet("/Users/yimingli/workspace/spark/parquet-files/timestamp_array.parquet")
```