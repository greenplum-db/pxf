from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql.types import IntegerType
import shutil, os

spark = (
    SparkSession.builder.master("local[2]")
    .appName("generate_parquet_timestamp_list")
    .getOrCreate()
)

# prepare the timestamp array dataset in string type
data = [
    (1, ["2022-10-05 11:30:00", "2022-10-06 12:30:00", "2022-10-07 13:30:00"]),
    (2, ["2022-10-05 11:30:00", "2022-10-05 11:30:00", "2022-10-07 13:30:00"]),
    (3, [None, "2022-10-05 11:30:00", "2022-10-05 11:30:00"]),
    (4, [None]),
    (5, []),
    (6, None),
]
columnNames = ["id", "tm_arr"]
df = spark.createDataFrame(data, columnNames)

# convert from Long to Integer
df1 = df.withColumn("id", df["id"].cast(IntegerType()))
# convert from array<String> to array<Timestamp>
df2 = df1.withColumn("tm_arr", expr("transform(tm_arr, x -> to_timestamp(x))"))
df2.show(df2.count(), False)
# +---+---------------------------------------------------------------+
# |id |tm_arr                                                         |
# +---+---------------------------------------------------------------+
# |1  |[2022-10-05 11:30:00, 2022-10-06 12:30:00, 2022-10-07 13:30:00]|
# |2  |[2022-10-05 11:30:00, 2022-10-05 11:30:00, 2022-10-07 13:30:00]|
# |3  |[null, 2022-10-05 11:30:00, 2022-10-05 11:30:00]               |
# |4  |[null]                                                         |
# |5  |[]                                                             |
# |6  |null                                                           |
# +---+---------------------------------------------------------------+

df2.printSchema()
# root
# |-- id: long (nullable = true)
# |-- tm_arr: array (nullable = true)
# |    |-- element: timestamp (containsNull = true)

if os.path.exists("tmp.parquet"):
    shutil.rmtree("tmp.parquet")
# write data into a single parquet file
df2.repartition(1).write.parquet("tmp.parquet")
