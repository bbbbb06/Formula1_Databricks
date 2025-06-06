##### Step 1 - Read the JSON file using the spark dataframe reader API

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("stop", StringType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("duration", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])
pit_stops_df = spark.read \
.schema(pit_stops_schema) \
.option("multiLine", True) \
.json("/mnt/formula1dl/raw/pit_stops.json")
pit_stops_df = spark.read \
.option("multiLine", True) \
.json("/mnt/formula1dl/raw/pit_stops.json")
display(pit_stops_df)

##### Step 2 - Rename columns and add new columns
# MAGIC 1. Rename driverId and raceId
# MAGIC 1. Add ingestion_date with current timestamp

from pyspark.sql.functions import current_timestamp
final_df = pit_stops_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("ingestion_date", current_timestamp())

##### Step 3 - Write to output to processed container in parquet format

final_df.write.mode("overwrite").parquet("/mnt/formula1dl/processed/pit_stops")



