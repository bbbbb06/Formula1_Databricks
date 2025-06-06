##### Step 1 - Read the CSV file using the spark dataframe reader API

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])
lap_times_df = spark.read \
.schema(lap_times_schema) \
.csv("/mnt/formula1dl/raw/lap_times")

##### Step 2 - Rename columns and add new columns
# MAGIC 1. Rename driverId and raceId
# MAGIC 1. Add ingestion_date with current timestamp

from pyspark.sql.functions import current_timestamp
final_df = lap_times_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("ingestion_date", current_timestamp())

##### Step 3 - Write to output to processed container in parquet format

final_df.write.mode("overwrite").parquet("/mnt/formula1dl/processed/lap_times")



