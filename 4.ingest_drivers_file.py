##### Step 1 - Read the JSON file using the spark dataframe reader API

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
name_schema = StructType(fields=[StructField("forename", StringType(), True),
                                 StructField("surname", StringType(), True)
  
])
drivers_schema = StructType(fields=[StructField("driverId", IntegerType(), False),
                                    StructField("driverRef", StringType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("code", StringType(), True),
                                    StructField("name", name_schema),
                                    StructField("dob", DateType(), True),
                                    StructField("nationality", StringType(), True),
                                    StructField("url", StringType(), True)  
])
drivers_df = spark.read \
.schema(drivers_schema) \
.json("/mnt/formula1dl/raw/drivers.json")

##### Step 2 - Rename columns and add new columns
# MAGIC 1. driverId renamed to driver_id  
# MAGIC 1. driverRef renamed to driver_ref  
# MAGIC 1. ingestion date added
# MAGIC 1. name added with concatenation of forename and surname

from pyspark.sql.functions import col, concat, current_timestamp, lit
drivers_with_columns_df = drivers_df.withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("driverRef", "driver_ref") \
                                    .withColumn("ingestion_date", current_timestamp()) \
                                    .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname")))

##### Step 3 - Drop the unwanted columns
# MAGIC 1. url

drivers_final_df = drivers_with_columns_df.drop(col("url"))

##### Step 4 - Write to output to processed container in parquet format

drivers_final_df.write.mode("overwrite").parquet("/mnt/formula1dl/processed/drivers")


