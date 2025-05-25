##### Step 1 - Read the CSV file using the spark dataframe reader

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True)
])
circuits_df = spark.read \
.option("header", True) \
.schema(circuits_schema) \
.csv("/mnt/formula1dl/raw/circuits.csv")

#####Step 2 - Select only the required columns

from pyspark.sql.functions import col
circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))

 ##### Step 3 - Rename the columns as required

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
.withColumnRenamed("circuitRef", "circuit_ref") \
.withColumnRenamed("lat", "latitude") \
.withColumnRenamed("lng", "longitude") \
.withColumnRenamed("alt", "altitude") 

##### Step 4 - Add ingestion date to the dataframe

from pyspark.sql.functions import current_timestamp
circuits_final_df = circuits_renamed_df.withColumn("ingestion_date", current_timestamp()) 

##### Step 5 - Write data to datalake as parquet

circuits_final_df.write.mode("overwrite").parquet("/mnt/formula1dl/processed/circuits")
display(spark.read.parquet("/mnt/formula1dl/processed/circuits"))


