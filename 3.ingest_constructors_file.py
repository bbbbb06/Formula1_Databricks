##### Step 1 - Read the JSON file using the spark dataframe reader

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"
constructor_df = spark.read \
.schema(constructors_schema) \
.json("/mnt/formula1dl/raw/constructors.json")

##### Step 2 - Drop unwanted columns from the dataframe

from pyspark.sql.functions import col
constructor_dropped_df = constructor_df.drop(col('url'))

##### Step 3 - Rename columns and add ingestion date

from pyspark.sql.functions import current_timestamp
constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
                                             .withColumnRenamed("constructorRef", "constructor_ref") \
                                             .withColumn("ingestion_date", current_timestamp())

##### Step 4 Write output to parquet file

constructor_final_df.write.mode("overwrite").parquet("/mnt/formula1dl/processed/constructors")


