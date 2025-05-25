##### Step 1 - Read the CSV file using the spark dataframe reader API

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                  StructField("year", IntegerType(), True),
                                  StructField("round", IntegerType(), True),
                                  StructField("circuitId", IntegerType(), True),
                                  StructField("name", StringType(), True),
                                  StructField("date", DateType(), True),
                                  StructField("time", StringType(), True),
                                  StructField("url", StringType(), True) 
])
races_df = spark.read \
.option("header", True) \
.schema(races_schema) \
.csv("/mnt/formula1dl/raw/races.csv")

##### Step 2 - Add ingestion date and race_timestamp to the dataframe

from pyspark.sql.functions import current_timestamp, to_timestamp, concat, col, lit
races_with_timestamp_df = races_df.withColumn("ingestion_date", current_timestamp()) \
                                  .withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))

##### Step 3 - Select only the columns required & rename as required

races_selected_df = races_with_timestamp_df.select(col('raceId').alias('race_id'), col('year').alias('race_year'), 
                                                   col('round'), col('circuitId').alias('circuit_id'),col('name'), col('ingestion_date'), col('race_timestamp'))

##### Write the output to processed container in parquet format

races_selected_df.write.mode('overwrite').partitionBy('race_year').parquet('/mnt/formula1dl/processed/races')


