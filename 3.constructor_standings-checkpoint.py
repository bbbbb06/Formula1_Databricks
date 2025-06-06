##### Produce driver standings

%run ".configuration"
race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

from pyspark.sql.functions import sum, when, count, col

constructor_standings_df = race_results_df \
.groupBy("race_year", "team") \
.agg(sum("points").alias("total_points"),
     count(when(col("position") == 1, True)).alias("wins"))
display(constructor_standings_df.filter("race_year = 2020"))

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank, asc

constructor_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = constructor_standings_df.withColumn("rank", rank().over(constructor_rank_spec))
display(final_df.filter("race_year = 2020"))

final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/constructor_standings")


