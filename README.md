# Formula1_Databricks



### The data exported from the Ergast website is manually imported into the Data Lake's raw container. Using Azure Databricks notebooks, it's processed into the ingested or processed layer, where schema is applied, stored in Parquet format, partitioned where applicable, and enriched with audit information. Further transformations are performed in Databricks, and the final data is stored in the presentation layer. Databricks notebooks are then used to analyze the data and create dashboards to meet the analysis requirements.
