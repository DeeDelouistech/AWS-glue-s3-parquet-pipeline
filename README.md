# AWS-glue-s3-parquet-pipeline
Building an End-to-End S3-to-Parquet ETL Pipeline on AWS!
# End-to-End AWS S3-to-Parquet ETL Pipeline

## Project Overview
This project demonstrates a production-style serverless data engineering pipeline built on AWS. It ingests raw data, catalogs it via AWS Glue, processes it using distributed PySpark code, and optimizes it into column-oriented Apache Parquet format ready for Machine Learning models and analytics.

## Architecture Diagram
[ S3 Raw Data ] ➔ [ AWS Glue Crawler ] ➔ [ Glue Data Catalog ] ➔ [ PySpark ETL Job ] ➔ [ S3 Parquet Output ]

## Tech Stack
* **Cloud Provider:** Amazon Web Services (AWS)
* **Storage:** Amazon S3
* **Catalog & Schema Discovery:** AWS Glue Crawlers, Glue Data Catalog
* **Processing Framework:** Apache Spark / PySpark (AWS Glue ETL)
* **Data Format:** CSV ➔ Apache Parquet

## Pipeline Steps
1. **Ingestion:** Raw CSV datasets are stored in an Amazon S3 bucket (`mla-c01-course-bucket-626185423973`).
2. **Cataloging:** An AWS Glue Crawler (`s3-raw-data-crawler`) automatically scans the S3 files and registers the schema in the `ml_raw_db` database.
3. **Transformation (PySpark):** A Glue ETL job reads from the Data Catalog, handles data cleaning (dropping nulls), and transforms the dataset.
4. **Optimization:** The processed data is written back to S3 in **Parquet format** to maximize query speeds and minimize storage costs for downstream AI consumption.

## Code Snippet (PySpark ETL)
```python
datasource0 = glueContext.create_dynamic_frame.from_catalog(
    database = "ml_raw_db", 
    table_name = "raw_data"
)

df = datasource0.toDF()
df_cleaned = df.dropna()

output_path = "s3://mla-c01-course-bucket-626185423973/processed-data/"
df_cleaned.write.mode("overwrite").parquet(output_path)
