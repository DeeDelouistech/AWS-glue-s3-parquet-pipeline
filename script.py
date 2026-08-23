import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# 1. READ FROM GLUE DATA CATALOG TABLE
datasource0 = glueContext.create_dynamic_frame.from_catalog(
    database = "ml_raw_db", 
    table_name = "raw_data"  # Replace with the exact table name created by your crawler
)

# 2. CONVERT TO SPARK DF & CLEAN DATA
df = datasource0.toDF()
df_cleaned = df.dropna()

# 3. WRITE TO S3 AS PARQUET
output_path = "s3://mla-c01-course-bucket-626185423973/processed-data/"
df_cleaned.write.mode("overwrite").parquet(output_path)

job.commit()
