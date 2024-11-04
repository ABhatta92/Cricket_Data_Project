# AWS Glue job imports
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
import pyspark.sql.functions as F
import sys

# Initialize Glue Context and Job
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Parameters
input_s3_path = 's3://cricket-project-bucket/raw_cricsheet_data/'  # S3 path to JSON files
output_database = 'cricket_db'          # Glue Database
output_table = 't20_raw'                        # Glue Table

# Step 1: Read JSON files from S3 into a DynamicFrame
input_dynamic_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [input_s3_path]},
    format="json"
)

# Convert to Spark DataFrame for easier transformation
df = input_dynamic_frame.toDF()

df = df.select(
    F.col("json_payload.info.dates").alias("dates"),
    F.col("json_payload.info.event.name").alias("event_name"),
    F.col("json_payload.info.gender").alias("gender"),
    F.col("json_payload.info.match_type").alias("match_type")
    F.col("json_payload.innings").alias("innings")
)

# Step 3: Further Normalize innings data
df_final = df.withColumn("innings",  F.explode('innings')\
.select("dates", "event_name", "gender", "match_type", "innings.*")
        
# Step 4: Convert back to a DynamicFrame for Glue output
output_dynamic_frame = DynamicFrame.fromDF(final_df, glueContext, "output_dynamic_frame")

# Step 5: Write the final data to Glue Catalog Table
glueContext.write_dynamic_frame.from_catalog(
    frame=output_dynamic_frame,
    database=output_database,
    table_name=output_table,
    additional_options={"partitionKeys": ["info_dates"]}
)

# Commit the job
job.commit()
