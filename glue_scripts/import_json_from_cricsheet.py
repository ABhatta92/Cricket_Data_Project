import sys
import zipfile
import io
import boto3
import urllib.request
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Retrieve job parameters if needed
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize Glue job context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# S3 configuration
bucket_name = 'cricket-bucket-obi'
output_folder = 'raw_cricsheet_data/'

s3_client = boto3.client('s3')

# URL for cricsheet data
url = 'https://cricsheet.org/downloads/t20s_json.zip'

# Download and extract data directly to S3
try:
    with urllib.request.urlopen(url) as response:
        with zipfile.ZipFile(io.BytesIO(response.read())) as zip_ref:
            for file_name in zip_ref.namelist():
                with zip_ref.open(file_name) as extracted_file:
                    s3_client.upload_fileobj(extracted_file, bucket_name, f'{output_folder}{file_name}')
    print(f'Files successfully uploaded to S3 bucket: {bucket_name}/{output_folder}')
except Exception as e:
    print(f"Failed to download or upload data: {e}")

job.commit()
