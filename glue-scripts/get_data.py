# Imports
import requests
import zipfile
import io
import os
import boto3

bucket_name = 'cricket-bucket-obi'
output_folder = 'raw_cricsheet_data'

s3_client = boto3.client('s3')

# Collecting json data from cricsheet
url = 'https://cricsheet.org/downloads/t20s_json.zip'
output_folder = os.path.join(os.getcwd(),'data')

os.makedirs(output_folder, exist_ok=True)

response = requests.get(url)
response.raise_for_status()  

# Extracting all data into data folder
with zipfile.ZipFile(io.BytesIO(response.content)) as zip_ref:
    for file_name in zip_ref.namelist():
        with zip_ref.open(file_name) as extracted_file:
            s3_client.upload_fileobj(extracted_file, bucket_name, f'{output_folder}{file_name}')

print(f'Files extracted to {output_folder}')