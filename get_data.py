# Imports
import requests
import zipfile
import io
import os

# Collecting json data from cricsheet
url = 'https://cricsheet.org/downloads/t20s_json.zip'
output_folder = os.path.join(os.getcwd(),'data')

os.makedirs(output_folder, exist_ok=True)

response = requests.get(url)
response.raise_for_status()  

# Extracting all data into data folder
with zipfile.ZipFile(io.BytesIO(response.content)) as zip_ref:
    zip_ref.extractall(output_folder)

print(f'Files extracted to {output_folder}')