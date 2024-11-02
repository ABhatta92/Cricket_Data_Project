# Imports
import pandas as pd
import json
import os
import sqlite3


# Writing all json files into a pandas dataframe
data_folder = 'data'
data = []

for file_name in os.listdir(data_folder):
    if file_name.endswith('.json'):
        file_path = os.path.join(data_folder,file_name)
        print(file_path)
        
        with open(file_path, 'r') as f:
            json_payload = json.load(f)

        data.append({"file_path":file_path,"json_payload":json_payload})

df = pd.DataFrame(data)

df.head()

# Level 1 normalization
df_normalized = pd.json_normalize(df['json_payload'], sep="_")
df_normalized.head()

# Creating a match_id before explosion
# df_normalized['match_id'] = df_normalized['innings'].apply(lambda x : hashlib.sha256(str(x).encode()).hexdigest())
# First explosion due to 2 innings per match
exploded_df = df_normalized.explode('innings')
exploded_df.head()

# Collecting Key columns
df_meta = exploded_df[['info_dates', 'info_event_name', 'info_gender', 'info_match_type']]
# Level 2 normalization
df_final = pd.json_normalize(exploded_df['innings'], sep="_")
final_df = pd.concat([df_meta.reset_index(drop=True), df_final.reset_index(drop=True)], axis=1)
final_df.head()


# Writing to final csv file
table_name = 't20_raw.csv'
folder_path = os.path.join(os.getcwd(),'tables')

if not os.path.exists(folder_path):
    os.makedirs(folder_path)
    print(f"Folder '{folder_path}' created.")
else:
    print(f"Folder '{folder_path}' already exists.")

final_df.to_csv(os.path.join(folder_path,table_name), index=False)