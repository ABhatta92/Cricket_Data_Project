from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import os
import sys
import boto3

current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.abspath(os.path.join(current_dir, os.pardir))
sys.path.insert(0, root_dir)

from scripts.common_functions import *

table_name = 't20_matches_cleansed.csv'
folder_path = os.path.join(root_dir,'tables')

spark = create_spark_session("raw_to_cleansed_overs")

df = spark.read.csv(os.path.join(folder_path, table_name), header=True)
df.show()

df_overs = df.select('innings_id', 'overs')

overs_schema = 'ARRAY<STRUCT<over: BIGINT,deliveries: ARRAY<STRUCT<batter: STRING,bowler: STRING,non_striker: STRING,runs: STRUCT<batter: BIGINT,extras: BIGINT,total: BIGINT>,extras: STRUCT<wides: BIGINT,byes: BIGINT,legbyes: BIGINT,noballs: BIGINT,penalty: BIGINT>>>>>'

df_overs = df_overs.withColumn('overs_parsed', from_json(col('overs'), overs_schema))
df_overs = df_overs.withColumn('overs_exploded', explode(col('overs_parsed')))
df_overs = df_overs.select('innings_id', 'overs_exploded.over', 'overs_exploded.deliveries')
df_overs = df_overs.withColumn('deliveries_exploded', explode('deliveries'))
df_overs = df_overs.select(col('innings_id'), col('over'), col('deliveries_exploded.batter'), col('deliveries_exploded.bowler'), col('deliveries_exploded.non_striker'), col('deliveries_exploded.runs.batter').alias('runs_off_bat'), col('deliveries_exploded.runs.extras').alias('runs_extra'), col('deliveries_exploded.runs.total').alias('total_runs'), col('deliveries_exploded.extras.wides').alias('wides'), col('deliveries_exploded.extras.byes').alias('byes'), col('deliveries_exploded.extras.legbyes').alias('legbyes'), col('deliveries_exploded.extras.noballs').alias('noballs'), col('deliveries_exploded.extras.penalty').alias('penalty'))
df_overs.show(truncate=True)

df_meta = df.select('match_id','innings_id','match_date','event_name','gender','match_type','team')

df_final = df_meta.join(df_overs, how='inner', on='innings_id')

df_final.show()

cleansed_path = os.path.join(folder_path, 't20_overs_cleansed.csv')
df_final = df_final.toPandas()
df_final.to_csv(cleansed_path, index=False)