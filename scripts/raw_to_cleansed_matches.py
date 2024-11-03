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


table_name = 't20_raw.csv'
folder_path = os.path.join(root_dir,'tables')

spark = create_spark_session("raw_to_cleansed_innings")

df = spark.read.csv(os.path.join(folder_path, table_name), header=True)
df.show()

df = df.withColumn('match_id', sha2(concat_ws('-', df.info_dates, df.info_event_name, df.info_gender, df.info_match_type), 256))\
.withColumn('innings_id', sha2(concat_ws('-', df.info_dates, df.info_event_name, df.info_gender, df.info_match_type, df.team), 256))\
.withColumn('match_date', regexp_replace(regexp_replace(regexp_replace(df.info_dates, "'",""),"\\[",""), "\\]",""))

df_t20_matches_cleansed = df.withColumn('match_id', col('match_id').cast(StringType()))\
.withColumn('innings_id', col('innings_id').cast(StringType()))\
.withColumn('match_date', col('match_date').cast(DateType()))\
.withColumn('event_name', col('info_event_name').cast(StringType()))\
.withColumn('gender', col('info_gender').cast(StringType()))\
.withColumn('match_type', col('info_match_type').cast(StringType()))\
.withColumn('team', col('team').cast(StringType()))\
.withColumn('target_overs', col('target_overs').cast(IntegerType()))\
.withColumn('target_runs', col('target_runs').cast(IntegerType()))\
.withColumn('absent_hurt', col('absent_hurt').cast(IntegerType()))\
.withColumn('penalty_runs_post', col('penalty_runs_post').cast(IntegerType()))\
.withColumn('penalty_runs_pre', col('penalty_runs_pre').cast(IntegerType()))\
.withColumn('overs', col('overs').cast(StringType()))\
.withColumn('powerplays', col('powerplays').cast(StringType()))\
.select(['match_id', 'innings_id', 'match_date', 'event_name', 'gender', 'match_type', 'team', 'target_overs', 'target_runs', 'absent_hurt', 'penalty_runs_post', 'penalty_runs_pre', 'overs', 'powerplays'])

df_t20_matches_cleansed.show()

cleansed_path = os.path.join(folder_path, 't20_matches_cleansed.csv')
df_t20_matches_cleansed = df_t20_matches_cleansed.toPandas()
df_t20_matches_cleansed.to_csv(cleansed_path, index=False)