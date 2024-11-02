from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T

import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.abspath(os.path.join(current_dir, os.pardir))
sys.path.insert(0, root_dir)

from scripts.common_functions import *


spark = create_spark_session("raw_to_cleansed_matches")

data_path = os.path.join(root_dir,'tables','t20_raw.csv')
df = spark.read.csv(data_path, header=True)
df.show(10, False)

df = df.withColumn('match_id', F.sha2(F.concat_ws('-', df.info_dates, df.info_event_name, df.info_gender, df.info_match_type), 256))\
.withColumn('match_date', F.regexp_replace(F.regexp_replace(F.regexp_replace(df.info_dates, "'",""),"\\[",""), "\\]",""))

df.select(['match_id', 'match_date', 'info_event_name', 'info_gender', 'info_match_type', 'team']).show(truncate=False)

df_t20_matches_cleansed = df.withColumn('match_id', F.col('match_id').cast(T.StringType()))\
.withColumn('match_date', F.col('match_date').cast(T.DateType()))\
.withColumn('event_name', F.col('info_event_name').cast(T.StringType()))\
.withColumn('gender', F.col('info_gender').cast(T.StringType()))\
.withColumn('match_type', F.col('info_match_type').cast(T.StringType()))\
.withColumn('team', F.col('team').cast(T.StringType()))\
.withColumn('target_overs', F.col('target_overs').cast(T.IntegerType()))\
.withColumn('target_runs', F.col('target_runs').cast(T.IntegerType()))\
.withColumn('absent_hurt', F.col('absent_hurt').cast(T.IntegerType()))\
.withColumn('penalty_runs_post', F.col('penalty_runs_post').cast(T.IntegerType()))\
.withColumn('penalty_runs_pre', F.col('penalty_runs_pre').cast(T.IntegerType()))\
.select(['match_id', 'match_date', 'event_name', 'gender', 'match_type', 'team', 'target_overs', 'target_runs', 'absent_hurt', 'penalty_runs_post', 'penalty_runs_pre'])

cleansed_path = os.path.join(root_dir, 'tables', 't20_matches_cleansed.csv')
df_t20_matches_cleansed = df_t20_matches_cleansed.toPandas()
df_t20_matches_cleansed.to_csv(cleansed_path, index=False)