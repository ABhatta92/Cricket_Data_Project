from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import os
import sys
import boto3
import yaml

current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.abspath(os.path.join(current_dir, os.pardir))
sys.path.insert(0, root_dir)

def create_spark_session(name):
    return SparkSession.builder.appName(name).getOrCreate()

def create_spark_session_with_s3(name):
    spark = 



def get_aws_creds():
    with open(os.path.join(root_dir,'env.yaml')) as file:
        env_vars = yaml.safe_load(file)

    access_key = env_vars['aws']['access_key']
    secret_key = env_vars['aws']['access_secret']

    return (access_key, secret_key)