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

def create_spark_session_with_s3(name, access_key, secret_key):
    spark = SparkSession.builder \
        .appName(name) \
        .config("spark.hadoop.fs.s3a.access.key", access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.maximum", "100") \
        .config("spark.hadoop.fs.s3a.attempts.maximum", "10") \
        .config("spark.hadoop.fs.s3a.paging.enabled", "true") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0,com.amazonaws:aws-java-sdk-bundle:1.11.375") \
        .getOrCreate()
    
    return spark



def get_aws_creds():
    with open(os.path.join(root_dir,'env.yaml')) as file:
        env_vars = yaml.safe_load(file)

    access_key = env_vars['aws']['access_key']
    secret_key = env_vars['aws']['access_secret']

    return (access_key, secret_key)