from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import os
import sys
import boto3

current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.abspath(os.path.join(current_dir, os.pardir))
sys.path.insert(0, root_dir)

def create_spark_session(name):
    return SparkSession.builder.appName(name).getOrCreate()