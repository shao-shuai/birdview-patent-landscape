import boto3
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import desc
from pyspark.sql.functions import asc
from pyspark.sql.functions import sum as Fsum
from pyspark.sql.functions import udf
from pyspark.sql.functions import explode, array
import pyspark.sql.functions as sf

import datetime

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

import json

from neo4j import GraphDatabase
import neo4j

import logging
from functools import reduce
from pyspark.sql import DataFrame

spark = SparkSession\
            .builder\
            .config("spark.sql.session.timeZone", "UTC") \
            .appName("read_data")\
            .getOrCreate()

# get publication number of patent
def get_publication_number(path):
    patent = spark.read.json(path)
    publication_number = patent \
        .select(['publication_number']) \
        .dropDuplicates()

    return publication_number

# construct file list to read
def file_list(file_range):
    base_path = 's3a://testbucket-shaoshuai/us_publications_20200121_' # should also keep this one confidential
    data_path = [base_path + '{0:012}'.format(i) + '.json' for i in range(file_range)]

    return data_path


# read file
data_path = file_list(1)
shard_list = [get_publication_number(i) for i in data_path]

#shard_list[0].show(10)
