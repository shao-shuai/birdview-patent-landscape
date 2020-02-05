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
import os

spark = SparkSession\
            .builder\
            .config("spark.sql.session.timeZone", "UTC") \
            .appName("read_data")\
            .getOrCreate()

def get_publication_number(path):
    """Get the publication number of patent."""
    patent = spark.read.json(path)
    publication_number = patent \
        .select(['publication_number']) \
        .dropDuplicates()

    return publication_number

def get_publication_number_company(path):
    """Get publication nubmer and company name of patent."""
    patent = spark.read.json(path)
    publication_number_company = patent \
        .select(['publication_number', explode('assignee_harmonized.name')])

    return publication_number_company

def file_list(file_range):
    """Construct file list for reading."""
    base_path = os.environ['S3ADDRESS']
    data_path = [base_path + '{0:012}'.format(i) + '.json' for i in range(file_range)]

    return data_path

def get_backward_citation(path):
    """Get backward citaion of a patent."""
    patent = spark.read.json(path)
    backward_citation = patent \
        .withColumnRenamed('publication_number', 'pnum') \
        .select(['pnum', explode('citation.publication_number')]) \
        .filter(sf.col('col').contains('US'))

    return backward_citation

data_path = file_list(1)
shard_list = [get_backward_citation(i) for i in data_path]

shard_list[0].show(10)
