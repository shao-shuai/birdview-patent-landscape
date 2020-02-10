import boto3
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
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

def writeBatchData(b):
    session = gradb.session()
    session.run('UNWIND {batch} AS row CREATE (n:Node {v: row})', {'batch': b})
    session.close()

uri = "bolt://ec2-44-232-97-138.us-west-2.compute.amazonaws.com:7687"
gradb=GraphDatabase.driver(uri, auth=(os.environ['NEO4JUSER'], os.environ['NEO4JPASS']), encrypted=False)

sc = SparkContext()

dt = sc.parallelize(range(1, 1000))

dt.foreach(writeBatchData)

