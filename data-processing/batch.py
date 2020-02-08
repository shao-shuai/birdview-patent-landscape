import sys
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

from neo4j import GraphDatabase
import neo4j

import logging
from functools import reduce
from pyspark.sql import DataFrame
import os

logging.basicConfig(filename = 'write.log', level = logging.INFO,
                    format = '%(asctime)s:%(levelname)s:%(message)s')

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

def file_list(index_start, index_end):
    """Construct file list for reading."""
    base_path = os.environ['S3ADDRESS']
    data_path = [base_path + '{0:012}'.format(i) + '.json' for i in range(index_start, index_end)]

    return data_path

def single_file(file_index):
    """Return the address of a single file."""
    base_path = os.environ['S3ADDRESS']
    data_path = base_path + '{0:012}'.format(file_index) + '.json'

    return data_path

def get_backward_citation(path):
    """Get backward citaion of a patent."""
    patent = spark.read.json(path)
    backward_citation = patent \
        .withColumnRenamed('publication_number', 'pnum') \
        .select(['pnum', explode('citation.publication_number')]) \
        .filter(sf.col('col').contains('US'))

    return backward_citation

def write_neo4j_constraint():
    query='''CREATE CONSTRAINT ON (p:PATENT) ASSERT p.name IS UNIQUE'''
    with gradb.session() as session:
        with session.begin_transaction() as tx:
            tx.run(query)

def write_neo4j_relationship(data):

    query='''WITH $names AS nested
    UNWIND nested AS x
    MERGE (w:PATENT {name: x[0]})
    MERGE (n:PATENT {name: x[1]})
    MERGE (w)-[r:CITE]-(n)
    '''
    with gradb.session() as session:
        with session.begin_transaction() as tx:
            tx.run(query, names=data)

def write_database_relationship(path):
    data = get_backward_citation(i)
    data = data.collect()
    relationship = [[i['pnum'], i['col']] for i in data]
    write_neo4j_relationship(relationship)
    relationship = []

if __name__ == '__main__':

    gradb=GraphDatabase.driver(os.environ['NEO4JURI'], auth=(os.environ['NEO4JUSER'], os.environ['NEO4JPASS']), encrypted=False)

    data_path = file_list(int(sys.argv[1]), int(sys.argv[2]))

    for i in data_path:
        try:
            write_database_relationship(i)
            logging.info("Writing file " + i + " succeeded")
        except:
            logging.info("Writing file " + i + " failed")


