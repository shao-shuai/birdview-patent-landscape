import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import desc
from pyspark.sql.functions import asc
from pyspark.sql.functions import sum as Fsum
from pyspark.sql.functions import udf
from pyspark.sql.functions import explode, array
import pyspark.sql.functions as sf

import datetime

from neo4j import GraphDatabase
import neo4j

import logging
from functools import reduce
from pyspark.sql import DataFrame
import os

logging.basicConfig(filename = 'write_title.log', level = logging.INFO,
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
    join_udf = udf(lambda x: ",".join(x))

    patent = spark.read.json(path)
    publication_number_company = patent \
        .select(['publication_number', 'assignee_harmonized.name']) \
        .withColumn('name', join_udf(col("name")))
    return publication_number_company

def get_publication_number_title(path):
    """Get publication number and title of patent."""

    patent = spark.read.json(path)
    publication_number_title = patent \
        .select(['publication_number', explode('title_localized.text')])

    return publication_number_title

def file_list(index_start, index_end):
    """Construct a list of file path on S3."""
    base_path = os.environ['S3ADDRESS']
    data_path = [base_path + '{0:012}'.format(i) + '.json' for i in range(index_start, index_end)]

    return data_path

def single_file(file_index):
    """Return the path of a single file."""
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
    '''Register index for label PATENT.name.'''
    query='''CREATE CONSTRAINT ON (p:PATENT) ASSERT p.name IS UNIQUE'''
    with gradb.session() as session:
        with session.begin_transaction() as tx:
            tx.run(query)

def write_neo4j_relationship(data):
    '''Neo4j session for writing relationship.'''

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
    '''Function for writing relationship.'''
    data = get_backward_citation(i)
    data = data.collect()
    relationship = [[i['pnum'], i['col']] for i in data]
    write_neo4j_relationship(relationship)
    relationship = []

def write_property_session(data):
    '''Writing property company name for each node.'''

    query = '''WITH $names AS nested
    UNWIND nested AS x
    MATCH (n:PATENT {name: x[0]})
    SET n.company=x[1]
    '''

    with gradb.session() as session:
        with session.begin_transaction() as tx:
            tx.run(query, names=data)

def write_database_property_company(path):
    '''Function for writing company name.'''

    data = get_publication_number_company(path)
    data = data.collect()
    relationship = [[i['publication_number'], i['name']] for i in data]
    write_property_session(relationship)
    relationship = []


def write_title_session(data):
    '''Writing property company name for each node.'''

    query = '''WITH $names AS nested
    UNWIND nested AS x
    MATCH (n:PATENT {name: x[0]})
    SET n.title=x[1]
    '''

    with gradb.session() as session:
        with session.begin_transaction() as tx:
            tx.run(query, names=data)

def write_database_property_title(path):
    '''Function for writing title of patent.'''

    data = get_publication_number_title(path)
    data = data.collect()
    relationship = [[i['publication_number'], i['col']] for i in data]
    write_title_session(relationship)
    relationship = []

if __name__ == '__main__':

    gradb=GraphDatabase.driver(os.environ['NEO4JURI'], auth=(os.environ['NEO4JUSER'], os.environ['NEO4JPASS']), encrypted=False)

    data_path = file_list(int(sys.argv[1]), int(sys.argv[2]))

#    for i in data_path:
#        '''Loop for writing relationship.'''
#
#        try:
#            write_database_property_company(i)
#            logging.info("Writing company " + i + " succeeded")
#        except Exception:
#             logging.info("Writing company " + i + " failed")
#
#    for i in data_path:
#        '''Loop for writing company name as property of each node.'''
#
#        try:
#            write_database_relationship(i)
#            logging.info("Writing file " + i + " succeeded")
#        except Exception:
#            logging.info("Writing file " + i + " failed")

    for i in data_path:
        '''Loop for writing title.'''

        try:
            write_database_property_title(i)
            logging.info("Writing title " + i + " succeeded")
        except Exception:
             logging.info("Writing title" + i + " failed")
