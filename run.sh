#!/bin/bash
#
# Use this shell script to compile (if necessary) your code and then execute it. Belw is an example of what might be found in this file if your program was written in Python 3.7

spark-submit \
    --master spark://ec2-35-166-229-121.us-west-2.compute.amazonaws.com:7077 \
    --conf "spark.driver.extraClassPath=/usr/local/spark/jars/aws-java-sdk-bundle-1.11.375.jar" \
    --conf "spark.driver.extraClassPath=/usr/local/spark/jars/hadoop-aws-3.2.0.jar" \
    --executor-memory 3G \
    --conf "spark.cores.max=15" \
    --conf "spark.executor.cores=5" \
    --jars /usr/local/spark/jars/aws-java-sdk-bundle-1.11.375.jar,/usr/local/spark/jars/hadoop-aws-3.2.0.jar \
    ./data-processing/batch.py 491 1668
   # ./data-processing/batch.py 805 806

