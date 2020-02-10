# Patent Vista

A bird's-eye view of patent landscape.

Click [here](https://docs.google.com/presentation/d/1GYmk3EMiETBooWJDNu6uFWAWcya_pcZEPCu1biHSnyw/edit#slide=id.gc6f73a04f_0_0) to see my presentation.

Click heer to see a video demo.

#### How to install and get it up and running

1. Migrate data from Google Cloud to AWS S3

2. Set up a Spark cluster on AWS

3. Install Neo4j on a separate instance

4. ```
   sh run.sh
   ```

### Introduction

---

Patent Vista is a data pipeline designed to visualizaing patent citation relationships to:

1. Spot VIP (very import patent) of a patent portfolio
2. Check how a patent portfoli is structured (technical fields distribution)
3. Identify potential licensees for monetizing patent asset

### Architecture

---

![](data pipeline.jpg)

1. 1.5 TB data is [migrated](/birdview-patent-landscape/ingestion/README.md) from Google Cloud to Amazon S3
2. The data is splitted into 1667 batches, each batch is 1 GB
3. The batch data is fetched into Apache Spark for batch processing
4. The result of the batch processing is sotred into Neo4j to visualizaing relatinships

### Dataset

---

Raw dataset is patents public data from Google Cloud.

### Engineering challenges

---



### Trade-offs

---

