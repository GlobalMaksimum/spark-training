# What is Spark?
* Open source cluster computing engine
  - Very fast: In-memory ops 100x faster than MR
  - On-disk ops 10x faster than MR
  - General purpose: MR, SQL, streaming, machine learning, analytics
  - Compatible: Clusters run on Hadoop/YARN, Mesos, standalone, Kubernetes
    + Works with many data stores: HDFS, S3, Cassandra, HBase, Hive, ...
  - Easier to code: Word count in 2 lines

* Spark's roots:
  - Came out of Berkeley AMP Lab
  - Now top-level Apache project
  - Version 1.6 / Jan. 2016
  - Version 2.0 / July 2016, Version 2.1 / Dec. 2016

“First Big Data platform to integrate batch, streaming and interactive
computations in a unified framework” – stratio.com

# The Spark Stack

* Spark provides a stack of libraries built on core Spark
    - Core Spark provides the fundamental Spark abstraction: Resilient Distributed
Datasets (RDDs)
* Spark SQL works with structured data
* MLlib supports scalable machine learning
* Spark Streaming applications process data in real time
* GraphX works with graphs and graph-parallel computation

# Spark Core
* Building blocks for distributed compute engine
  - Task schedulers and memory management
  - Fault recovery (recovers missing pieces on node failure)
  - Storage system interfaces

* Defines Spark API and data model

* Core Data Model: RDD (Resilient Distributed Dataset)
  - Distributed collection of items
  - Can be worked on in parallel
  - Easily created from many data sources

* Spark API: Scala, Python, Java, and R
  - Compact API for working with RDD and interacting with Spark
  - Much easier to use than MapReduce API

# Resilent Distributed dataset
    * first class citizen
    * low level
    * complete control of how spark working do any optimization
    * but without optimizations ?

What describe RDD
* A list of partitions
* A function for computing each split
* A list of dependencies on other RDDs 
* Optionally, a Partitioner for key-value RDDs (e.g., to say that the RDD is hash-partitioned)
* Optionally, a list of preferred locations on which to compute each split (e.g., block locations for a Hadoop Distributed File System [HDFS] file)
