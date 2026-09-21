# Apache Spark: Core Architecture & Concepts

---

## 1. What Is Apache Spark?

Apache Spark is a unified engine designed for large-scale distributed data processing, operating seamlessly on-premises in data centers or across cloud environments.

* **In-Memory Computation:** Spark stores intermediate computations in memory rather than writing them to disk after each step, making it substantially faster than legacy Hadoop MapReduce.
* **Unified Ecosystem:** Spark bundles high-level composable APIs for machine learning (**MLlib**), SQL/relational queries (**Spark SQL**), stream processing (**Structured Streaming**), and graph analytics (**GraphX**).

---

## 2. Key Architectural Pillars

### Speed

* **Multithreading & Parallelism:** Executes workloads across nodes concurrently via worker threads.
* **Query Optimization & DAG:** The DAG (Directed Acyclic Graph) scheduler and Catalyst optimizer transform operations into efficient execution graphs distributed across worker tasks.
* **Project Tungsten:** Uses a physical execution engine with whole-stage code generation to emit compact, JVM-optimized bytecode at runtime.
* **Reduced I/O Bottlenecks:** Retaining intermediate state in RAM minimizes costly disk write/read cycles.

### Ease of Use

* **Core Abstraction:** Built on the fundamental **Resilient Distributed Dataset (RDD)**, atop which higher-level structured abstractions (**DataFrames** and **Datasets**) are constructed.
* **Transformations & Actions:** Simplifies distributed computing into functional primitives (`map`, `filter`, `join`, `count`, etc.) available in standard programming languages.

### Modularity

* **Polyglot Interface:** Write workloads interchangeably in **Scala, Java, Python, SQL, or R**.
* **One Unified Application:** Interleave streaming, SQL queries, and machine learning pipelines in a single codebase without maintaining separate, disconnected computing engines.

### Extensibility (Compute/Storage Decoupling)

* Spark is strictly an in-memory computation layer; it does not bundle its own dedicated storage system.
* **Universal Connectivity:** Uses pluggable `DataFrameReader` and `DataFrameWriter` APIs to pull from and write to:
* **Databases & Warehouses:** Apache Hive, Apache Cassandra, Apache HBase, MongoDB, JDBC/RDBMS.
* **File Systems & Object Storage:** HDFS, Amazon S3, Azure Blob Storage/ADLS, Google Cloud Storage.
* **Event Streams:** Apache Kafka, AWS Kinesis.



---

## 3. Apache Spark Component Stack

```
+-------------------------------------------------------------------+
|  Spark SQL   |   Spark MLlib   |   Structured Streaming   |  GraphX  |
+-------------------------------------------------------------------+
|                  Catalyst Optimizer & Tungsten Engine             |
+-------------------------------------------------------------------+
|                           Spark Core                              |
+-------------------------------------------------------------------+

```

### Spark SQL

Processes structured tabular data from relational systems or structured files (CSV, JSON, Avro, ORC, Parquet). Allows writing pure SQL queries directly over Spark DataFrames.

```python
# Create a temporary in-memory view from a JSON source
spark.read.json("s3://apache_spark/data/committers.json").createOrReplaceTempView("committers")

# Query with ANSI SQL syntax
results = spark.sql("""
    SELECT name, org, module, release, num_commits
    FROM committers 
    WHERE module = 'mllib' AND num_commits > 10
    ORDER BY num_commits DESC
""")

```

### Spark MLlib

A machine learning library providing feature extractors, transformers, model training pipelines, and persistence utilities.

```python
from pyspark.ml.classification import LogisticRegression

# Load dataset
training = spark.read.csv("s3://path/to/train.csv", header=True, inferSchema=True)
test = spark.read.csv("s3://path/to/test.csv", header=True, inferSchema=True)

# Initialize estimator and fit model
lr = LogisticRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)
lrModel = lr.fit(training)

# Generate predictions
predictions = lrModel.transform(test)

```

### Spark Structured Streaming

A stream-processing engine built on top of the Catalyst SQL optimizer. It treats a real-time data stream as an unbounded, continuously appended table, enabling end-to-end fault tolerance and exactly-once processing guarantees.

### GraphX

A specialized library for graph-parallel computation (network graphs, topologies, routes). Provides built-in community algorithms such as:

* **PageRank**
* **Connected Components**
* **Triangle Counting**

---

## 4. Distributed Execution Architecture

A Spark application runs as an independent set of processes on a cluster, coordinated by the **Spark driver**.

```
                       +-------------------+
                       |    Spark Driver   |
                       |  (SparkSession)   |
                       +---------+---------+
                                 |
                     Requests / Allocates Resources
                                 |
                       +---------v---------+
                       |  Cluster Manager  |
                       | (YARN/K8s/Stand.) |
                       +----+---------+----+
                            |         |
               +------------+         +------------+
               |                                   |
     +---------v---------+               +---------v---------+
     |    Worker Node    |               |    Worker Node    |
     | +---------------+ |               | +---------------+ |
     | | Spark Executor| |               | | Spark Executor| |
     | |  [Tasks...]   | |               | |  [Tasks...]   | |
     | +---------------+ |               | +---------------+ |
     +-------------------+               +-------------------+

```

### Core Execution Roles

* **Spark Driver:** The central controller process.
* Instantiates the `SparkSession`.
* Negotiates execution resources with the Cluster Manager.
* Translates logical operations into a DAG of physical execution stages and schedules individual tasks across executors.


* **SparkSession:** Introduced in Spark 2.0 as the unified entry point. It supersedes older disparate contexts (`SparkContext`, `SQLContext`, `HiveContext`, `StreamingContext`), exposing runtime configs, metadata catalogs, and data-reading APIs through a single handle.
* **Cluster Manager:** Controls and allocates physical nodes/containers across the cluster. Spark supports:
* **Standalone** (built-in simple manager)
* **Apache Hadoop YARN**
* **Kubernetes**
* **Apache Mesos**


* **Spark Executor:** A persistent JVM worker process running on a cluster node. Responsible for executing tasks, caching data partitions in memory, and reporting execution health back to the driver.

---

## 5. Deployment Modes

### Table 1-1. Cheat Sheet for Spark Deployment Modes

| Mode | Spark Driver | Spark Executor | Cluster Manager |
| --- | --- | --- | --- |
| **Local** | Runs on a single JVM (e.g., developer laptop) | Runs within the same JVM as the driver | Runs on the local host |
| **Standalone** | Can run on any node in the cluster | Launches its own executor JVM per node | Allocated across cluster nodes |
| **YARN (client)** | Runs on the client machine outside the cluster | Runs within YARN `NodeManager` containers | YARN Resource Manager & Application Master coordinate allocation |
| **YARN (cluster)** | Runs inside the cluster within the YARN Application Master | Runs within YARN `NodeManager` containers | Same as YARN client mode |
| **Kubernetes** | Runs inside an allocated Kubernetes Pod | Each executor launches within its own isolated Pod | Kubernetes API / Master |

---

## 6. Distributed Data and Partitions

Physical data on disk (HDFS, S3, ADLS) is split into chunks called **partitions**. Spark maps these physical partitions directly into its logical abstraction—the **DataFrame** or **RDD**.

* **Data Locality:** Spark schedules tasks on the executor physically closest to the requested data partition (same host or rack) to avoid network transport costs.
* **Parallelism Control:** The number of partitions determines the maximum degree of task parallelism.

```python
# Force an input text file into exactly 8 partitions
log_df = spark.read.text("path_to_large_text_file").repartition(8)
print(log_df.rdd.getNumPartitions())  # Output: 8

# Generate a sequence of 10,000 integers distributed across 8 partitions
df = spark.range(0, 10000, 1, 8)
print(df.rdd.getNumPartitions())      # Output: 8

```