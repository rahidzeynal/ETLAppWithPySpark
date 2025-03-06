# This file describs the overall architecture, installation steps, and usage instructions for the skeleton ETL pipeline.

---
---
---
---

## ETL Pipeline Skeleton (Bronze → Silver → Gold) with PySpark, Apache Iceberg, Airflow, and pyhocon

This repository demonstrates a basic framework for an ETL pipeline using PySpark, Apache Iceberg, Airflow for scheduling, and pyhocon for configuration management. The pipeline is split into three layers:

1. **Bronze**: Ingest raw data from sources (e.g., Postgres, MSSQL) into a raw storage layer.  
2. **Silver**: Refine and cleanse data from the Bronze layer into a more structured form.  
3. **Gold**: Final layer for curated, business-ready data, often used for analytics or reporting.

---

## Project Structure

```
my_etl_project/
├── configs/
│   ├── bronze.conf
│   ├── silver.conf
│   └── gold.conf
├── dags/
│   └── my_etl_dag.py
├── main_etl_job.py
├── requirements.txt
└── README.md
```

- **`configs/`**  
  - `bronze.conf`, `silver.conf`, `gold.conf` are separate HOCON configuration files for each layer.  
  - Each config file includes the Spark configuration (`spark-config`), paths for data storage, and any relevant layer-specific settings or data source details.

- **`dags/`**  
  - `my_etl_dag.py` is the Airflow DAG definition. It has three tasks (Bronze → Silver → Gold) in a linear sequence, each triggered via SparkSubmitOperator.

- **`main_etl_job.py`**  
  - The core PySpark ETL script. Determines which steps to run based on the config file (bronze, silver, or gold).

- **`requirements.txt`**  
  - Python dependencies needed for this project (pyhocon, pyspark, airflow, etc.).

- **`README.md`**  
  - This documentation file.

---

## Requirements

- **Python 3.7+**  
- **Apache Spark 3.x**  
- **Apache Airflow 2.x**  
- **JDBC drivers** (for Postgres, MSSQL, or any other source you need to connect to)  
- **Apache Iceberg** (with the appropriate Spark and Iceberg extensions configured if you plan to write/read using Iceberg format)  
- A configured **Airflow** environment with a working **SparkSubmitOperator** setup (i.e., a valid `conn_id="spark_default"`).

Install required Python libraries:

```bash
pip install -r requirements.txt
```

---

## Configuration

You have **three HOCON config files**, one for each layer:

### Example: `configs/bronze.conf`

```hocon
{
  layer = "bronze"

  spark-config: {
    appName: "BronzeLayerJob"
    master: "local[*]"
    # Additional Spark + Iceberg configs
  }

  data-sources: {
    postgres: {
      url = "jdbc:postgresql://YOUR_POSTGRES_HOST:5432/YOUR_DB"
      user = "YOUR_USERNAME"
      password = "YOUR_PASSWORD"
      driver = "org.postgresql.Driver"
    }
    mssql: {
      url = "jdbc:sqlserver://YOUR_MSSQL_HOST:1433;databaseName=YOUR_DB"
      user = "YOUR_USERNAME"
      password = "YOUR_PASSWORD"
      driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }
  }

  paths: {
    bronze = "s3://your-bucket/bronze-layer"
    silver = "s3://your-bucket/silver-layer"
    gold   = "s3://your-bucket/gold-layer"
  }
}
```

> The `layer` parameter must be set to `"bronze"`, `"silver"`, or `"gold"` in each respective config so the script knows which portion of the pipeline to run.

---

## Usage

### 1. Running Locally (Without Airflow)

You can run each layer’s job manually by passing the corresponding config file to `main_etl_job.py`:

```bash
# Bronze layer
python main_etl_job.py configs/bronze.conf

# Silver layer
python main_etl_job.py configs/silver.conf

# Gold layer
python main_etl_job.py configs/gold.conf
```

This will launch a local Spark session, read the config, and execute the appropriate ingestion/transformation steps.

### 2. Running via Airflow

1. Ensure your Airflow environment is correctly configured, and you have a Spark connection (`spark_default` or equivalent) set up in Airflow connections.  
2. Copy (or symlink) the `my_etl_dag.py` into your Airflow `dags/` directory or set your `AIRFLOW_HOME` to this project.  
3. Start Airflow services (webserver, scheduler, etc.).  
4. In the Airflow UI, enable the **`my_etl_dag`**.  
5. Airflow will run the three tasks in sequence:

   - **`run_bronze_layer`** → Reads from sources, writes to Bronze.  
   - **`run_silver_layer`** → Reads from Bronze, writes to Silver.  
   - **`run_gold_layer`** → Reads from Silver, writes to Gold.  

### 3. Customizing the Spark Submit Parameters

Inside the DAG (`my_etl_dag.py`), you can modify each `SparkSubmitOperator` with your desired memory, cores, or other Spark config:

```python
run_bronze = SparkSubmitOperator(
    task_id="run_bronze_layer",
    application="/path/to/my_etl_project/main_etl_job.py",
    name="BronzeLayerJob",
    conn_id="spark_default",
    application_args=["/path/to/my_etl_project/configs/bronze.conf"],
    executor_cores=2,
    executor_memory="2g",
    driver_memory="1g",
    verbose=True
)
```

---

## Troubleshooting

1. **JDBC driver issues**: Make sure you have the correct driver jar(s) accessible to Spark. If running locally, you might need to place them in your Spark `jars/` directory or supply `--jars` when submitting Spark jobs.  
2. **Iceberg configuration**: Ensure your Spark session is properly configured for Iceberg (e.g., adding `spark.sql.extensions` and setting up catalogs). This may involve additional Spark config or referencing a Hive metastore.  
3. **Airflow scheduling**: If tasks do not appear or do not run, ensure your Airflow environment is active, the DAG is unpaused, and you don’t have conflicting DAG IDs or errors in your logs.  

---

## Notes & Next Steps

- **Data Validation & Quality Checks**: In a production pipeline, incorporate validation or schema checks before writing to subsequent layers.  
- **Error Handling & Logging**: Integrate robust error handling, log collection, and alerting to track failures and data anomalies.  
- **Dependency & Partition Management**: If data is partitioned by date or other fields, ensure your read/write operations handle those partitions correctly.  
- **Scalability**: For larger datasets, configure Spark to run on a cluster (e.g., YARN, Kubernetes, or a managed service like EMR/Databricks).  
- **Security**: Secure passwords and sensitive credentials using environment variables or Airflow connections, rather than storing them in plain text.

---
