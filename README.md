# My ETL Project

My ETL Project provides a scalable framework for extracting, transforming, and loading (ETL) data using Apache Spark and Apache Airflow. The project is designed to support multi-layer data processing, starting with a Bronze layer (data ingestion) with placeholders for Silver and Gold layers (data transformation and refinement) to be developed in the future.

## Project Structure

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

- **configs/**: Contains configuration files in HOCON format.
  - **bronze.conf**: Configuration for the Bronze layer including Spark settings, Hive metastore details, and ETL-specific parameters such as JDBC connection settings and table definitions.
  - **silver.conf** & **gold.conf**: Placeholder configuration files for future Silver and Gold layers.
- **dags/**: Contains the Airflow DAG definition.
  - **my_etl_dag.py**: Defines the DAG to run the ETL job daily via the SparkSubmitOperator.
- **main_etl_job.py**: The main Python script that loads configurations, creates a Spark session, and executes the ETL process.
- **requirements.txt**: Lists project dependencies (e.g., `pyhocon`, `pyspark`, `apache-airflow`).

## Prerequisites

- **Apache Spark** (v3.4.0)
- **Apache Airflow** (v2.3.0)
- **Python** (v3.7+ recommended)
- Java (for Spark)
- A properly configured Hadoop/YARN or local Spark deployment

## Installation

1. **Clone the repository:**

   ```bash
   git clone https://your-repository-url.git
   cd my_etl_project
   ```

2. **Set up a virtual environment (recommended):**

   ```bash
   python -m venv venv
   source venv/bin/activate
   ```

3. **Install dependencies:**

   ```bash
   pip install -r requirements.txt
   ```

## Configuration

The project uses HOCON configuration files to manage settings for each ETL layer.

- **bronze.conf**:
  - **app**: Defines the application name (e.g., "bronzeLayer").
  - **sparkConf**: Sets Spark properties such as the SQL warehouse directory, scheduler mode, and Hive metastore URIs.
  - **etlConf**: Specifies ETL-related settings including JDBC connection details for Microsoft SQL Server and PostgreSQL, and defines the tables to ingest.
  
  *Note: Update placeholder values (e.g., `YOUR_HOST`, `YOUR_DATABASE`, `YOUR_USERNAME`, `YOUR_PASSWORD`) with actual connection details.*

- **silver.conf** & **gold.conf**:
  - Currently serve as placeholders with similar Spark configurations as the Bronze layer.
  - Future enhancements will include additional ETL and transformation logic.

## Running the ETL Job

### Using Spark Submit

Run the ETL job with a command similar to the following (adjust paths and parameters as needed):

```bash
spark3-submit \
  --master yarn \
  --deploy-mode client \
  --archives pyspark_myprj_env.tar.gz#pyspark_myprj_env \
  --conf spark.pyspark.python=./pyspark_myprj_env/bin/python \
  --conf spark.pyspark.driver.python=./unarchive_new/bin/python \
  --driver-memory 2G \
  --executor-memory 4G \
  --principal user1@BIGDATA.YOURHOST.COM \
  --keytab /home/user1/user1.keytab \
  --driver-class-path /home/user1/user1_curl_point/postgresql-42.7.1.jar:/home/user1/user1_curl_point/mssql-jdbc-9.2.1.jre8.jar \
  --jars /home/user1/user1_curl_point/postgresql-42.7.1.jar,/home/user1/user1_curl_point/mssql-jdbc-9.2.1.jre8.jar \
  --conf spark.shuffle.service.enabled=true \
  --conf spark.shuffle.service.port=7447 \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.dynamicAllocation.minExecutors=0 \
  --conf spark.dynamicAllocation.maxExecutors=20 \
  --conf spark.dynamicAllocation.executorIdleTimeout=60 \
  --conf spark.dynamicAllocation.schedulerBacklogTimeout=1 \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=hdfs://nameservice1/user/spark/applicationHistory \
  --conf spark.yarn.historyServer.address=http://mn03.yourhost.com:18088 \
  --conf spark.yarn.historyServer.allowTracking=true \
  /home/user1/user1_curl_point/my_prj_modernization/main_etl_job.py \
  /home/user1/user1_curl_point/my_prj_modernization/configs/bronze.conf
```

### Running the Airflow DAG

The Airflow DAG located in `dags/my_etl_dag.py` schedules the Bronze layer ETL job to run daily.

1. **Configure Airflow**:
   - Place the `my_etl_dag.py` file in your Airflow DAGs folder.
   - Update the `BASE_PATH` variable in the DAG file to point to your project directory.
2. **Start Airflow**:
   ```bash
   airflow scheduler
   airflow webserver
   ```
3. **Trigger the DAG**:
   - Use the Airflow web interface to monitor and manually trigger the DAG if needed.

## Main ETL Job Overview

- **Configuration Parsing**: The `main_etl_job.py` script uses the `pyhocon` library to load and parse configuration files.
- **Spark Session Creation**: A Spark session is created with Hive support using the settings specified in the configuration.
- **ETL Process**: The script reads data from source databases via JDBC, applies basic transformations, and writes the data to Iceberg tables in Hive.
- **Error Handling**: Errors during configuration or data processing are logged, and exceptions are raised for critical issues.

## Future Enhancements

- **Silver and Gold Layers**: Further data transformations and refinements will be implemented.
- **Advanced Transformations**: Incorporation of more complex data processing logic as requirements evolve.
- **Testing & Monitoring**: Integration with testing frameworks and monitoring tools for enhanced reliability.

## Contributing

Contributions are welcome! Please fork the repository and submit pull requests for any improvements or bug fixes.
