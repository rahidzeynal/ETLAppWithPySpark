import sys
import argparse
import os
import re
from pyhocon import ConfigFactory, ConfigTree
from pyspark.sql import SparkSession, DataFrame, functions as F, Window
from pyspark.sql.utils import AnalysisException
from pyspark import SparkConf

def conf_to_SparkConf(config: ConfigTree) -> SparkConf:
    try:
        spark_conf = SparkConf()
        config = config.get('sparkConf', {})  

        def extract_and_set(config, parent_key=""):
            for key, value in config.items():
                full_key = f"{parent_key}.{key}" if parent_key else key
                
                if isinstance(value, ConfigTree) or isinstance(value, dict):
                    extract_and_set(value, full_key)
                else:
                    spark_conf.set(full_key, str(value))
                    print(f"Setting SparkConf: {full_key} = {value}") 

        extract_and_set(config)
    except Exception as e: 
        print(f"Something Wrong with Config: {e}")
        raise 
    
    return spark_conf

def create_spark_session(config: ConfigTree) -> SparkSession:
    try: 
        spark_conf  = conf_to_SparkConf(config) 
        spark = SparkSession.builder \
            .config(conf=spark_conf) \
            .enableHiveSupport() \
            .getOrCreate()

        return spark
    except Exception as e:
        print(f"Error creating Spark session: {e}")
        raise


def etl(config: ConfigTree, spark: SparkSession) -> None:
    etl_conf = {}
    etl_config = config.get('etlConf', {})

    def extract_and_set(config, parent_key=""):
        for key, value in config.items():
            full_key = f"{parent_key}.{key}" if parent_key else key

            if isinstance(value, ConfigTree) or isinstance(value, dict):
                extract_and_set(value, full_key)
            else:
                etl_conf[full_key] = str(value)
                print(f"Setting etlConf: {full_key} = {value}")

    extract_and_set(etl_config)

    for table in etl_config.get("tables", []):
        try:
            source_connection = table.get('connection')
            source_driver = source_connection.get('driver')
            source_url = source_connection.get('url')
            source_username = source_connection.get('username')
            source_password = source_connection.get('password')
            source_table = f"{table.get('schema')}.{table.get('name')}"
            query = "SELECT * FROM " + source_table

            dest_table = "akart." + re.sub(r'\W+', '_', source_table).strip('_')

            print(f"Reading data for table: {source_table}")
            df = spark.read.format("jdbc").options(
                driver=source_driver,
                url=source_url,
                query=query,
                user=source_username,
                password=source_password,
            ).load()

            writer = df.writeTo(dest_table).using("iceberg")
            if 'table_properties' in table:
                for key, value in table.get('table_properties').items():
                    writer = writer.tableProperty(key, value)

            writer.createOrReplace()
        except AnalysisException as ae:
            print(f"AnalysisException for table {table.get('name')}: {ae}")
        except Exception as e:
            print(f"Error processing table {table.get('name')}: {e}")

    print("Data inserted successfully.")

def main(config_path: str):
    if not os.path.exists(config_path):
        print(f"Error: The path {config_path} does not exist.")
        return

    config = ConfigFactory.parse_file(config_path)
    spark = create_spark_session(config)
    etl(config)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the ETL job with the specified configuration file.")
    parser.add_argument("config_path", type=str, help="Path to the configuration file.")
    args = parser.parse_args()
    
    main(config_path=args.config_path)
