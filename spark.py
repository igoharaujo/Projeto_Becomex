from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor


builder = SparkSession.builder \
    .appName("SparkTurboDelta") \
    .master("local[*]") \
    .config("spark.driver.memory", "6g") \
    .config("spark.executor.memory", "6g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

def ingest_data_bronze(read_path, table_name, database_name):
    try:
        df = spark.read.format("delta").load(read_path)
        df.write.mode('overwrite').option('overwriteSchema', True).format('delta').saveAsTable(f"{database_name}.{table_name}")
    except Exception as e:
        print(f"❌ tabela {table_name} Falhou - {str(e)}")

def execute_spark():

    spark.sql("CREATE DATABASE IF NOT EXISTS bd_becomex LOCATION 'storage'")

    bd_becomex = "spark-warehouse/bd_becomex.db"

    # Lista os nomes das tabelas (pastas com _delta_log)
    table_names = [d.name for d in Path(bd_becomex).iterdir() if d.is_dir() and (d / "_delta_log").exists()]

    # Coloca num dicionário com a chave 'nome'
    parameters = {'nome': table_names}

    bd_becomex = "spark-warehouse/bd_becomex.db"

    parameters = {'nome': table_names}

    # Cria tuplas com os argumentos (read_path, table_name, database_name)
    args = [(f"{bd_becomex}/{name}", name, "bd_becomex") for name in parameters['nome']]

    # Roda em paralelo
    with ThreadPoolExecutor(max_workers=12) as executor:
        executor.map(lambda p: ingest_data_bronze(*p), args)
