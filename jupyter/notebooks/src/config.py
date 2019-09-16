import os

os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars /home/jovyan/postgresql-42.2.8.jar pyspark-shell'

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("retailrocket").getOrCreate()

postgres_url = "jdbc:postgresql://db/postgres"
postgres_properties = {
    "driver": "org.postgresql.Driver",
    "user": "postgres",
    "password": "retailrocket"
}

def read_table(table_name):
    return spark.read.jdbc(url=postgres_url,\
    table=table_name, \
    properties=postgres_properties)
