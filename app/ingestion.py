from pyspark.sql import SparkSession

def get_spark():
    return SparkSession.builder.appName("RetailETL").getOrCreate()

def read_data(spark, path):
    return spark.read.option("header", True).csv(path)