from pyspark.sql import SparkSession
from pyspark import SparkConf
import os

def get_spark():
    os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"

    conf = SparkConf()
    conf.set("spark.python.worker.faulthandler.enabled", "true") 
    conf.set("spark.driver.memory", "2g")      
    conf.set("spark.executor.memory", "2g")
    conf.set("spark.sql.shuffle.partitions", "2")
    conf.set("spark.network.timeout", "600s")
    conf.set("spark.executor.heartbeatInterval", "60s")

    spark = (
        SparkSession.builder
        .appName("Spark-DE-Assessment")
        .master("local[1]")   
        .config(conf=conf)
        .getOrCreate()
    )
    return spark
