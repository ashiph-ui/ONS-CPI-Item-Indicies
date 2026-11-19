from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def get_spark(master: str = "local[2]", num_partitions: int = 8) -> SparkSession:
    """
    Starts a local Spark session.

    """
    spark = (
        SparkSession.builder
        .master(master)                      
        .appName("ONS-CPI")
        .config("spark.sql.shuffle.partitions", num_partitions)   # default 200 is for clusters
        .getOrCreate()
    )
    return spark

    
   