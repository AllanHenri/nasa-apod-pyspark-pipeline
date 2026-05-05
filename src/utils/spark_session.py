from pyspark.sql import SparkSession

def create_spark_session(app_name: str = "NASA_APOD_Pipeline") -> SparkSession:
    return(
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .getOrCreate()
    )