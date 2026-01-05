from pyspark.sql import SparkSession
from transform import run_transformations

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("Plex-API-Transformation")
        .getOrCreate()
    )

    # Run the transformation
    count = run_transformations(spark)
    print(f"Active records count: {count}")

    # Stop Spark session
    spark.stop()
