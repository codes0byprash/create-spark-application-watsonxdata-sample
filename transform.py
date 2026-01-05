from pyspark.sql import Row, SparkSession

def run_transformations(spark: SparkSession) -> int:
    print("Running transformation logic...")

    # Sample data
    data = [
        Row(id=1, name="Alice", status="Active"),
        Row(id=2, name="Bob", status="Inactive")
    ]
    df = spark.createDataFrame(data)

    # Filter active users
    df_filtered = df.filter(df.status == "Active")

    print("Transformation Preview:")
    df_filtered.show()

    return df_filtered.count()
