from pyspark.sql.functions import col, sum, when

def transform_data(df):

    # Remove duplicates
    df = df.dropDuplicates()

    # Cast column
    df = df.withColumn("sales_amount", col("sales_amount").cast("int"))

    # 🔥 Handle NULL values
    df = df.withColumn(
        "sales_amount",
        when(col("sales_amount").isNull(), 0).otherwise(col("sales_amount"))
    )

    # 🔥 Handle negative values
    df = df.withColumn(
        "sales_amount",
        when(col("sales_amount") < 0, 0).otherwise(col("sales_amount"))
    )

    # Aggregation
    result = df.groupBy("product_id") \
        .agg(sum("sales_amount").alias("total_sales"))

    return result