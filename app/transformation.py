from pyspark.sql.functions import col, sum

def transform_data(df):
    df = df.dropDuplicates()

    df = df.withColumn("sales_amount", col("sales_amount").cast("int"))

    result = df.groupBy("product_id") \
        .agg(sum("sales_amount").alias("total_sales"))

    return result