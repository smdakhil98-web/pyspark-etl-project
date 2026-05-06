from pyspark.sql.functions import col

def validate_data(df):
    null_count = df.filter(col("sales_amount").isNull()).count()

    if null_count > 0:
        print(f"WARNING: Found {null_count} null values")

    df = df.dropna(subset=["sales_amount"])
    return df