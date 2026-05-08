from pyspark.sql import SparkSession
from app.ingestion import get_spark, read_data
from app.transformation import transform_data
from app.validation import validate_data

def main():
    print("Pipeline started")

    # Step 1: Create Spark Session
    spark = get_spark()

    # Step 2: Read Data
    input_path = "gs://etl-pyspark-bucket/data/input.csv"
    df = read_data(spark, input_path)
    print("Data read successfully")

    # Step 3: Validate Data
    df_valid = validate_data(df)
    print("Data validation completed")

    # Step 4: Transform Data
    df_transformed = transform_data(df_valid)
    print("Data transformation completed")

    # Step 5: Write Output
    output_path = "gs://etl-pyspark-bucket/output/"
    df_transformed.write.mode("overwrite").csv(output_path, header=True)

    print("Pipeline completed successfully")

if __name__ == "__main__":
    main()