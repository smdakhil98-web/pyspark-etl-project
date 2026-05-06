import logging
from ingestion import get_spark, read_data
from validation import validate_data
from transformation import transform_data
from config import load_config


# Step 1: Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)


def run_pipeline():
    logging.info("Pipeline started")

    # Load config
    config = load_config()

    # Start Spark
    spark = get_spark()
    logging.info("Spark session created")

    # Read data
    df = read_data(spark, config["input_path"])
    logging.info("Data read successfully")

    # Validate data
    df = validate_data(df)
    logging.info("Data validation completed")

    # Transform data
    result = transform_data(df)
    logging.info("Data transformation completed")

    # Show result
    result.show()

    logging.info("Pipeline finished successfully")


# Step 2: Add error handling
if __name__ == "__main__":
    try:
        run_pipeline()
    except Exception as e:
        logging.error(f"Pipeline failed: {str(e)}")