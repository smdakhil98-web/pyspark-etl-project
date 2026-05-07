import logging

from ingestion import get_spark, read_data
from transformation import transform_data
from validation import validate_data
from config import load_config


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
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
    df = transform_data(df)
    logging.info("Data transformation completed")

    # Write output
    df.write.mode("overwrite").csv(config["output_path"], header=True)
    logging.info("Data written successfully")

    logging.info("Pipeline completed successfully")
# test trigger

if __name__ == "__main__":
    run_pipeline()