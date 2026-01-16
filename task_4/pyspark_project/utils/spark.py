from pyspark.sql import SparkSession
from utils.logger import setup_logger

logger = setup_logger("SparkUtils")


def get_spark_session(app_name, jars_packages):
    try:
        logger.info("Initializing SparkSession...")
        spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.jars.packages", jars_packages) \
            .getOrCreate()

        logger.info("SparkSession created successfully.")
        return spark
    except Exception as e:
        logger.error(f"Failed to create SparkSession: {e}")
        raise e