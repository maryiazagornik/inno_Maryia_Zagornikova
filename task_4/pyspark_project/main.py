import os
import yaml
from utils.spark import get_spark_session
from utils.logger import setup_logger
from jobs.analytics import SakilaAnalyzer


def load_config(path="config/config.yaml"):
    with open(path, "r") as f:
        return yaml.safe_load(f)


def main():
    logger = setup_logger("Main")

    # 1. Load Config
    logger.info("Loading configuration...")
    conf = load_config()

    # 2. Get Secrets from Env
    db_user = os.getenv(conf['database']['user_env'])
    db_pass = os.getenv(conf['database']['password_env'])

    if not db_user or not db_pass:
        logger.error("Database credentials not found in environment variables.")
        return

    # 3. Initialize Spark
    spark = get_spark_session(conf['app_name'], conf['spark']['jars'])

    try:
        # 4. Initialize Job
        job = SakilaAnalyzer(spark, conf['database'], db_user, db_pass)

        # 5. Execute Tasks
        job.run_category_film_count()
        job.run_top_actors_by_rental()
        job.run_category_most_spent()
        job.run_films_not_in_inventory()
        job.run_top_actors_children_category()
        job.run_city_customer_stats()
        job.run_complex_rental_analysis()

    except Exception as e:
        logger.error(f"Job failed: {e}")
    finally:
        logger.info("Stopping Spark Session.")
        spark.stop()


if __name__ == "__main__":
    main()