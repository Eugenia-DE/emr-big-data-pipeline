import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, avg, countDistinct, desc
from pyspark.sql.types import TimestampType
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_spark_session():
    """
    Creates and configures a SparkSession with Hive support.
    """
    spark = SparkSession.builder \
        .appName("UserTransactionAnalysis") \
        .enableHiveSupport() \
        .getOrCreate()
    return spark

def process_user_transaction_data(spark, raw_users_path, raw_rentals_path, output_path):
    """
    Processes user and rental transaction data to derive user behavior metrics.

    Args:
        spark (SparkSession): The SparkSession object.
        raw_users_path (str): S3 path to the raw users dataset.
        raw_rentals_path (str): S3 path to the raw rental transactions dataset.
        output_path (str): S3 path to save the processed data.
    """
    logger.info(f"Starting processing for user and transaction analysis metrics.")
    logger.info(f"Reading users from: {raw_users_path}")
    logger.info(f"Reading rental transactions from: {raw_rentals_path}")

    try:
        # 1. Load DataFrames
        df_users = spark.read.csv(raw_users_path, header=True, inferSchema=True)
        df_rentals = spark.read.csv(raw_rentals_path, header=True, inferSchema=True)

        logger.info("Raw DataFrames loaded successfully.")
        logger.info(f"Users schema: {df_users.printSchema()}")
        logger.info(f"Rentals schema: {df_rentals.printSchema()}")

        # 2. Join Rental Transactions with Users
        df_joined_users = df_rentals.join(
            df_users.select("user_id", "first_name", "last_name", "email", "is_active", "creation_date"),
            on="user_id",
            how="inner"
        )
        logger.info("Joined rental transactions with users.")

        # 3. Aggregate User Transaction Metrics
        user_transaction_summary = df_joined_users.groupBy(
            "user_id", "first_name", "last_name", "email", "is_active", "creation_date"
        ).agg(
            count("rental_id").alias("total_rental_transactions"),
            sum("total_amount").alias("total_spending"),
            avg("total_amount").alias("average_transaction_value")
        )
        logger.info("Aggregated user transaction metrics.")

        # 4. Overall Transaction Statistics
        overall_transaction_stats = df_joined_users.agg(
            sum("total_amount").alias("overall_total_revenue"),
            avg("total_amount").alias("overall_avg_transaction_value"),
            count("rental_id").alias("overall_total_transactions"),
            countDistinct("user_id").alias("overall_total_unique_users"),
            countDistinct("vehicle_id").alias("overall_total_unique_vehicles_rented")
        )
        logger.info("Calculated overall transaction statistics.")

        # 5. Identify Top Users by Spending
        top_users_by_spending = user_transaction_summary.orderBy(col("total_spending").desc()).limit(100)
        logger.info("Identified top 100 users by spending.")

        # 6. Write Results to S3 in Parquet format
        output_user_summary_path = f"{output_path}/user_transaction_summary"
        output_overall_stats_path = f"{output_path}/overall_transaction_statistics"
        output_top_users_path = f"{output_path}/top_users_by_spending"

        logger.info(f"Writing user transaction summary data to: {output_user_summary_path}")
        user_transaction_summary.write.mode("overwrite").parquet(output_user_summary_path)
        logger.info(f"Writing overall transaction statistics data to: {output_overall_stats_path}")
        overall_transaction_stats.write.mode("overwrite").parquet(output_overall_stats_path)
        logger.info(f"Writing top users by spending data to: {output_top_users_path}")
        top_users_by_spending.write.mode("overwrite").parquet(output_top_users_path)

        logger.info("Processed data written to S3 successfully.")

    except Exception as e:
        logger.error(f"Error during Spark job execution: {e}", exc_info=True)
        sys.exit(1) # Exit with a non-zero code to indicate failure

if __name__ == "__main__":
    if len(sys.argv) != 4:
        logger.error("Usage: user_transaction_analysis.py <raw_users_path> <raw_rentals_path> <output_path>")
        sys.exit(1)

    raw_users_path = sys.argv[1]
    raw_rentals_path = sys.argv[2]
    output_path = sys.argv[3]

    spark = create_spark_session()
    process_user_transaction_data(spark, raw_users_path, raw_rentals_path, output_path)
    spark.stop()
    logger.info("SparkSession stopped.")
