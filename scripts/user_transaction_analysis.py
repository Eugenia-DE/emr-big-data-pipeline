import sys
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, avg, countDistinct, desc, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Dead-letter base path
DEAD_LETTER_PATH = "s3://lab4-big-data-processing-with-emr/dead_letter/"

# Explicit schemas
users_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone_number", StringType(), True),
    StructField("driver_license_number", StringType(), True),
    StructField("driver_license_expiry", StringType(), True),
    StructField("creation_date", StringType(), True),
    StructField("is_active", IntegerType(), True)
])

rentals_schema = StructType([
    StructField("rental_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("vehicle_id", StringType(), True),
    StructField("rental_start_time", StringType(), True),
    StructField("rental_end_time", StringType(), True),
    StructField("pickup_location", IntegerType(), True),
    StructField("dropoff_location", IntegerType(), True),
    StructField("total_amount", FloatType(), True)
])

def create_spark_session():
    return SparkSession.builder.appName("UserTransactionAnalysis").enableHiveSupport().getOrCreate()

def validate_columns(df, required_cols, name):
    for c in required_cols:
        if c not in df.columns:
            raise ValueError(f"Missing column '{c}' in {name} dataset")

def process_user_transaction_data(spark, raw_users_path, raw_rentals_path, output_path):
    logger.info("Starting processing for user and transaction analysis metrics.")

    try:
        # --- Execution date for partitioning and traceability ---
        execution_date = datetime.utcnow().strftime("%Y-%m-%d")

        # --- Header-only schema validation ---
        users_header = spark.read.csv(raw_users_path, header=True, inferSchema=False, samplingRatio=0.0001)
        rentals_header = spark.read.csv(raw_rentals_path, header=True, inferSchema=False, samplingRatio=0.0001)

        validate_columns(users_header, ["user_id", "first_name", "last_name", "email", "is_active", "creation_date"], "users")
        validate_columns(rentals_header, ["rental_id", "user_id", "vehicle_id", "total_amount"], "rentals")

        # --- Load datasets with defined schemas ---
        df_users_raw = spark.read.schema(users_schema).csv(raw_users_path, header=True)
        df_rentals_raw = spark.read.schema(rentals_schema).csv(raw_rentals_path, header=True)

        # --- Drop malformed records and write to dead-letter ---
        df_users = df_users_raw.dropna(subset=["user_id", "first_name", "last_name", "email", "creation_date"])
        df_users_raw.subtract(df_users).write.mode("overwrite").csv(f"{DEAD_LETTER_PATH}/users", header=True)

        df_rentals = df_rentals_raw.dropna(subset=["rental_id", "user_id", "vehicle_id", "total_amount"])
        df_rentals_raw.subtract(df_rentals).write.mode("overwrite").csv(f"{DEAD_LETTER_PATH}/rentals", header=True)

        logger.info("Data loaded and validated. Beginning join and aggregation.")

        # --- Join Rentals with Users ---
        df_joined_users = df_rentals.join(
            df_users.select("user_id", "first_name", "last_name", "email", "is_active", "creation_date"),
            on="user_id",
            how="inner"
        )

        # --- User Transaction Summary ---
        user_transaction_summary = df_joined_users.groupBy(
            "user_id", "first_name", "last_name", "email", "is_active", "creation_date"
        ).agg(
            count("rental_id").alias("total_rental_transactions"),
            sum("total_amount").alias("total_spending"),
            avg("total_amount").alias("average_transaction_value")
        ).withColumn("execution_date", lit(execution_date))

        # --- Overall Statistics ---
        overall_transaction_stats = df_joined_users.agg(
            sum("total_amount").alias("overall_total_revenue"),
            avg("total_amount").alias("overall_avg_transaction_value"),
            count("rental_id").alias("overall_total_transactions"),
            countDistinct("user_id").alias("overall_total_unique_users"),
            countDistinct("vehicle_id").alias("overall_total_unique_vehicles_rented")
        ).withColumn("execution_date", lit(execution_date))

        # --- Top 100 Users ---
        top_users_by_spending = user_transaction_summary.orderBy(col("total_spending").desc()).limit(100)

        # --- Output Paths with Execution Date Partitioning ---
        summary_out = f"{output_path}/user_transaction_summary/date={execution_date}"
        stats_out = f"{output_path}/overall_transaction_statistics/date={execution_date}"
        top_users_out = f"{output_path}/top_users_by_spending/date={execution_date}"

        logger.info(f"Writing user transaction summary to: {summary_out}")
        user_transaction_summary.write.mode("overwrite").parquet(summary_out)

        logger.info(f"Writing overall stats to: {stats_out}")
        overall_transaction_stats.write.mode("overwrite").parquet(stats_out)

        logger.info(f"Writing top users to: {top_users_out}")
        top_users_by_spending.write.mode("overwrite").parquet(top_users_out)

        logger.info("User transaction data processed and written to S3 successfully.")

    except Exception as e:
        logger.error(f"Error during Spark job execution: {e}", exc_info=True)
        sys.exit(1)

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
