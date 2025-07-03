import sys
import logging
import boto3
from pyspark.sql.functions import col, sum, count, avg, max, min, countDistinct, lit
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum, count, avg, max, min, countDistinct
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType, TimestampType
)

# Set execution date
execution_date = datetime.utcnow().strftime("%Y-%m-%d")

# Logging to file
LOG_FILE = "/tmp/vehicle_location_performance.log"
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Paths
LOG_S3_BUCKET = "lab4-big-data-processing-with-emr"
LOG_S3_KEY = f"application-logs/vehicle_location_performance_{execution_date}.log"
DEAD_LETTER_BASE_PATH = f"s3://{LOG_S3_BUCKET}/dead_letter/"

# Explicit schemas
vehicles_schema = StructType([
    StructField("active", IntegerType(), True),
    StructField("vehicle_license_number", StringType(), True),
    StructField("registration_name", StringType(), True),
    StructField("license_type", StringType(), True),
    StructField("expiration_date", StringType(), True),
    StructField("permit_license_number", StringType(), True),
    StructField("certification_date", StringType(), True),
    StructField("vehicle_year", IntegerType(), True),
    StructField("base_telephone_number", StringType(), True),
    StructField("base_address", StringType(), True),
    StructField("vehicle_id", StringType(), True),
    StructField("last_update_timestamp", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("vehicle_type", StringType(), True)
])

locations_schema = StructType([
    StructField("location_id", IntegerType(), True),
    StructField("location_name", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zip_code", IntegerType(), True),
    StructField("latitude", FloatType(), True),
    StructField("longitude", FloatType(), True)
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
    return SparkSession.builder.appName("VehicleLocationPerformance").enableHiveSupport().getOrCreate()

def validate_columns(df, required_columns, name):
    for col_name in required_columns:
        if col_name not in df.columns:
            raise ValueError(f"Missing required column '{col_name}' in {name} dataset")

def upload_log_to_s3():
    try:
        s3 = boto3.client("s3")
        s3.upload_file(LOG_FILE, LOG_S3_BUCKET, LOG_S3_KEY)
        print(f"Uploaded log file to s3://{LOG_S3_BUCKET}/{LOG_S3_KEY}")
    except Exception as e:
        print(f"Failed to upload logs to S3: {e}")

def process_vehicle_location_data(spark, raw_vehicles_path, raw_locations_path, raw_rentals_path, output_path):
    logger.info("Starting processing for vehicle and location performance metrics.")

    try:
        # 1. Header-only validation
        vehicle_header = spark.read.csv(raw_vehicles_path, header=True, inferSchema=False, samplingRatio=0.0001)
        rental_header = spark.read.csv(raw_rentals_path, header=True, inferSchema=False, samplingRatio=0.0001)
        location_header = spark.read.csv(raw_locations_path, header=True, inferSchema=False, samplingRatio=0.0001)

        validate_columns(vehicle_header, ["vehicle_id", "vehicle_type"], "vehicles")
        validate_columns(rental_header, ["rental_id", "vehicle_id", "rental_start_time", "rental_end_time", "pickup_location", "dropoff_location", "total_amount"], "rentals")
        validate_columns(location_header, ["location_id", "location_name", "city", "state"], "locations")

        # 2. Load DataFrames with schemas
        df_vehicles = spark.read.schema(vehicles_schema).csv(raw_vehicles_path, header=True)
        df_locations = spark.read.schema(locations_schema).csv(raw_locations_path, header=True)
        df_rentals_raw = spark.read.schema(rentals_schema).csv(raw_rentals_path, header=True)

        # 3. Filter and store dead-letter data
        df_rentals = df_rentals_raw.dropna(subset=["rental_id", "vehicle_id", "pickup_location", "dropoff_location", "rental_start_time", "rental_end_time", "total_amount"])
        df_rentals_raw.subtract(df_rentals).write.mode("overwrite").csv(f"{DEAD_LETTER_BASE_PATH}/rentals/{execution_date}", header=True)

        df_vehicles_valid = df_vehicles.dropna(subset=["vehicle_id", "vehicle_type"])
        df_vehicles.subtract(df_vehicles_valid).write.mode("overwrite").csv(f"{DEAD_LETTER_BASE_PATH}/vehicles/{execution_date}", header=True)

        df_locations_valid = df_locations.dropna(subset=["location_id", "location_name", "city", "state"])
        df_locations.subtract(df_locations_valid).write.mode("overwrite").csv(f"{DEAD_LETTER_BASE_PATH}/locations/{execution_date}", header=True)

        logger.info("Data loaded and validated. Beginning transformations.")

        # 4. Type casting
        df_rentals = df_rentals.withColumn("rental_start_time", col("rental_start_time").cast(TimestampType())) \
                               .withColumn("rental_end_time", col("rental_end_time").cast(TimestampType()))

        # 5. Join rentals with vehicles
        df_joined_vehicles = df_rentals.join(
            df_vehicles.select("vehicle_id", "vehicle_type"),
            on="vehicle_id",
            how="inner"
        )

        # 6. Join with pickup and dropoff location metadata
        df_locations_pickup = df_locations.withColumnRenamed("location_id", "pickup_location_id") \
                                          .withColumnRenamed("location_name", "pickup_location_name") \
                                          .withColumnRenamed("city", "pickup_city") \
                                          .withColumnRenamed("state", "pickup_state")

        df_locations_dropoff = df_locations.withColumnRenamed("location_id", "dropoff_location_id") \
                                           .withColumnRenamed("location_name", "dropoff_location_name") \
                                           .withColumnRenamed("city", "dropoff_city") \
                                           .withColumnRenamed("state", "dropoff_state")

        df_joined_locations = df_joined_vehicles.join(
            df_locations_pickup,
            col("pickup_location") == col("pickup_location_id"),
            how="left"
        ).join(
            df_locations_dropoff,
            col("dropoff_location") == col("dropoff_location_id"),
            how="left"
        )

        # 7. Rental duration
        df_with_duration = df_joined_locations.withColumn(
            "rental_duration_hours",
            (col("rental_end_time").cast("long") - col("rental_start_time").cast("long")) / 3600
        ).withColumn("execution_date", lit(execution_date))

        # 8. Aggregation
        location_performance = df_with_duration.groupBy(
            "pickup_state", "pickup_city", "execution_date",
            col("pickup_location").alias("location_id"),
            col("pickup_location_name").alias("location_name")
        ).agg(
            sum("total_amount").alias("total_revenue_at_location"),
            count("rental_id").alias("total_transactions_at_location"),
            avg("total_amount").alias("avg_transaction_amount_at_location"),
            max("total_amount").alias("max_transaction_amount_at_location"),
            min("total_amount").alias("min_transaction_amount_at_location"),
            countDistinct("vehicle_id").alias("unique_vehicles_rented_at_location"),
            avg("rental_duration_hours").alias("avg_rental_duration_hours_at_location")
        )

        vehicle_type_performance = df_with_duration.groupBy("vehicle_type", "execution_date").agg(
            sum("total_amount").alias("total_revenue_by_vehicle_type"),
            count("rental_id").alias("total_transactions_by_vehicle_type"),
            avg("rental_duration_hours").alias("avg_rental_duration_hours_by_vehicle_type"),
            avg("total_amount").alias("avg_transaction_amount_by_vehicle_type")
        )

        # 9. Write to partitioned S3 paths
        location_output = f"{output_path}/location_performance"
        vehicle_output = f"{output_path}/vehicle_type_performance"

        location_performance.write.mode("overwrite") \
            .partitionBy("execution_date", "pickup_state", "pickup_city") \
            .parquet(location_output)

        vehicle_type_performance.write.mode("overwrite") \
            .partitionBy("execution_date", "vehicle_type") \
            .parquet(vehicle_output)

        logger.info("Metrics written successfully.")

    except Exception as e:
        logger.error(f"Spark job failed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        upload_log_to_s3()

if __name__ == "__main__":
    if len(sys.argv) != 5:
        logger.error("Usage: vehicle_location_performance.py <raw_vehicles_path> <raw_locations_path> <raw_rentals_path> <output_path>")
        sys.exit(1)

    raw_vehicles_path = sys.argv[1]
    raw_locations_path = sys.argv[2]
    raw_rentals_path = sys.argv[3]
    output_path = sys.argv[4]

    spark = create_spark_session()
    process_vehicle_location_data(spark, raw_vehicles_path, raw_locations_path, raw_rentals_path, output_path)
    spark.stop()
    logger.info("SparkSession stopped.")
