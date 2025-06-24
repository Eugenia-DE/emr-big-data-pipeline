import sys
from pyspark.sql import SparkSession
# Added countDistinct to the import list
from pyspark.sql.functions import col, sum, count, avg, max, min, datediff, hour, minute, second, expr, countDistinct
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
        .appName("VehicleLocationPerformance") \
        .enableHiveSupport() \
        .getOrCreate()
    return spark

def process_vehicle_location_data(spark, raw_vehicles_path, raw_locations_path, raw_rentals_path, output_path):
    """
    Processes vehicle, location, and rental transaction data to derive performance metrics.

    Args:
        spark (SparkSession): The SparkSession object.
        raw_vehicles_path (str): S3 path to the raw vehicles dataset.
        raw_locations_path (str): S3 path to the raw locations dataset.
        raw_rentals_path (str): S3 path to the raw rental transactions dataset.
        output_path (str): S3 path to save the processed data.
    """
    logger.info(f"Starting processing for vehicle and location performance metrics.")
    logger.info(f"Reading vehicles from: {raw_vehicles_path}")
    logger.info(f"Reading locations from: {raw_locations_path}")
    logger.info(f"Reading rental transactions from: {raw_rentals_path}")

    try:
        # 1. Load DataFrames
        df_vehicles = spark.read.csv(raw_vehicles_path, header=True, inferSchema=True)
        df_locations = spark.read.csv(raw_locations_path, header=True, inferSchema=True)
        df_rentals = spark.read.csv(raw_rentals_path, header=True, inferSchema=True)

        logger.info("Raw DataFrames loaded successfully.")
        # printSchema() returns None, so it shouldn't be inside an f-string
        df_vehicles.printSchema()
        df_locations.printSchema()
        df_rentals.printSchema()

        # 2. Type Casting for Date/Time Columns in Rentals
        df_rentals = df_rentals \
            .withColumn("rental_start_time", col("rental_start_time").cast(TimestampType())) \
            .withColumn("rental_end_time", col("rental_end_time").cast(TimestampType()))

        # 3. Join Rental Transactions with Vehicles
        df_joined_vehicles = df_rentals.join(
            df_vehicles.select("vehicle_id", "vehicle_type"),
            on="vehicle_id",
            how="inner"
        )
        logger.info("Joined rentals with vehicles.")

        # 4. Join with Locations (for pickup and dropoff details)
        # Rename location columns to avoid ambiguity after joining
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
        logger.info("Joined rentals with pickup and dropoff locations.")

        # 5. Calculate Rental Duration in Hours
        # duration in seconds / 3600 to get hours
        df_with_duration = df_joined_locations.withColumn(
            "rental_duration_hours",
            (col("rental_end_time").cast("long") - col("rental_start_time").cast("long")) / 3600
        )
        logger.info("Calculated rental duration in hours.")

        # 6. Aggregate by Pickup Location for Performance Metrics
        location_performance = df_with_duration.groupBy(
            col("pickup_location").alias("location_id"),
            col("pickup_location_name").alias("location_name"),
            col("pickup_city").alias("city"),
            col("pickup_state").alias("state")
        ).agg(
            sum("total_amount").alias("total_revenue_at_location"),
            count("rental_id").alias("total_transactions_at_location"),
            avg("total_amount").alias("avg_transaction_amount_at_location"),
            max("total_amount").alias("max_transaction_amount_at_location"),
            min("total_amount").alias("min_transaction_amount_at_location"),
            # CORRECTED LINE: Using countDistinct function
            countDistinct(col("vehicle_id")).alias("unique_vehicles_rented_at_location"),
            avg("rental_duration_hours").alias("avg_rental_duration_hours_at_location")
        )
        logger.info("Aggregated performance metrics by pickup location.")

        # 7. Aggregate by Vehicle Type for Performance Metrics
        vehicle_type_performance = df_with_duration.groupBy("vehicle_type").agg(
            sum("total_amount").alias("total_revenue_by_vehicle_type"),
            count("rental_id").alias("total_transactions_by_vehicle_type"),
            avg("rental_duration_hours").alias("avg_rental_duration_hours_by_vehicle_type"),
            avg("total_amount").alias("avg_transaction_amount_by_vehicle_type")
        )
        logger.info("Aggregated performance metrics by vehicle type.")

        # 8. Write Results to S3 in Parquet format
        # Create separate output paths for each aggregated result
        output_location_path = f"{output_path}/location_performance"
        output_vehicle_type_path = f"{output_path}/vehicle_type_performance"

        logger.info(f"Writing location performance data to: {output_location_path}")
        location_performance.write.mode("overwrite").parquet(output_location_path)
        logger.info(f"Writing vehicle type performance data to: {output_vehicle_type_path}")
        vehicle_type_performance.write.mode("overwrite").parquet(output_vehicle_type_path)

        logger.info("Processed data written to S3 successfully.")

    except Exception as e:
        logger.error(f"Error during Spark job execution: {e}", exc_info=True)
        sys.exit(1) # Exit with a non-zero code to indicate failure

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