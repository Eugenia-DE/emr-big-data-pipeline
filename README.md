## Project Summary: Car Rental Data Processing Pipeline
This project successfully built and automated a robust, serverless data processing pipeline for car rental data on AWS, designed to transform raw operational data into actionable business insights.

1. Understanding the Business Requirement and Needs  
The core business requirement was to gain deeper insights from car rental operational data, specifically concerning:

- Vehicle Performance: How different vehicle types are performing.
- Location Performance: Which rental locations are most profitable or active.
- User Behavior: Understanding user spending habits and identifying key customers.
- Overall Business Metrics: Getting a high-level view of total revenue, transactions, and user engagement.

The business needed a system that could:

- Process large volumes of data: Rental transactions can grow rapidly.
- Provide timely insights: Data should be processed regularly to inform decision-making.
- Be cost-effective and scalable: Avoid managing fixed infrastructure.
- Be easy to query: Business analysts should be able to access data using standard SQL.
- Automate the workflow: Minimize manual intervention and human error.

2. Solution Approach and System Design to Meet Business Needs  
To address these needs, a serverless-first architecture was chosen, leveraging AWS services for their scalability, managed nature, and cost-efficiency.

## Key Design Principles:

- Serverless: Minimizing operational overhead and scaling automatically with demand.
- Data Lake First: Storing raw and processed data in S3 for flexibility and cost-effectiveness.
- Schema-on-Read: Using Glue Data Catalog to infer schemas as needed, supporting evolving data.
- Orchestration: Automating the entire workflow for reliability and efficiency.

3. Steps Taken to Build the System

### AWS S3 Data Lake Setup:
**Action**: Configured an S3 bucket (`s3://lab4-big-data-processing-with-emr`) with a structured folder system for raw data, processed data, PySpark scripts, and logs.  
**Alignment**: This established a scalable and cost-effective central repository for all data, a fundamental component of any modern data platform.

### IAM Roles Configuration:
**Action**: Created and configured specific IAM roles (`EMRServerless_Execution_Role_CarRental`, `AWSGlueCrawlerRole-CarRental`, `StepFunctionsExecutionRole-CarRental`) with precise permissions for each AWS service interaction.  
**Alignment**: Ensured secure access control and adherence to the principle of least privilege, crucial for enterprise environments.

### PySpark ETL Script Development:
**Action**: Developed two PySpark scripts:
- `vehicle_location_performance.py`: Joined `vehicles.csv`, `locations.csv`, and `rental_transactions.csv` to calculate revenue, transaction counts, and rental durations per location and vehicle type.
- `user_transaction_analysis.py`: Joined `users.csv` and `rental_transactions.csv` to compute total spending, average transaction values per user, and overall business metrics, including identifying top-spending users.

**Alignment**: Directly addressed the business's need for specific KPIs by transforming raw data into meaningful insights. The use of PySpark on Spark ensured scalability for large datasets.

### Amazon EMR Serverless Application Setup:
**Action**: Provisioned an EMR Serverless application (`00ftgcai6ru5s60p`) as the runtime environment for the PySpark jobs.  
**Alignment**: Provided a fully managed, scalable Spark environment, eliminating the overhead of cluster management and allowing the business to focus on data processing logic rather than infrastructure.

### AWS Glue Data Catalog and Crawlers:
**Action**: Created an AWS Glue Database (`car_rental_db`) and two Glue Crawlers (`car-rental-vehicle-location-crawler`, `car-rental-user-transaction-crawler`). These crawlers were configured to scan the processed Parquet data in S3 and automatically infer schemas, creating tables in the `car_rental_db`.  
**Alignment**: Enabled self-service data access for analysts via Athena. By cataloging the data, it made the complex Parquet files accessible as standard SQL tables, directly addressing the need for easy querying.

### AWS Step Functions Workflow Orchestration:
**Action**: Designed and implemented an AWS Step Functions State Machine (`CarRentalDataPipeline`). This workflow orchestrates the entire pipeline:
- Running both PySpark jobs in parallel on EMR Serverless.
- Triggering both Glue crawlers in parallel upon successful Spark job completion.
- Executing Athena queries to validate and summarize data post-cataloging.
- Catching and handling failures with retries and notifications.

**Alignment**: Fulfilled the crucial business need for automation. This ensures the pipeline runs reliably and efficiently without manual intervention, guarantees data freshness, and minimizes operational costs associated with manual oversight.

### Data Querying with Amazon Athena:
**Action**: Demonstrated how to query the processed data using Amazon Athena with SQL, leveraging the tables defined in the Glue Data Catalog.  
**Alignment**: Directly met the requirement for analysts to easily access and query the derived KPIs using familiar SQL, empowering data-driven decision-making.

---

## Failure Handling Strategy
The pipeline was built with robust failure detection and response mechanisms to ensure observability and resilience:

- **Retry Logic**: All critical Spark, Glue, and Athena tasks include automatic retry policies with exponential backoff. This guards against transient failures such as API throttling or short-lived service issues.
- **Catch Blocks**: Each parallel processing branch in Step Functions uses `Catch` clauses to detect job failures and reroute execution.
- **Failure Isolation**: Each failure is isolated to its specific job (e.g., `VehicleLocationJobFailed`, `UserTransactionJobFailed`) to prevent cascading effects.
- **SNS Notifications**: On any failure, a message is sent to an Amazon SNS topic (`carrental-pipeline-alerts`) to notify stakeholders immediately of which job failed and why.
- **Final Catch and Notification**: The entire workflow has a final `Catch` block to catch any unhandled failure in Athena queries and send a terminal alert before the workflow fails.

This strategy ensures that issues are identified early, logged clearly, and acted upon without impacting the entire pipeline unnecessarily.

---

## Data Quality Strategy
Several design decisions were implemented to validate the quality of data after it is processed:

- **Glue Crawler Schema Validation**: Glue Crawlers were configured to scan processed output folders and register schemas in the Data Catalog. If the schema is invalid or files are unreadable, the crawler fails, serving as an early signal of data issues.
- **Athena Query Execution**: Following cataloging, Athena queries are executed to confirm that the resulting tables are queryable, non-empty, and contain expected structure and content.
- **Folder Structure and Path Control**: All output paths are well-defined and isolated to prevent accidental overwrites or miswrites of data between runs.
- **Failure-Based Halting**: If either schema inference or query validation fails, the pipeline does not proceed silently; it halts, sends alerts, and prevents propagation of bad data.
- **Manual Inspection Readiness**: Athena tables give analysts immediate access to review row counts, check schema consistency, and spot anomalies, enabling faster debugging and issue resolution.

Together, these mechanisms ensure that each pipeline run ends with valid, accessible, and trusted data ready for downstream reporting.

---

## Conclusion
By leveraging AWS's serverless and managed services, this project successfully transformed raw car rental data into an efficient automated data processing pipeline. It provides a scalable, cost-effective, and easy-to-use solution that directly addresses the business's need for comprehensive insights into vehicle, location, and user performance, setting a strong foundation for advanced analytics and reporting.
