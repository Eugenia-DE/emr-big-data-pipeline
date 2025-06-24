## Car Rental Data Processing Pipeline
This project implements a fully automated, serverless-first data processing pipeline on AWS for car rental transaction data. It transforms raw CSV data into queryable insights, leveraging modern big data services.

Table of Contents
Project Overview

Features

Architecture

Prerequisites

Setup Guide

5.1. AWS S3 Bucket Setup

5.2. AWS IAM Roles Configuration

5.3. Amazon EMR Serverless Application

5.4. AWS Glue Database and Crawlers

5.5. AWS Step Functions State Machine

How to Run the Pipeline

Output Data and KPIs

Troubleshooting

Future Enhancements

1. Project Overview
This project builds an end-to-end data pipeline to process raw car rental data. It demonstrates best practices for ETL (Extract, Transform, Load) using AWS services to generate key performance indicators (KPIs) and make the data readily available for analysis.

2. Features
Automated ETL: Converts raw CSV data into optimized Parquet format.

Scalable Processing: Utilizes Amazon EMR Serverless for efficient Spark job execution without server management.

Data Cataloging: Integrates with AWS Glue Data Catalog for schema discovery and management.

Ad-hoc Querying: Enables interactive querying of processed data using Amazon Athena.

Workflow Orchestration: Automates the entire pipeline from Spark jobs to Glue crawlers and Athena queries using AWS Step Functions.

KPI Generation: Calculates crucial business metrics like revenue by location, vehicle type performance, and top-spending users.

3. Architecture
The pipeline leverages the following AWS services:

Amazon S3: Centralized storage for raw data, processed data, PySpark scripts, and logs.

AWS IAM: Manages permissions for all service interactions.

Amazon EMR Serverless: Executes the PySpark transformation jobs.

AWS Glue: A fully managed ETL service that creates and manages table metadata in the Glue Data Catalog.

Amazon Athena: A serverless query service that allows running SQL queries directly on data in S3 via the Glue Data Catalog.

AWS Step Functions: Orchestrates the entire workflow, defining the sequence and parallel execution of tasks.

AWS CloudWatch Logs: Collects and monitors logs from EMR Serverless jobs and Step Functions executions.

The data flow within the architecture is as follows:
Raw data is stored in S3. EMR Serverless Spark Jobs process this raw data, and the processed data is then stored back in S3. AWS Glue Crawlers discover the schema of this processed data, registering it in the AWS Glue Data Catalog. Amazon Athena can then query the data using the Glue Data Catalog. The entire process, from running Spark jobs to triggering Glue crawlers and making data available for Athena, is orchestrated by AWS Step Functions. Logs from EMR Serverless and Step Functions are sent to S3 and CloudWatch Logs.

4. Prerequisites
Before deploying this pipeline, ensure you have:

An active AWS Account with credentials that have sufficient permissions to create and manage the AWS services listed in the architecture (S3, IAM, EMR Serverless, Glue, Athena, Step Functions).

5. Setup Guide
Follow these steps to set up the Car Rental Data Processing Pipeline.

5.1. AWS S3 Bucket Setup
Create S3 Bucket: Create an S3 bucket named lab4-big-data-processing-with-emr.

Ensure the bucket is in the eu-west-1 region.

Consider enabling S3 Block Public Access (recommended for security).

Create Folder Structure: Inside s3://lab4-big-data-processing-with-emr/, create the following empty folders:

data/raw/

data/processed/

scripts/

emr-serverless-logs/

athena-query-output/

Upload Raw Data: Upload your raw CSV data files to their respective paths:

vehicles.csv to data/raw/vehicles/

locations.csv to data/raw/locations/

rental_transactions.csv to data/raw/rental_transactions/

users.csv to data/raw/users/

Upload PySpark Scripts: Upload the provided PySpark scripts to s3://lab4-big-data-processing-with-emr/scripts/:

vehicle_location_performance.py

user_transaction_analysis.py

5.2. AWS IAM Roles Configuration
Create or verify the following IAM roles and attach the specified policies.

5.2.1. EMRServerless_Execution_Role_CarRental
Purpose: Role assumed by EMR Serverless for running Spark jobs.

Trusted Entity: emr-serverless.amazonaws.com

Policies to Attach:

AmazonS3FullAccess (Managed Policy)

AWSGlueConsoleFullAccess (Managed Policy - grants access to Glue Data Catalog)

CloudWatchLogsFullAccess (Managed Policy)

5.2.2. AWSGlueCrawlerRole-CarRental
Purpose: Role assumed by AWS Glue Crawlers.

Trusted Entity: glue.amazonaws.com

Policies to Attach:

AmazonS3FullAccess (Managed Policy)

AWSGlueServiceRole (Managed Policy)

5.2.3. StepFunctionsExecutionRole-CarRental
Purpose: Role assumed by AWS Step Functions to orchestrate the pipeline.

Trusted Entity: states.amazonaws.com

Policies to Attach:

CloudWatchLogsFullAccess (Managed Policy)

Inline Policy (StepFunctionsEMRServerlessOrchestrationPolicy):
This policy grants granular permissions to the Step Functions execution role, allowing it to:

Manage the EMR Serverless application (start, stop, get application details for arn:aws:emr-serverless:eu-west-1:371439860588:/applications/00ftgcai6ru5s60p).

Run, get details, cancel, and tag/untag EMR Serverless job runs within the specified application (arn:aws:emr-serverless:eu-west-1:371439860588:/applications/00ftgcai6ru5s60p/jobruns/*).

Start and get details of AWS Glue Crawlers for your two specific crawlers (car-rental-vehicle-location-crawler, car-rental-user-transaction-crawler).

Initiate, get details, and list Athena query executions for the primary workgroup and AwsDataCatalog within your account.

Perform comprehensive S3 operations (get, list, put, delete objects, manage multipart uploads) on your lab4-big-data-processing-with-emr bucket and its contents.

Create log groups/streams and put log events in CloudWatch Logs for Step Functions vended logs.

5.3. Amazon EMR Serverless Application
Create Application:

Navigate to EMR Serverless Console -> Applications.

Click "Create application".

Name: Choose a descriptive name (e.g., CarRentalProcessingApp).

Release: Select a recent emr-6.x.x or emr-7.x.x release (e.g., emr-7.9.0).

Type: Spark.

Leave other settings as default for this project.

Note down your Application ID: 00ftgcai6ru5s60p

5.4. AWS Glue Database and Crawlers
Create Glue Database:

Navigate to AWS Glue Console -> Databases.

Click "Add database".

Database name: car_rental_db.

Create Glue Crawlers:

Navigate to AWS Glue Console -> Crawlers.

Click "Create crawler".

Crawler Name: car-rental-vehicle-location-crawler

Data source: S3 path s3://lab4-big-data-processing-with-emr/data/processed/vehicle_location_performance/

IAM Role: AWSGlueCrawlerRole-CarRental

Output: car_rental_db

Crawler Name: car-rental-user-transaction-crawler

Data source: S3 path s3://lab4-big-data-processing-with-emr/data/processed/user_transaction_analysis/

IAM Role: AWSGlueCrawlerRole-CarRental

Output: car_rental_db

5.5. AWS Step Functions State Machine
Create State Machine:

Navigate to AWS Step Functions Console -> State machines.

Click "Create state machine".

Choose "Write your workflow in Amazon States Language".

Type: Standard.

Definition: The Step Functions workflow is defined to automate the entire data pipeline. It starts by running both Spark jobs in parallel on EMR Serverless. Once both Spark jobs complete successfully, it proceeds to trigger both Glue crawlers in parallel to update the data catalog. Finally, after the crawlers finish, it initiates an Athena query to generate a summary report. The state machine includes comprehensive error handling, directing any failures to a FailState.

The key steps and their parameters are:

RunSparkJobsInParallel: A parallel state with two branches:

SubmitVehicleLocationJob: Submits the vehicle_location_performance.py script to your EMR Serverless application (00ftgcai6ru5s60p) using the emr_serverless_exe_role. It passes arguments for raw vehicle, location, and rental data, and specifies the output path and logging configuration (s3://lab4-big-data-processing-with-emr/emr-serverless-logs/ for S3 logs, and CloudWatch logs). It waits for the job to complete.

SubmitUserTransactionJob: Submits the user_transaction_analysis.py script to the same EMR Serverless application and role. It passes arguments for raw user and rental data, and specifies the output path and logging configuration. It also waits for the job to complete.

RunGlueCrawlersInParallel: A parallel state that executes after the Spark jobs finish, with two branches:

StartVehicleLocationCrawler: Triggers the car-rental-vehicle-location-crawler Glue crawler and waits for it to finish.

StartUserTransactionCrawler: Triggers the car-rental-user-transaction-crawler Glue crawler and waits for it to finish.

AthenaGenerateSummaryReport: After crawlers complete, this task initiates an Athena query.

It runs a query (SELECT * FROM car_rental_db.overall_transaction_statistics LIMIT 10;) against the car_rental_db database.

The query results are stored in s3://lab4-big-data-processing-with-emr/athena-query-output/.

It includes retry logic for transient Athena errors.

SuccessState: A final state indicating successful completion of the entire pipeline.

FailState: A terminal state for handling any errors that occur during the workflow execution.

Name: CarRentalDataPipeline

Permissions: Select "Choose an existing role" and pick StepFunctionsExecutionRole-CarRental.

6. How to Run the Pipeline
Once all AWS resources are configured:

Navigate to AWS Step Functions Console -> State machines.

Select your CarRentalDataPipeline state machine.

Click the "Start execution" button.

You can leave the input JSON as default ({}).

Click "Start execution" to begin the automated pipeline.

Monitor the execution in the Step Functions console for status and detailed step information.

7. Output Data and KPIs
Upon successful execution of the Step Functions pipeline:

Processed Parquet files will be available in s3://lab4-big-data-processing-with-emr/data/processed/.

Table definitions will be updated in the car_rental_db in AWS Glue Data Catalog.

You can query the data using Amazon Athena.

Key Performance Indicator (KPI) Queries in Athena:

Open the Athena console, ensure car_rental_db is selected, and run the following queries:

Highest Revenue-Generating Location: This query identifies the location that has generated the most total revenue from rental transactions.

Most Rented Vehicle Type: This query determines which type of vehicle (e.g., 'basic', 'premium') has been rented the highest number of times.

Top-Spending Users: This query lists the users who have spent the most on car rentals, showing their names, email, and total spending.

8. Troubleshooting
Refer to the comprehensive Car Rental Data Processing Pipeline Documentation for detailed troubleshooting steps for common issues related to IAM permissions, Spark jobs, Glue crawlers, and Step Functions validation.

9. Future Enhancements

Scheduling: Trigger the Step Functions workflow on a schedule using Amazon EventBridge.

Notifications: Implement SNS notifications for pipeline success or failure

More Granular Permissions: Refine IAM policies for least privilege access in a production environment.
