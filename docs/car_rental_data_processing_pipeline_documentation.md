Car Rental Data Processing Pipeline Documentation
This document provides a comprehensive overview and detailed steps for the Car Rental Data Processing Pipeline, covering AWS resource setup, data processing with PySpark on EMR Serverless, data cataloging with AWS Glue, querying with Amazon Athena, and automation using AWS Step Functions.

1. Project Overview
Goal: To establish an automated data pipeline that processes raw car rental data (vehicles, locations, rental transactions), transforms it into actionable insights, makes it queryable, and orchestrates the entire workflow using AWS serverless services.

Key AWS Services Used:

Amazon S3: Data storage (raw, scripts, processed, logs, query output).

AWS IAM: Identity and Access Management for secure service interaction.

Amazon EMR Serverless: Scalable and cost-effective Spark job execution.

AWS Glue: Data cataloging (metastore) and schema discovery (crawlers).

Amazon Athena: Interactive SQL querying on data in S3.

AWS Step Functions: Workflow orchestration and automation.

AWS CloudWatch Logs: Centralized logging for debugging and monitoring.

2. AWS Resource Setup
All resources are configured in the eu-west-1 (Ireland) region for AWS Account ID: 371439860588.

2.1. Amazon S3 Bucket Structure
Your primary S3 bucket for this project is s3://lab4-big-data-processing-with-emr.
The following directory structure is used:

s3://lab4-big-data-processing-with-emr/

data/

raw/

vehicles/vehicles.csv

locations/locations.csv

rental_transactions/rental_transactions.csv

users/users.csv

processed/

vehicle_location_performance/ (Output from vehicle_location_performance.py)

user_transaction_analysis/ (Output from user_transaction_analysis.py)

scripts/

vehicle_location_performance.py

user_transaction_analysis.py

emr-serverless-logs/ (Logs from EMR Serverless job runs)

athena-query-output/ (Output from Athena queries initiated by Step Functions)

2.2. AWS IAM Roles
Several IAM roles were created or modified to grant necessary permissions:

EMRServerless_Execution_Role_CarRental

Purpose: This role is assumed by the EMR Serverless application to run your Spark jobs. It grants permissions to read raw data from S3, write processed data to S3, interact with the Glue Data Catalog, and write logs.

Attached Policies:

AmazonS3FullAccess

AWSGlueConsoleFullAccess (or AWSGlueServiceRole)

CloudWatchLogsFullAccess

Trust Policy: Allows emr-serverless.amazonaws.com to assume this role.

ARN: arn:aws:iam::371439860588:role/emr_serverless_exe_role

AWSGlueCrawlerRole-CarRental

Purpose: This role is assumed by AWS Glue Crawlers to read data from S3 and write table definitions to the AWS Glue Data Catalog.

Attached Policies:

AmazonS3FullAccess

AWSGlueServiceRole

Trust Policy: Allows glue.amazonaws.com to assume this role.

StepFunctionsExecutionRole-CarRental

Purpose: This role is assumed by the AWS Step Functions State Machine to orchestrate the workflow (start EMR Serverless jobs, trigger Glue crawlers, run Athena queries).

Attached Policies:

CloudWatchLogsFullAccess (for Step Functions execution logs)

Inline Policy (StepFunctionsEMRServerlessOrchestrationPolicy):

{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "emr-serverless:StartApplication",
        "emr-serverless:StopApplication",
        "emr-serverless:GetApplication"
      ],
      "Resource": "arn:aws:emr-serverless:eu-west-1:371439860588:/applications/00ftgcai6ru5s60p"
    },
    {
      "Effect": "Allow",
      "Action": [
        "emr-serverless:StartJobRun",
        "emr-serverless:GetJobRun",
        "emr-serverless:CancelJobRun",
        "emr-serverless:TagResource",
        "emr-serverless:UntagResource"
      ],
      "Resource": "arn:aws:emr-serverless:eu-west-1:371439860588:/applications/00ftgcai6ru5s60p/jobruns/*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "glue:StartCrawler",
        "glue:GetCrawler"
      ],
      "Resource": [
        "arn:aws:glue:eu-west-1:371439860588:crawler/car-rental-vehicle-location-crawler",
        "arn:aws:glue:eu-west-1:371439860588:crawler/car-rental-user-transaction-crawler"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "athena:StartQueryExecution",
        "athena:GetQueryExecution",
        "athena:GetQueryResults",
        "athena:ListQueryExecutions",
        "athena:GetWorkGroup"
      ],
      "Resource": [
        "arn:aws:athena:eu-west-1:371439860588:workgroup/primary",
        "arn:aws:athena:eu-west-1:371439860588:datacatalog/AwsDataCatalog"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:ListBucket",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:AbortMultipartUpload"
      ],
      "Resource": [
        "arn:aws:s3:::lab4-big-data-processing-with-emr",
        "arn:aws:s3:::lab4-big-data-processing-with-emr/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "arn:aws:logs:eu-west-1:371439860588:log-group:/aws/vendedlogs/states/*"
    }
  ]
}

Trust Policy: Allows states.amazonaws.com to assume this role.

2.3. Amazon EMR Serverless Application
Application Name: (Your chosen name during creation)

Application ID: 

Release: emr-6.x.x (or latest Spark runtime)

Purpose: Provides the runtime environment for your PySpark jobs without managing servers. Jobs are submitted to this application.

2.4. AWS Glue Database and Crawlers
Glue Database: car_rental_db

Purpose: A central metadata repository (Hive Metastore compatible) for defining schemas of your data in S3.

Glue Crawlers:

car-rental-vehicle-location-crawler

S3 Path(s): s3://lab4-big-data-processing-with-emr/data/processed/vehicle_location_performance/

Purpose: Discovers the schema of the processed vehicle and location performance data and creates/updates tables in car_rental_db.

car-rental-user-transaction-crawler

S3 Path(s): s3://lab4-big-data-processing-with-emr/data/processed/user_transaction_analysis/

Purpose: Discovers the schema of the processed user transaction data and creates/updates tables in car_rental_db.

3. PySpark Scripts
Your two PySpark scripts are stored in s3://lab4-big-data-processing-with-emr/scripts/.

3.1. vehicle_location_performance.py
Purpose: Processes raw vehicles.csv, locations.csv, and rental_transactions.csv to derive performance metrics related to vehicle types and rental locations.

Key Transformations:

Type casting for rental_start_time and rental_end_time to TimestampType.

Joining rental data with vehicle types and detailed pickup/dropoff locations.

Calculating rental_duration_hours.

Aggregating data by pickup location to compute: total_revenue_at_location, total_transactions_at_location, avg_transaction_amount_at_location, max_transaction_amount_at_location, min_transaction_amount_at_location, unique_vehicles_rented_at_location, avg_rental_duration_hours_at_location.

Aggregating data by vehicle type to compute: total_revenue_by_vehicle_type, total_transactions_by_vehicle_type, avg_rental_duration_hours_by_vehicle_type, avg_transaction_amount_by_vehicle_type.

Output: Parquet files in s3://lab4-big-data-processing-with-emr/data/processed/vehicle_location_performance/ (overwrites existing data).

3.2. user_transaction_analysis.py
Purpose: Processes raw users.csv and rental_transactions.csv to derive user behavior and overall business performance metrics.

Key Transformations:

Joining rental transactions with user details.

Aggregating data by user to compute: total_rental_transactions, total_spending, average_transaction_value.

Calculating overall business statistics: overall_total_revenue, overall_avg_transaction_value, overall_total_transactions, overall_total_unique_users, overall_total_unique_vehicles_rented.

Identifying top-spending users.

Output: Parquet files in s3://lab4-big-data-processing-with-emr/data/processed/user_transaction_analysis/ (overwrites existing data).

4. Data Processing & ETL Workflow
Raw Data Upload: CSV files are initially uploaded to s3://lab4-big-data-processing-with-emr/data/raw/.

Spark Job Execution: PySpark jobs are submitted to the EMR Serverless application. These jobs read raw data from S3, perform transformations (joins, aggregations, calculations), and write the processed data in optimized Parquet format back to s3://lab4-big-data-processing-with-emr/data/processed/. The overwrite mode ensures fresh data with each run.

Logging: Spark job logs are streamed to s3://lab4-big-data-processing-with-emr/emr-serverless-logs/ and CloudWatch Logs for monitoring and debugging.

5. Data Cataloging with AWS Glue
After data processing, AWS Glue is used to create and manage the metadata (schema) for your processed data.

Glue Database: All processed data tables are registered within the car_rental_db in the AWS Glue Data Catalog.

Crawler Execution: The Glue crawlers (car-rental-vehicle-location-crawler and car-rental-user-transaction-crawler) are run. They scan the Parquet files in their respective S3 paths, infer the schema, and automatically create or update table definitions in car_rental_db.

Expected Tables in car_rental_db:

location_performance

vehicle_type_performance

user_transaction_summary

overall_transaction_statistics

top_users_by_spending

6. Data Querying with Amazon Athena
Once tables are created in the Glue Data Catalog, Amazon Athena can be used to query the processed data using standard SQL.

To query in Athena:

Go to the Amazon Athena Console (https://console.aws.amazon.com/athena/home#/query-editor).

Ensure car_rental_db is selected as your database.

You will see the tables listed in the left panel.

Sample KPI Queries:

Find the highest revenue-generating location:

SELECT
    location_name,
    city,
    state,
    total_revenue_at_location
FROM
    car_rental_db.location_performance
ORDER BY
    total_revenue_at_location DESC
LIMIT 1;

Find the most rented vehicle type:

SELECT
    vehicle_type,
    total_transactions_by_vehicle_type
FROM
    car_rental_db.vehicle_type_performance
ORDER BY
    total_transactions_by_vehicle_type DESC
LIMIT 1;

Identify top-spending users:

SELECT
    first_name,
    last_name,
    email,
    total_spending
FROM
    car_rental_db.top_users_by_spending
ORDER BY
    total_spending DESC
LIMIT 10;

7. Pipeline Automation with AWS Step Functions
AWS Step Functions orchestrates the entire data processing workflow, ensuring steps execute in sequence and handling potential failures.

7.1. State Machine Definition
Your CarRentalDataPipeline Step Functions State Machine is defined by the following Amazon States Language (ASL). This uses EMR Serverless for Spark jobs and manages Glue crawler execution and an Athena query.

State Machine Name: CarRentalDataPipeline
Execution Role: StepFunctionsExecutionRole-CarRental (with StepFunctionsEMRServerlessOrchestrationPolicy attached).

{
  "Comment": "Car Rental Data Pipeline: EMR Serverless Jobs, Glue Crawlers, and Athena Query",
  "StartAt": "RunSparkJobsInParallel",
  "States": {
    "RunSparkJobsInParallel": {
      "Type": "Parallel",
      "Branches": [
        {
          "StartAt": "SubmitVehicleLocationJob",
          "States": {
            "SubmitVehicleLocationJob": {
              "Type": "Task",
              "Resource": "arn:aws:states:::emr-serverless:startJobRun.sync",
              "Parameters": {
                "ApplicationId": "00ftgcai6ru5s60p",
                "ExecutionRoleArn": "arn:aws:iam::371439860588:role/emr_serverless_exe_role",
                "JobDriver": {
                  "SparkSubmit": {
                    "EntryPoint": "s3://lab4-big-data-processing-with-emr/scripts/vehicle_location_performance.py",
                    "EntryPointArguments": [
                      "s3://lab4-big-data-processing-with-emr/data/raw/vehicles/vehicles.csv",
                      "s3://lab4-big-data-processing-with-emr/data/raw/locations/locations.csv",
                      "s3://lab4-big-data-processing-with-emr/data/raw/rental_transactions/rental_transactions.csv",
                      "s3://lab4-big-data-processing-with-emr/data/processed/vehicle_location_performance/"
                    ]
                  }
                },
                "ConfigurationOverrides": {
                  "MonitoringConfiguration": {
                    "S3MonitoringConfiguration": {
                      "LogUri": "s3://lab4-big-data-processing-with-emr/emr-serverless-logs/"
                    },
                    "CloudWatchLoggingConfiguration": {
                      "Enabled": true,
                      "LogGroupName": "/aws/emr-serverless/jobs",
                      "LogStreamNamePrefix": "vehicle-location-performance-job-sf"
                    }
                  }
                }
              },
              "Catch": [
                {
                  "ErrorEquals": [
                    "States.ALL"
                  ],
                  "Next": "FailState",
                  "Comment": "Catch all errors for Vehicle Location Spark job"
                }
              ],
              "End": true
            }
          }
        },
        {
          "StartAt": "SubmitUserTransactionJob",
          "States": {
            "SubmitUserTransactionJob": {
              "Type": "Task",
              "Resource": "arn:aws:states:::emr-serverless:startJobRun.sync",
              "Parameters": {
                "ApplicationId": "00ftgcai6ru5s60p",
                "ExecutionRoleArn": "arn:aws:iam::371439860588:role/emr_serverless_exe_role",
                "JobDriver": {
                  "SparkSubmit": {
                    "EntryPoint": "s3://lab4-big-data-processing-with-emr/scripts/user_transaction_analysis.py",
                    "EntryPointArguments": [
                      "s3://lab4-big-data-processing-with-emr/data/raw/users/users.csv",
                      "s3://lab4-big-data-processing-with-emr/data/raw/rental_transactions/rental_transactions.csv",
                      "s3://lab4-big-data-processing-with-emr/data/processed/user_transaction_analysis/"
                    ]
                  }
                },
                "ConfigurationOverrides": {
                  "MonitoringConfiguration": {
                    "S3MonitoringConfiguration": {
                      "LogUri": "s3://lab4-big-data-processing-with-emr/emr-serverless-logs/"
                    },
                    "CloudWatchLoggingConfiguration": {
                      "Enabled": true,
                      "LogGroupName": "/aws/emr-serverless/jobs",
                      "LogStreamNamePrefix": "user-transaction-analysis-job-sf"
                    }
                  }
                }
              },
              "Catch": [
                {
                  "ErrorEquals": [
                    "States.ALL"
                  ],
                  "Next": "FailState",
                  "Comment": "Catch all errors for User Transaction Spark job"
                }
              ],
              "End": true
            }
          }
        }
      ],
      "Catch": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "Next": "FailState",
          "Comment": "Catch all errors from parallel Spark jobs"
        }
      ],
      "Next": "RunGlueCrawlersInParallel"
    },
    "RunGlueCrawlersInParallel": {
      "Type": "Parallel",
      "Branches": [
        {
          "StartAt": "StartVehicleLocationCrawler",
          "States": {
            "StartVehicleLocationCrawler": {
              "Type": "Task",
              "Resource": "arn:aws:states:::glue:startCrawler.sync",
              "Parameters": {
                "Name": "car-rental-vehicle-location-crawler"
              },
              "Catch": [
                {
                  "ErrorEquals": [
                    "States.ALL"
                  ],
                  "Next": "FailState",
                  "Comment": "Catch all errors for Vehicle Location Crawler"
                }
              ],
              "End": true
            }
          }
        },
        {
          "StartAt": "StartUserTransactionCrawler",
          "States": {
            "StartUserTransactionCrawler": {
              "Type": "Task",
              "Resource": "arn:aws:states:::glue:startCrawler.sync",
              "Parameters": {
                "Name": "car-rental-user-transaction-crawler"
              },
              "Catch": [
                {
                  "ErrorEquals": [
                    "States.ALL"
                  ],
                  "Next": "FailState",
                  "Comment": "Catch all errors for User Transaction Crawler"
                }
              ],
              "End": true
            }
          }
        }
      ],
      "Catch": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "Next": "FailState",
          "Comment": "Catch all errors from parallel Glue crawlers"
        }
      ],
      "Next": "AthenaGenerateSummaryReport"
    },
    "AthenaGenerateSummaryReport": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:athena:startQueryExecution",
      "Parameters": {
        "QueryString": "SELECT * FROM car_rental_db.overall_transaction_statistics LIMIT 10;",
        "QueryExecutionContext": {
          "Database": "car_rental_db"
        },
        "ResultConfiguration": {
          "OutputLocation": "s3://lab4-big-data-processing-with-emr/athena-query-output/"
        }
      },
      "Retry": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "IntervalSeconds": 15,
          "MaxAttempts": 2,
          "BackoffRate": 2
        }
      ],
      "Catch": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "Next": "FailState",
          "Comment": "Catch all errors for Athena query"
        }
      ],
      "Next": "SuccessState"
    },
    "SuccessState": {
      "Type": "Succeed"
    },
    "FailState": {
      "Type": "Fail",
      "Error": "PipelineFailed",
      "Cause": "The EMR Serverless data pipeline failed during execution."
    }
  }
}

7.2. How to Run the Automated Pipeline
Ensure all IAM Roles and Policies are Correct: Double-check that EMRServerless_Execution_Role_CarRental, AWSGlueCrawlerRole-CarRental, and especially StepFunctionsExecutionRole-CarRental have all the necessary permissions as detailed in Section 2.2.

Update State Machine Definition: Go to the AWS Step Functions Console, select your CarRentalDataPipeline state machine, click "Edit", and paste the ASL definition from Section 7.1. Save the changes.

Start an Execution:

In the AWS Step Functions console, select your CarRentalDataPipeline state machine.

Click "Start execution".

You can optionally provide an input JSON (e.g., {}).

Click "Start execution".

Monitor Progress:

The Step Functions console will show a graphical representation of your workflow execution. You can click on each state to view its input, output, and any errors.

For Spark job details, navigate to the Amazon EMR Serverless console -> Applications -> your application ID -> Job runs.

For Glue crawler details, navigate to the AWS Glue console -> Crawlers.

For detailed logs, check AWS CloudWatch Logs.

8. Troubleshooting Common Issues
This section outlines common issues encountered during the project and how to troubleshoot them.

8.1. IAM Permissions Errors (AccessDenied, The security token included in the request is invalid)
Symptom: Jobs fail with AccessDenied errors when trying to read/write S3, or Glue crawlers fail to create tables. Step Functions execution fails with permission errors.

Resolution:

Verify IAM Role Policies: Ensure the correct IAM roles (EMRServerless_Execution_Role_CarRental, AWSGlueCrawlerRole-CarRental, StepFunctionsExecutionRole-CarRental) have all required managed and inline policies attached (e.g., AmazonS3FullAccess, AWSGlueServiceRole, emr-serverless:* actions, glue:* actions, athena:* actions).

Check Trust Relationships: For each role, verify that the Trust relationships policy correctly allows the AWS service (emr-serverless.amazonaws.com, glue.amazonaws.com, states.amazonaws.com) to assume the role.

S3 Bucket Policy: Ensure your S3 bucket s3://lab4-big-data-processing-with-emr does not have any Deny statements in its bucket policy that might override IAM role permissions. An explicit Allow statement for the service principal in the bucket policy might be necessary for cross-account or complex setups.

KMS Encryption: If your S3 bucket uses KMS encryption, ensure the IAM roles have kms:Decrypt permissions on the KMS key.

8.2. Spark Job Failures (TypeError: 'Column' object is not callable, Job Failed)
Symptom: EMR Serverless job run fails with Python errors in logs.

Resolution:

Check Spark Job Logs: Navigate to the EMR Serverless console, click on your application, then on "Job runs". Find the failed job run, and click on "View logs" (this will take you to CloudWatch Logs).

PySpark Syntax Errors: The TypeError: 'Column' object is not callable was resolved by correcting the use of countDistinct (e.g., countDistinct(col("vehicle_id")) instead of count(col("vehicle_id").distinct())).

Input/Output Paths: Ensure the S3 input paths in your Spark scripts and job arguments are correct and contain data. Verify output paths are writable.

8.3. Glue Crawler Failures (No tables created, Resource not recognized)
Symptom: Glue crawlers complete successfully but do not create tables, or they fail with errors. Step Functions validation errors related to Glue resources.

Resolution:

Check Crawler Logs: In the AWS Glue Console, go to "Crawlers", select the crawler, and click the "Log group" link to view CloudWatch logs. Look for messages like "No new objects found", "Failed to infer schema", or AccessDenied.

S3 Path Configuration: Ensure the S3 paths configured for the crawlers are exact and lead directly to the Parquet files (e.g., s3://bucket/data/processed/folder/ with a trailing slash).

Crawler Role Permissions: Confirm AWSGlueCrawlerRole-CarRental has AmazonS3FullAccess and AWSGlueServiceRole.

Step Functions ASL for Glue: If using Step Functions, ensure the Resource ARN for glue:startCrawler is arn:aws:states:::glue:startCrawler.sync as provided in the latest ASL, which uses the optimized integration with built-in waiting. (Note: Avoid aws-sdk:glue:startCrawler or .sync if the plain glue: one works, as availability can vary by region).

8.4. Step Functions ASL Validation Errors (Resource not recognized, Next missing target)
Symptom: Step Functions console displays validation errors when attempting to save the State Machine definition.

Resolution:

Service Integration Syntax: The Resource ARNs for integrating with other services are highly specific.

EMR Serverless: Use arn:aws:states:::emr-serverless:startJobRun.sync.

Glue: Use arn:aws:states:::glue:startCrawler.sync.

Athena: Use arn:aws:states:::aws-sdk:athena:startQueryExecution.

Copy/Paste Integrity: Ensure no invisible characters or formatting issues are introduced during copy-pasting the ASL. A clean copy-paste often resolves mysterious parsing errors.

State Name Consistency: Verify that all Next properties point to state names that are exactly defined in the States block (case-sensitive).

8.5. TerminateEMRCluster: Parameters: The field 'ClusterId' is required but was missing (If using traditional EMR)
Symptom: This error indicates that the CreateEMRCluster step (if you were using traditional EMR) failed to produce a ClusterId.

Resolution: This is a symptom of CreateEMRCluster failing. You would need to check the execution details of the CreateEMRCluster step in Step Functions, specifically its "Error" and "Output" tabs, to find the root cause of the cluster creation failure (e.g., network issues, instance capacity, incorrect JobFlowRole ARN, security group issues).

9. Future Enhancements
Data Visualization: Connect Amazon QuickSight to your car_rental_db in Athena to build interactive dashboards for non-technical users.

Scheduling: Use Amazon EventBridge (CloudWatch Events) to schedule your Step Functions State Machine to run periodically (e.g., daily, weekly).

Notification: Add an SNS topic to the Step Functions workflow to send success/failure notifications.

Cost Optimization: Implement more fine-grained S3 permissions, and consider EMR Serverless managed scaling options if not already using them.

Advanced Error Handling: Incorporate more specific ErrorEquals statements and recovery logic in your Step Functions workflow for different types of failures.

Input Parameterization: Make the Step Function input parameters configurable for different datasets or output locations.
