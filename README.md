# Lakehouse Architecture for E-Commerce Transactions

## Overview

The Lakehouse_Architecture project delivers a robust Lakehouse solution for e-commerce transaction processing, leveraging AWS Glue, Lambda, Step Functions, and Delta Lake. It ensures efficient data ingestion, rigorous validation, and real-time analytics for `order_items`, `orders`, and `products`, embodying a scalable framework for data-driven commerce.
![Architecture Diagram](./images/architecture3.svg)
## Directory Structure

- `Data/`: Stores raw data files (`order_items`, `orders`, `products`).
- `glue_jobs/`: Hosts AWS Glue ETL scripts:
  - `order-items-glue-job.py`: Orchestrates order item processing with reference data validation and Delta table updates.
  - `orders-glue-job.py`: Manages order data with multi-mode execution, advanced validation, and CloudWatch metrics.
  - `products-glue-job.py`: Handles product data with schema enforcement, deduplication, and catalog synchronization.
- `images/`: Visual assets:
  - `athena_output.png`: Athena query results.
  - `sns_notification.png`: SNS success notification.
  - `step_function.png`: Step Function workflow diagram.
- `lambda-string-replace.py`: Lambda function for string manipulation.
- `lambda-trigger.py`: S3-event-driven Lambda for ETL initiation.
- `stepfunction.json`: Workflow orchestration configuration.
- `tests/`: Contains test files:
  - `__init__.py`: Initialization file for the test package.
  - `conftest.py`: Configuration for pytest fixtures.
  - `test_glue_etl.py`: Integration tests for Glue ETL processes with PySpark.

## Features

- **Data Ingestion**: Processes CSV/Excel from S3 with column validation.
- **Data Quality**: Enforces validation, quarantines invalid records, and cleans data.
- **Delta Lake**: Uses ACID-compliant tables, partitioned by date, with Z-order optimization.
- **Workflow Automation**: Step Functions manage end-to-end execution with S3 consistency.
- **Analytics**: Runs Athena queries post-ETL.
- **Notifications**: Sends SNS alerts for success, failures, and analytics issues.
- **Monitoring**: Logs metrics via CloudWatch (e.g., record counts, validation rates).

## Requirements

- AWS services: S3, Glue, Lambda, Step Functions, SNS, Athena, IAM.
- Dependencies: Spark, Delta Lake (Glue-integrated), Boto3, Python 3.x.
- Infrastructure: S3 buckets for raw, processed, archived, and rejected data.

## Installation & Setup

1. **Clone Repository**:

   - `git clone https://github.com/Courage-codes/Lakehouse_Architecture.git`
   - `git checkout dev` to use the development branch.

2. **Configure Environment**:

   - Set `lambda-trigger.py` environment variables:
     - `BUCKET_NAME`: S3 data bucket.
     - `RAW_PREFIX`: Raw data prefix (default: `raw/`).
     - `RAW_ZONE_PREFIX`: Raw zone prefix (default: `raw-zone/`).
     - `SUBFOLDERS`: Subfolders (default: `products,orders,order-items`).
     - `STEP_FUNCTION_ARN`: ARN (e.g., `arn:aws:states:REGION:ACCOUNT_ID:stateMachine:NAME`).

3. **Deploy Lambda Functions**:

   - Deploy `lambda-trigger.py` as an S3 event trigger.
   - Deploy `lambda-string-replace.py` as a workflow utility.

4. **Configure Step Function**:

   - Upload `stepfunction.json` to AWS Step Functions.
   - Verify `STEP_FUNCTION_ARN` matches the state machine ARN.

5. **Set Up Glue Jobs**:

   - Configure in AWS Glue console:
     - `raw_bucket`: Raw data S3 bucket.
     - `processed_bucket`: Processed data S3 bucket.
     - `database_name`: Glue database (e.g., `ecommerce_lakehouse`).
     - `table_name`: Target table.

6. **Enable Notifications**:

   - Create SNS topic with ARN `arn:aws:sns:REGION:ACCOUNT_ID:TOPIC_NAME`.

7. **Test Deployment**:

   - Upload sample files to S3 raw zone (e.g., `s3://<bucket>/raw/products/`).
   - Monitor via CloudWatch logs and SNS notifications.
   - Last tested on: 04:11 AM GMT, Wednesday, July 02, 2025.

## Usage

- Trigger workflows by uploading files to the S3 raw zone.
- Step Functions execute Glue jobs, Athena queries, and send SNS updates.
- Review logs and metrics in CloudWatch.

## GitHub Actions

The project employs GitHub Actions for CI/CD to automate testing and deployment. It includes a workflow that triggers on pushes and merges to the `main` branch. The process involves running PySpark integration tests on a compatible environment, followed by deploying Glue scripts to AWS S3, but only if tests pass and on direct pushes to `main`. AWS credentials and configuration secrets are required for deployment.

## Contributing

1. **Fork the Repository**: Create your own copy.
2. **Branching**: Use feature branches (e.g., `git checkout -b feature-name`).
3. **Commit Guidelines**: Follow Conventional Commits (e.g., `feat: add new feature`).
4. **Pull Requests**: Submit PRs with clear descriptions and tests.
5. **Code Standards**: Adhere to PEP 8 and include unit tests.
6. **Review Process**: Expect feedback and iterate as needed.
