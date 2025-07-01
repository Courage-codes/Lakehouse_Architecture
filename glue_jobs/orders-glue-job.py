import sys
import boto3
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class OrdersETL:
    def __init__(self, spark, glue_context, job_name, raw_bucket, processed_bucket, database_name, table_name, mode="full"):
        self.spark = spark
        self.glue_context = glue_context
        self.job_name = job_name
        self.mode = mode  # full, validate_only, upsert_only
        
        # Configuration
        self.raw_bucket = raw_bucket
        self.processed_bucket = processed_bucket
        self.database_name = database_name
        self.table_name = table_name
        
        # Paths aligned with provided S3 structure
        self.raw_path = f"s3://{self.raw_bucket}/raw-zone/orders/"
        self.processed_path = f"s3://{self.processed_bucket}/processed-zone/orders_delta/"
        self.quarantine_path = f"s3://{self.processed_bucket}/rejected/orders/"
        
        # Schema definition - enforced during read
        self.schema = StructType([
            StructField("order_num", StringType(), True),  # Read as string first for validation
            StructField("order_id", StringType(), False),
            StructField("user_id", StringType(), False),
            StructField("order_timestamp", StringType(), True),  # Read as string for flexible parsing
            StructField("total_amount", StringType(), True),  # Read as string for validation
            StructField("date", StringType(), True)  # Read as string for validation
        ])
        
        # Target schema after validation
        self.target_schema = StructType([
            StructField("order_num", LongType(), False),
            StructField("order_id", StringType(), False),
            StructField("user_id", StringType(), False),
            StructField("order_timestamp", TimestampType(), False),
            StructField("total_amount", DecimalType(10,2), False),
            StructField("date", DateType(), False),
            StructField("ingestion_timestamp", TimestampType(), False),
            StructField("source_file", StringType(), False)
        ])
        
        # Initialize CloudWatch client for metrics
        self.cloudwatch = boto3.client('cloudwatch')
        self.s3_client = boto3.client('s3')
        
    def read_raw_data(self, file_path):
        """Read CSV data from S3 with enforced schema"""
        try:
            logger.info(f"Reading raw data from: {file_path}")
            
            # Read CSV with explicit schema for better error handling
            df_raw = self.spark.read.format("csv") \
                .option("header", "true") \
                .option("timestampFormat", "yyyy-MM-dd HH:mm:ss") \
                .option("dateFormat", "yyyy-MM-dd") \
                .option("mode", "PERMISSIVE") \
                .option("columnNameOfCorruptRecord", "_corrupt_record") \
                .schema(self.schema) \
                .load(file_path)
            
            # Add _corrupt_record column if it doesn't exist
            if "_corrupt_record" not in df_raw.columns:
                df_raw = df_raw.withColumn("_corrupt_record", lit(None).cast(StringType()))
            
            # Check for corrupt records
            corrupt_count = df_raw.filter(col("_corrupt_record").isNotNull()).count()
            if corrupt_count > 0:
                logger.warning(f"Found {corrupt_count} corrupt records")
                self.publish_metric("CorruptRecords", corrupt_count)
            
            total_count = df_raw.count()
            logger.info(f"Raw data read successfully. Row count: {total_count}")
            self.publish_metric("RawRecordsRead", total_count)
            
            return df_raw
            
        except Exception as e:
            logger.error(f"Error reading raw data: {str(e)}")
            self.publish_metric("ReadErrors", 1)
            raise

    def validate_and_clean_data(self, df_raw, source_file):
        """Comprehensive data validation and cleaning with improved error handling"""
        logger.info("Starting data validation and cleaning")
        
        # Add metadata columns
        df_with_metadata = df_raw.withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("source_file", lit(source_file.split('/')[-1] if '/' in source_file else source_file))
        
        # Enhanced data type conversions with validation
        df_typed = df_with_metadata.withColumn(
            "order_num_clean", 
            when(col("order_num").rlike("^[0-9]+$"), col("order_num").cast(LongType()))
            .otherwise(None)
        ).withColumn(
            "total_amount_clean",
            when(col("total_amount").rlike("^[0-9]+\\.?[0-9]*$"), 
                 col("total_amount").cast(DecimalType(10,2)))
            .otherwise(None)
        ).withColumn(
            "order_timestamp_clean",
            coalesce(
                to_timestamp(col("order_timestamp"), "yyyy-MM-dd HH:mm:ss"),
                to_timestamp(col("order_timestamp"), "yyyy-MM-dd'T'HH:mm:ss"),
                to_timestamp(col("order_timestamp"), "MM/dd/yyyy HH:mm:ss")
            )
        ).withColumn(
            "date_clean",
            coalesce(
                to_date(col("date"), "yyyy-MM-dd"),
                to_date(col("date"), "MM/dd/yyyy"),
                to_date(col("date"), "dd/MM/yyyy")
            )
        )
        
        # FIXED: Break down complex validation into separate steps to avoid Py4JError
        
        # Step 1: Basic null and corruption checks
        step1_filter = df_typed.filter(
            (col("_corrupt_record").isNull()) &
            (col("order_num_clean").isNotNull()) &
            (col("order_num_clean") > 0)
        )
        
        # Step 2: Order ID validation
        step2_filter = step1_filter.filter(
            (col("order_id").isNotNull()) &
            (length(trim(col("order_id"))) >= 3) &
            (length(trim(col("order_id"))) <= 50) &
            (col("order_id").rlike("^[A-Za-z0-9_-]+$"))
        )
        
        # Step 3: User ID validation
        step3_filter = step2_filter.filter(
            (col("user_id").isNotNull()) &
            (length(trim(col("user_id"))) >= 1) &
            (length(trim(col("user_id"))) <= 100)
        )
        
        # Step 4: Timestamp validation
        step4_filter = step3_filter.filter(
            (col("order_timestamp_clean").isNotNull()) &
            (col("order_timestamp_clean") >= to_timestamp(lit("2020-01-01 00:00:00"))) &
            (col("order_timestamp_clean") <= current_timestamp())
        )
        
        # Step 5: Amount validation
        step5_filter = step4_filter.filter(
            (col("total_amount_clean").isNotNull()) &
            (col("total_amount_clean") > 0) &
            (col("total_amount_clean") <= 100000.00)
        )
        
        # Step 6: Date validation
        valid_records = step5_filter.filter(
            (col("date_clean").isNotNull()) &
            (col("date_clean") >= to_date(lit("2020-01-01"))) &
            (col("date_clean") <= current_date())
        )
        
        # Get invalid records for quarantine using exceptAll
        try:
            invalid_records = df_typed.exceptAll(valid_records)
        except Exception as e:
            logger.warning(f"exceptAll failed, using alternative method: {str(e)}")
            # Alternative method if exceptAll fails
            valid_order_ids = valid_records.select("order_id").distinct()
            invalid_records = df_typed.join(valid_order_ids, ["order_id"], "left_anti")
        
        # Log validation results and publish metrics
        total_records = df_typed.count()
        valid_count = valid_records.count()
        invalid_count = invalid_records.count()
        
        logger.info(f"Data validation completed:")
        logger.info(f"  Total records: {total_records}")
        logger.info(f"  Valid records: {valid_count}")
        logger.info(f"  Invalid records: {invalid_count}")
        
        # Publish metrics
        self.publish_metric("ValidRecords", valid_count)
        self.publish_metric("InvalidRecords", invalid_count)
        self.publish_metric("ValidationSuccessRate", (valid_count / total_records * 100) if total_records > 0 else 0)
        
        # Quarantine invalid records if any
        if invalid_count > 0:
            self.quarantine_invalid_records(invalid_records, source_file)
        
        # Stop processing if validation-only mode
        if self.mode == "validate_only":
            logger.info("Validation-only mode: stopping after validation")
            return valid_records.limit(0)  # Return empty dataframe
        
        # Apply final cleaning and select required columns
        cleaned_df = valid_records.select(
            col("order_num_clean").alias("order_num"),
            trim(upper(col("order_id"))).alias("order_id"),
            trim(col("user_id")).alias("user_id"),
            col("order_timestamp_clean").alias("order_timestamp"),
            col("total_amount_clean").alias("total_amount"),
            col("date_clean").alias("date"),
            col("ingestion_timestamp"),
            col("source_file")
        )
        
        return cleaned_df
        
    def list_raw_files(self):
        """List all CSV files in the raw zone"""
        try:
            logger.info(f"Listing files in raw zone: {self.raw_path}")
            
            # Extract bucket and prefix from S3 path
            bucket_parts = self.raw_path.replace('s3://', '').split('/', 1)
            bucket_name = bucket_parts[0]
            prefix = bucket_parts[1] if len(bucket_parts) > 1 else ""
            
            response = self.s3_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix=prefix
            )
            
            files = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    # Only include CSV files and exclude directory markers
                    if obj['Key'].endswith('.csv') and obj['Size'] > 0:
                        files.append(f"s3://{bucket_name}/{obj['Key']}")
            
            logger.info(f"Found {len(files)} CSV files to process")
            return files
            
        except Exception as e:
            logger.error(f"Error listing raw files: {str(e)}")
            return []
    
    def move_file_to_archive(self, file_path):
        """Move processed file from raw zone to archive zone"""
        try:
            # Extract bucket and key from S3 path
            source_parts = file_path.replace('s3://', '').split('/', 1)
            bucket_name = source_parts[0]
            source_key = source_parts[1]
            
            # Create archive key
            file_name = source_key.split('/')[-1]
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            archive_key = f"archived/orders/{timestamp}_{file_name}"
            
            # Copy file to archive
            copy_source = {'Bucket': bucket_name, 'Key': source_key}
            self.s3_client.copy_object(
                CopySource=copy_source,
                Bucket=bucket_name,
                Key=archive_key
            )
            
            # Delete original file
            self.s3_client.delete_object(Bucket=bucket_name, Key=source_key)
            
            logger.info(f"File moved from {file_path} to s3://{bucket_name}/{archive_key}")
            return f"s3://{bucket_name}/{archive_key}"
            
        except Exception as e:
            logger.error(f"Error moving file to archive: {str(e)}")
            raise
    
    def move_file_to_rejected(self, file_path, error_reason):
        """Move failed file from raw zone to rejected zone"""
        try:
            # Extract bucket and key from S3 path
            source_parts = file_path.replace('s3://', '').split('/', 1)
            bucket_name = source_parts[0]
            source_key = source_parts[1]
            
            # Create rejected key with error info
            file_name = source_key.split('/')[-1]
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            rejected_key = f"rejected/orders/{timestamp}_FAILED_{file_name}"
            
            # Copy file to rejected zone
            copy_source = {'Bucket': bucket_name, 'Key': source_key}
            self.s3_client.copy_object(
                CopySource=copy_source,
                Bucket=bucket_name,
                Key=rejected_key
            )
            
            # Create error log file
            error_log_key = f"rejected/orders/{timestamp}_FAILED_{file_name}.error_log"
            error_content = f"Processing failed at: {datetime.now(timezone.utc)}\nError: {error_reason}\nOriginal file: {file_path}\n"
            
            self.s3_client.put_object(
                Bucket=bucket_name,
                Key=error_log_key,
                Body=error_content.encode('utf-8'),
                ContentType='text/plain'
            )
            
            # Delete original file
            self.s3_client.delete_object(Bucket=bucket_name, Key=source_key)
            
            logger.info(f"File moved from {file_path} to s3://{bucket_name}/{rejected_key}")
            return f"s3://{bucket_name}/{rejected_key}"
            
        except Exception as e:
            logger.error(f"Error moving file to rejected zone: {str(e)}")
            raise

    def quarantine_invalid_records(self, invalid_df, source_file):
        """Save invalid records with detailed rejection reasons"""
        try:
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            file_name = source_file.split('/')[-1] if '/' in source_file else source_file
            quarantine_file_path = f"{self.quarantine_path}{timestamp}_{file_name}"
            
            # Add comprehensive rejection reasons
            invalid_with_reasons = invalid_df.withColumn(
                "rejection_reason",
                when(col("_corrupt_record").isNotNull(), "Corrupt record format")
                .when(col("order_num").isNull() | ~col("order_num").rlike("^[0-9]+$"), 
                     "Invalid or missing order_num")
                .when(col("order_id").isNull() | (length(trim(col("order_id"))) < 3), 
                      "Invalid or missing order_id")
                .when(col("user_id").isNull() | (length(trim(col("user_id"))) == 0), 
                      "Missing user_id")
                .when(col("order_timestamp_clean").isNull(), 
                      "Invalid order_timestamp format")
                .when(col("total_amount_clean").isNull() | (col("total_amount_clean") <= 0), 
                      "Invalid total_amount")
                .when(col("date_clean").isNull(), 
                      "Invalid date format")
                .when(col("total_amount_clean") > 100000.00, 
                      "Amount exceeds maximum limit")
                .otherwise("Multiple validation failures")
            ).withColumn("quarantine_timestamp", current_timestamp()) \
             .withColumn("job_name", lit(self.job_name))
            
            # Write to quarantine with single file
            invalid_with_reasons.coalesce(1).write.mode("overwrite") \
                .option("header", "true") \
                .csv(quarantine_file_path)
            
            logger.info(f"Invalid records quarantined to: {quarantine_file_path}")
            
        except Exception as e:
            logger.error(f"Error quarantining invalid records: {str(e)}")
            self.publish_metric("QuarantineErrors", 1)
    
    def upsert_to_delta_table(self, df_cleaned):
        """Enhanced upsert with retry logic and performance optimization"""
        try:
            logger.info("Starting Delta table upsert operation")
            
            if df_cleaned.count() == 0:
                logger.info("No records to upsert")
                return
            
            # Deduplicate within current batch
            window_spec = Window.partitionBy("order_id").orderBy(col("ingestion_timestamp").desc())
            df_deduped = df_cleaned.withColumn("row_num", row_number().over(window_spec)) \
                .filter(col("row_num") == 1) \
                .drop("row_num")
            
            # Repartition by date for better write performance
            df_deduped = df_deduped.repartition("date")
            
            logger.info(f"Records after deduplication: {df_deduped.count()}")
            
            # Retry logic for Delta operations
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    if DeltaTable.isDeltaTable(self.spark, self.processed_path):
                        logger.info(f"Delta table exists, performing merge operation (attempt {attempt + 1})")
                        
                        delta_table = DeltaTable.forPath(self.spark, self.processed_path)
                        
                        # Enhanced merge with conflict resolution
                        merge_condition = "target.order_id = source.order_id"
                        
                        delta_table.alias("target").merge(
                            df_deduped.alias("source"),
                            merge_condition
                        ).whenMatchedUpdate(
                            condition="source.ingestion_timestamp > target.ingestion_timestamp",
                            set={
                                "order_num": "source.order_num",
                                "user_id": "source.user_id",
                                "order_timestamp": "source.order_timestamp",
                                "total_amount": "source.total_amount",
                                "date": "source.date",
                                "ingestion_timestamp": "source.ingestion_timestamp",
                                "source_file": "source.source_file"
                            }
                        ).whenNotMatchedInsertAll().execute()
                        
                        logger.info("Merge operation completed successfully")
                        
                    else:
                        logger.info(f"Creating new partitioned Delta table (attempt {attempt + 1})")
                        
                        df_deduped.write.format("delta") \
                            .mode("overwrite") \
                            .partitionBy("date") \
                            .option("mergeSchema", "true") \
                            .option("delta.autoOptimize.optimizeWrite", "true") \
                            .option("delta.autoOptimize.autoCompact", "true") \
                            .option("delta.deletedFileRetentionDuration", "interval 7 days") \
                            .save(self.processed_path)
                        
                        logger.info("New partitioned Delta table created successfully")
                    
                    # If we get here, operation succeeded
                    self.publish_metric("UpsertSuccess", 1)
                    break
                    
                except Exception as e:
                    if attempt == max_retries - 1:
                        raise e
                    logger.warning(f"Attempt {attempt + 1} failed, retrying: {str(e)}")
                    import time
                    time.sleep(2 ** attempt)  # Exponential backoff
            
            # Optimize Delta table
            self.optimize_delta_table(df_deduped)
            
        except Exception as e:
            logger.error(f"Error in Delta table upsert: {str(e)}")
            self.publish_metric("UpsertErrors", 1)
            raise
    
    def optimize_delta_table(self, df_processed):
        """Enhanced Delta table optimization"""
        try:
            logger.info("Starting Delta table optimization")
            
            # Get unique partition values
            partitions = df_processed.select("date").distinct().collect()
            
            for row in partitions:
                partition_date = row['date']
                logger.info(f"Optimizing partition: date={partition_date}")
                
                # Optimize with Z-ordering
                self.spark.sql(f"""
                    OPTIMIZE delta.`{self.processed_path}`
                    WHERE date = '{partition_date}'
                    ZORDER BY (user_id, order_timestamp)
                """)
            
            # Vacuum old files (configurable retention)
            vacuum_hours = 168  # 7 days
            self.spark.sql(f"""
                VACUUM delta.`{self.processed_path}` RETAIN {vacuum_hours} HOURS
            """)
            
            logger.info("Delta table optimization completed")
            self.publish_metric("OptimizationSuccess", 1)
            
        except Exception as e:
            logger.error(f"Error optimizing Delta table: {str(e)}")
            self.publish_metric("OptimizationErrors", 1)
   
    def update_glue_catalog(self):
        """Update AWS Glue Data Catalog with error handling"""
        try:
            logger.info("Updating Glue Data Catalog")
            
            # Create database if not exists
            self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.database_name}")
            
            # Check if table already exists in catalog
            table_exists_in_catalog = False
            try:
                self.spark.sql(f"DESCRIBE TABLE {self.database_name}.{self.table_name}")
                table_exists_in_catalog = True
                logger.info(f"Table {self.database_name}.{self.table_name} already exists in catalog")
            except Exception:
                logger.info(f"Table {self.database_name}.{self.table_name} does not exist in catalog, will create")
            
            if not table_exists_in_catalog:
                # Method 1: Create table with explicit schema matching existing Delta properties
                self.spark.sql(f"""
                    CREATE TABLE {self.database_name}.{self.table_name} (
                        order_num BIGINT,
                        order_id STRING,
                        user_id STRING,
                        order_timestamp TIMESTAMP,
                        total_amount DECIMAL(10,2),
                        ingestion_timestamp TIMESTAMP,
                        source_file STRING,
                        date DATE
                    )
                    USING DELTA
                    LOCATION '{self.processed_path}'
                    PARTITIONED BY (date)
                    TBLPROPERTIES (
                        'delta.autoOptimize.optimizeWrite' = 'true',
                        'delta.autoOptimize.autoCompact' = 'true',
                        'delta.deletedFileRetentionDuration' = 'interval 7 days'
                    )
                """)
            else:
                # Table exists, just refresh metadata
                logger.info("Table exists in catalog, refreshing metadata only")
            
            # Alternative Method 2: Let Delta infer schema first, then add partitioning
            # Uncomment this block if Method 1 doesn't work
            """
            # First create table without partitioning to let Delta infer schema
            self.spark.sql(f'''
                CREATE TABLE IF NOT EXISTS {self.database_name}.{self.table_name}
                USING DELTA
                LOCATION '{self.processed_path}'
                TBLPROPERTIES (
                    'delta.autoOptimize.optimizeWrite' = 'true',
                    'delta.autoOptimize.autoCompact' = 'true'
                )
            ''')
            
            # Then add partitioning (this might require recreating the table)
            try:
                self.spark.sql(f"ALTER TABLE {self.database_name}.{self.table_name} ADD PARTITION (date)")
            except Exception as partition_error:
                logger.warning(f"Could not add partition specification: {str(partition_error)}")
            """
            
            # Repair table to discover partitions
            self.spark.sql(f"MSCK REPAIR TABLE {self.database_name}.{self.table_name}")
            
            # Refresh table metadata
            self.spark.sql(f"REFRESH TABLE {self.database_name}.{self.table_name}")
            
            logger.info("Glue Data Catalog updated successfully")
            self.publish_metric("CatalogUpdateSuccess", 1)
            
        except Exception as e:
            logger.error(f"Error updating Glue Data Catalog: {str(e)}")
            self.publish_metric("CatalogUpdateErrors", 1)
            # Don't raise the exception - let the job continue even if catalog update fails
            logger.warning("Continuing job execution despite catalog update failure")
    
    def publish_metric(self, metric_name, value):
        """Publish custom metrics to CloudWatch"""
        try:
            self.cloudwatch.put_metric_data(
                Namespace='GlueJobs/OrdersETL',
                MetricData=[
                    {
                        'MetricName': metric_name,
                        'Value': value,
                        'Unit': 'Count',
                        'Dimensions': [
                            {
                                'Name': 'JobName',
                                'Value': self.job_name
                            }
                        ]
                    }
                ]
            )
        except Exception as e:
            logger.warning(f"Failed to publish metric {metric_name}: {str(e)}")
    
    def get_processing_stats(self):
        """Get comprehensive processing statistics"""
        try:
            delta_table = DeltaTable.forPath(self.spark, self.processed_path)
            df = delta_table.toDF()
            
            # Basic stats
            total_records = df.count()
            
            # Date range stats
            date_stats = df.agg(
                min("date").alias("min_date"),
                max("date").alias("max_date"),
                min("order_timestamp").alias("min_timestamp"),
                max("order_timestamp").alias("max_timestamp"),
                max("ingestion_timestamp").alias("last_ingestion"),
                countDistinct("user_id").alias("unique_users")
            ).collect()[0]
            
            # Amount stats
            amount_stats = df.agg(
                sum("total_amount").alias("total_revenue"),
                avg("total_amount").alias("avg_order_value"),
                min("total_amount").alias("min_amount"),
                max("total_amount").alias("max_amount")
            ).collect()[0]
            
            # Partition stats
            partition_stats = df.groupBy("date").count().orderBy("date").collect()
            
            stats = {
                "total_records": total_records,
                "unique_users": date_stats['unique_users'],
                "date_range": {
                    "min_date": str(date_stats['min_date']),
                    "max_date": str(date_stats['max_date']),
                    "min_timestamp": str(date_stats['min_timestamp']),
                    "max_timestamp": str(date_stats['max_timestamp'])
                },
                "revenue_metrics": {
                    "total_revenue": float(amount_stats['total_revenue']) if amount_stats['total_revenue'] is not None else 0,
                    "avg_order_value": float(amount_stats['avg_order_value']) if amount_stats['avg_order_value'] is not None else 0,
                    "min_amount": float(amount_stats['min_amount']) if amount_stats['min_amount'] is not None else 0,
                    "max_amount": float(amount_stats['max_amount']) if amount_stats['max_amount'] is not None else 0
                },
                "partition_counts": {str(row['date']): row['count'] for row in partition_stats},
                "last_ingestion": str(date_stats['last_ingestion']),
                "table_location": self.processed_path
            }
            
            # Publish key metrics
            self.publish_metric("TotalRecords", total_records)
            self.publish_metric("UniqueUsers", date_stats['unique_users'])
            self.publish_metric("TotalRevenue", float(amount_stats['total_revenue']) if amount_stats['total_revenue'] is not None else 0)
            
            logger.info(f"Processing stats: {stats}")
            return stats
            
        except Exception as e:
            logger.error(f"Error getting processing stats: {str(e)}")
            return {}
    
    def run_etl(self):
        """Main ETL orchestration with mode support and automatic file processing"""
        try:
            logger.info(f"Starting Orders ETL job: {self.job_name} (mode: {self.mode})")
            
            # Get all CSV files from raw zone
            raw_files = self.list_raw_files()
            
            if not raw_files:
                logger.info("No CSV files found in raw zone")
                return {"message": "No files to process", "mode": self.mode, "files_processed": 0}
            
            processed_files = []
            failed_files = []
            total_valid_records = 0
            
            # Process each file individually
            for file_path in raw_files:
                try:
                    logger.info(f"Processing file: {file_path}")
                    
                    # Read raw data for this file
                    df_raw = self.read_raw_data(file_path)
                    
                    # Validate and clean data for this file
                    df_cleaned = self.validate_and_clean_data(df_raw, file_path)
                    
                    if self.mode == "validate_only":
                        # Move to archive even in validate-only mode if validation passes
                        archived_path = self.move_file_to_archive(file_path)
                        processed_files.append({
                            "original": file_path,
                            "archived": archived_path,
                            "status": "validated"
                        })
                        continue
                    
                    if df_cleaned.count() == 0:
                        logger.warning(f"No valid records in file: {file_path}")
                        # Move to rejected zone due to no valid records
                        rejected_path = self.move_file_to_rejected(file_path, "No valid records after validation")
                        failed_files.append({
                            "original": file_path,
                            "rejected": rejected_path,
                            "reason": "No valid records"
                        })
                        continue
                    
                    # Upsert to Delta table
                    self.upsert_to_delta_table(df_cleaned)
                    total_valid_records += df_cleaned.count()
                    
                    # Move successfully processed file to archive
                    archived_path = self.move_file_to_archive(file_path)
                    processed_files.append({
                        "original": file_path,
                        "archived": archived_path,
                        "status": "processed",
                        "records": df_cleaned.count()
                    })
                    
                    logger.info(f"Successfully processed file: {file_path}")
                    
                except Exception as file_error:
                    logger.error(f"Failed to process file {file_path}: {str(file_error)}")
                    try:
                        # Move failed file to rejected zone
                        rejected_path = self.move_file_to_rejected(file_path, str(file_error))
                        failed_files.append({
                            "original": file_path,
                            "rejected": rejected_path,
                            "reason": str(file_error)
                        })
                    except Exception as move_error:
                        logger.error(f"Failed to move file to rejected zone: {str(move_error)}")
                        failed_files.append({
                            "original": file_path,
                            "rejected": None,
                            "reason": f"Processing: {str(file_error)}, Move: {str(move_error)}"
                        })
            
            # Update Glue Catalog only if we have processed files and not in upsert_only mode
            if processed_files and self.mode != "upsert_only":
                self.update_glue_catalog()
            
            # Get final stats
            stats = {}
            if total_valid_records > 0:
                stats = self.get_processing_stats()
            
            # Add processing summary
            stats.update({
                "mode": self.mode,
                "files_processed": len(processed_files),
                "files_failed": len(failed_files),
                "total_files": len(raw_files),
                "total_valid_records": total_valid_records,
                "processed_files": processed_files,
                "failed_files": failed_files
            })
            
            logger.info("Orders ETL job completed")
            logger.info(f"Files processed: {len(processed_files)}, Files failed: {len(failed_files)}")
            
            return stats
            
        except Exception as e:
            logger.error(f"ETL job failed: {str(e)}")
            self.publish_metric("JobFailures", 1)
            raise

def get_job_parameters():
    """Get job parameters with proper handling of optional arguments"""
    # Define required parameters
    required_args = [
        'JOB_NAME',
        'raw_bucket',
        'processed_bucket',
        'database_name',
        'table_name'
    ]
    
    # Get required parameters first
    args = getResolvedOptions(sys.argv, required_args)
    
    # Handle optional parameters manually to avoid GlueArgumentError
    optional_params = {
        'mode': 'full'  # Default mode - removed input_path
    }
    
    # Parse optional parameters from sys.argv
    for i, arg in enumerate(sys.argv):
        if arg.startswith('--mode') and i + 1 < len(sys.argv):
            optional_params['mode'] = sys.argv[i + 1]
    
    # Merge required and optional parameters
    args.update(optional_params)
    
    return args

def main():
    """Main function with enhanced parameter handling"""
    # Get job parameters
    args = get_job_parameters()
    
    # Initialize Spark session with enhanced configurations
    spark = SparkSession.builder \
        .appName("OrdersETL") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()
    
    # Set log level
    spark.sparkContext.setLogLevel("INFO")
    
    # Initialize Glue context and job
    glue_context = GlueContext(spark)
    job = Job(glue_context)
    job.init(args['JOB_NAME'], args)
    
    try:
        # Log job parameters
        logger.info(f"Job parameters: {args}")
        
        # Initialize ETL class
        etl = OrdersETL(
            spark,
            glue_context,
            args['JOB_NAME'],
            args['raw_bucket'],
            args['processed_bucket'],
            args['database_name'],
            args['table_name'],
            args['mode']
        )
        
        # Run ETL pipeline (no input_path parameter)
        stats = etl.run_etl()
        
        # Commit the job
        job.commit()
        
        logger.info(f"Job completed successfully with stats: {stats}")
        
    except Exception as e:
        logger.error(f"Job failed with error: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()