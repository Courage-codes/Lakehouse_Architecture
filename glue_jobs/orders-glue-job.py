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