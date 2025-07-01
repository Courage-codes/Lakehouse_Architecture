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