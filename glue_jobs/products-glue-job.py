import sys
import boto3
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ProductsETL:
    def __init__(self, spark, glue_context, job_name, raw_bucket, processed_bucket, database_name, table_name):
        self.spark = spark
        self.glue_context = glue_context
        self.job_name = job_name
        
        # Configuration
        self.raw_bucket = raw_bucket
        self.processed_bucket = processed_bucket
        self.database_name = database_name
        self.table_name = table_name
        
        # Paths aligned with provided S3 structure
        self.raw_path = f"s3://{self.raw_bucket}/raw-zone/products/"
        self.processed_path = f"s3://{self.processed_bucket}/processed-zone/products_delta/"
        self.quarantine_path = f"s3://{self.processed_bucket}/rejected/products/"
        
        # Schema definition
        self.schema = StructType([
            StructField("product_id", StringType(), False),
            StructField("department_id", StringType(), False),
            StructField("department", StringType(), False),
            StructField("product_name", StringType(), False),
            StructField("ingestion_timestamp", TimestampType(), False),
            StructField("source_file", StringType(), False)
        ])
        
    def read_raw_data(self, file_path):
        """Read CSV data from S3 with error handling"""
        try:
            logger.info(f"Reading raw data from: {file_path}")
            
            # Read CSV with schema inference disabled for initial validation
            df_raw = self.spark.read.format("csv") \
                .option("header", "true") \
                .option("inferSchema", "false") \
                .load(file_path)
            
            logger.info(f"Raw data read successfully. Row count: {df_raw.count()}")
            return df_raw
            
        except Exception as e:
            logger.error(f"Error reading raw data: {str(e)}")
            raise



def main():
    """Main function to run the Glue job"""
    # Get job parameters (removed input_path)
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME',
        'raw_bucket',
        'processed_bucket',
        'database_name',
        'table_name'
    ])
    
    # Initialize Spark session with Delta Lake
    spark = SparkSession.builder \
        .appName("ProductsETL") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    # Initialize Glue context and job
    glue_context = GlueContext(spark)
    job = Job(glue_context)
    job.init(args['JOB_NAME'], args)
    
    try:
        # Initialize ETL class
        etl = ProductsETL(
            spark,
            glue_context,
            args['JOB_NAME'],
            args['raw_bucket'],
            args['processed_bucket'],
            args['database_name'],
            args['table_name']
        )
        
        # Run ETL pipeline (removed input_path parameter)
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