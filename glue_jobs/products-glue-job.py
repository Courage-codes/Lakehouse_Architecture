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
    
    def move_files_to_archive(self, processed_files):
        """Move successfully processed files from raw zone to archive zone"""
        try:
            logger.info("Moving processed files to archive zone")
            
            s3_client = boto3.client('s3')
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            
            for file_path in processed_files:
                # Extract filename from S3 path
                filename = file_path.split('/')[-1]
                
                # Define source and destination
                source_key = f"raw-zone/products/{filename}"
                dest_key = f"archived/products/{timestamp}_{filename}"
                
                # Copy file to archive
                s3_client.copy_object(
                    Bucket=self.raw_bucket,
                    CopySource={'Bucket': self.raw_bucket, 'Key': source_key},
                    Key=dest_key
                )
                
                # Delete from raw zone
                s3_client.delete_object(Bucket=self.raw_bucket, Key=source_key)
                
                logger.info(f"Moved {filename} to archive: {dest_key}")
                
        except Exception as e:
            logger.error(f"Error moving files to archive: {str(e)}")
            # Don't fail the job for archiving errors


    def move_files_to_rejected(self, failed_files, error_reason):
        """Move failed files from raw zone to rejected zone"""
        try:
            logger.info("Moving failed files to rejected zone")
            
            s3_client = boto3.client('s3')
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            
            for file_path in failed_files:
                # Extract filename from S3 path
                filename = file_path.split('/')[-1]
                
                # Define source and destination
                source_key = f"raw-zone/products/{filename}"
                dest_key = f"rejected/products/{timestamp}_{error_reason}_{filename}"
                
                # Copy file to rejected
                s3_client.copy_object(
                    Bucket=self.raw_bucket,
                    CopySource={'Bucket': self.raw_bucket, 'Key': source_key},
                    Key=dest_key
                )
                
                # Delete from raw zone
                s3_client.delete_object(Bucket=self.raw_bucket, Key=source_key)
                
                logger.info(f"Moved {filename} to rejected: {dest_key}")
                
        except Exception as e:
            logger.error(f"Error moving files to rejected: {str(e)}")

    def list_raw_files(self):
        """List all CSV files in the raw zone"""
        try:
            s3_client = boto3.client('s3')
            response = s3_client.list_objects_v2(
                Bucket=self.raw_bucket,
                Prefix='raw-zone/products/'
            )
            
            files = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    if obj['Key'].endswith('.csv'):
                        files.append(f"s3://{self.raw_bucket}/{obj['Key']}")
            
            logger.info(f"Found {len(files)} CSV files in raw zone")
            return files
            
        except Exception as e:
            logger.error(f"Error listing raw files: {str(e)}")
            return []

    def validate_and_clean_data(self, df_raw, source_file):
        """Comprehensive data validation and cleaning"""
        logger.info("Starting data validation and cleaning")
        
        # Add metadata columns
        df_with_metadata = df_raw.withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("source_file", lit(source_file))
        
        # Data quality checks
        valid_records = df_with_metadata.filter(
            # Primary key validation
            (col("product_id").isNotNull()) &
            (col("product_id") != "") &
            (length(trim(col("product_id"))) > 0) &
            
            # Department validation
            (col("department_id").isNotNull()) &
            (col("department_id") != "") &
            (col("department").isNotNull()) &
            (col("department") != "") &
            
            # Product name validation
            (col("product_name").isNotNull()) &
            (col("product_name") != "") &
            (length(trim(col("product_name"))) > 0)
        )
        
        # Invalid records for quarantine
        invalid_records = df_with_metadata.exceptAll(valid_records)
        
        # Log validation results
        total_records = df_with_metadata.count()
        valid_count = valid_records.count()
        invalid_count = invalid_records.count()
        
        logger.info(f"Data validation completed:")
        logger.info(f"  Total records: {total_records}")
        logger.info(f"  Valid records: {valid_count}")
        logger.info(f"  Invalid records: {invalid_count}")
        
        # If all records are invalid, move file to rejected zone
        if valid_count == 0 and total_records > 0:
            logger.warning(f"All records in {source_file} are invalid, moving to rejected zone")
            self.move_files_to_rejected([source_file], "all_records_invalid")
            raise ValueError(f"All records in {source_file} failed validation")
        
        # Quarantine invalid records if any
        if invalid_count > 0:
            self.quarantine_invalid_records(invalid_records, source_file)
        
        # Apply data cleansing
        cleaned_df = valid_records.select(
            trim(upper(col("product_id"))).alias("product_id"),
            trim(col("department_id")).alias("department_id"),
            trim(col("department")).alias("department"),
            trim(col("product_name")).alias("product_name"),
            col("ingestion_timestamp"),
            col("source_file")
        )
        
        return cleaned_df

    
    def quarantine_invalid_records(self, invalid_df, source_file):
        """Save invalid records to quarantine zone"""
        try:
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            quarantine_file_path = f"{self.quarantine_path}{timestamp}_{source_file.split('/')[-1]}"
            
            # Add rejection reasons
            invalid_with_reasons = invalid_df.withColumn(
                "rejection_reason",
                when(col("product_id").isNull() | (col("product_id") == ""), "Missing product_id")
                .when(col("department_id").isNull() | (col("department_id") == ""), "Missing department_id")
                .when(col("department").isNull() | (col("department") == ""), "Missing department")
                .when(col("product_name").isNull() | (col("product_name") == ""), "Missing product_name")
                .otherwise("Multiple validation failures")
            ).withColumn("quarantine_timestamp", current_timestamp())
            
            invalid_with_reasons.coalesce(1).write.mode("overwrite") \
                .option("header", "true") \
                .csv(quarantine_file_path)
            
            logger.info(f"Invalid records quarantined to: {quarantine_file_path}")
            
        except Exception as e:
            logger.error(f"Error quarantining invalid records: {str(e)}")
            # Don't fail the job for quarantine errors
    
    def upsert_to_delta_table(self, df_cleaned):
        """Upsert data to Delta table with deduplication"""
        try:
            logger.info("Starting Delta table upsert operation")
            
            # Deduplicate within the current batch
            df_deduped = df_cleaned.dropDuplicates(["product_id"])
            
            # Check if Delta table exists
            if DeltaTable.isDeltaTable(self.spark, self.processed_path):
                logger.info("Delta table exists, performing merge operation")
                
                # Load existing Delta table
                delta_table = DeltaTable.forPath(self.spark, self.processed_path)
                
                # Perform merge (upsert) operation
                delta_table.alias("target").merge(
                    df_deduped.alias("source"),
                    "target.product_id = source.product_id"
                ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
                
                logger.info("Merge operation completed successfully")
                
            else:
                logger.info("Delta table doesn't exist, creating new table")
                
                # Create new Delta table
                df_deduped.write.format("delta") \
                    .mode("overwrite") \
                    .option("mergeSchema", "true") \
                    .save(self.processed_path)
                
                logger.info("New Delta table created successfully")
            
            # Optimize Delta table
            self.optimize_delta_table()
            
        except Exception as e:
            logger.error(f"Error in Delta table upsert: {str(e)}")
            raise
    def optimize_delta_table(self):
        """Optimize Delta table for better performance"""
        try:
            logger.info("Starting Delta table optimization")
            
            # Run OPTIMIZE command
            self.spark.sql(f"""
                OPTIMIZE delta.`{self.processed_path}`
            """)
            
            # Run VACUUM to clean up old files (7 days retention)
            self.spark.sql(f"""
                VACUUM delta.`{self.processed_path}` RETAIN 168 HOURS
            """)
            
            logger.info("Delta table optimization completed")
            
        except Exception as e:
            logger.error(f"Error optimizing Delta table: {str(e)}")
            # Don't fail the job for optimization errors
        

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