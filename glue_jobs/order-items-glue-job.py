import sys
import boto3
import os
from urllib.parse import urlparse
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
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

class OrderItemsETL:
    def __init__(self, spark, glue_context, job_name, raw_bucket, processed_bucket, database_name, table_name):
        self.spark = spark
        self.glue_context = glue_context
        self.job_name = job_name

        self.raw_bucket = raw_bucket
        self.processed_bucket = processed_bucket
        self.database_name = database_name
        self.table_name = table_name

        self.raw_path = f"s3://{self.raw_bucket}/raw-zone/order-items/"
        self.processed_path = f"s3://{self.processed_bucket}/processed-zone/order_items_delta/"
        self.quarantine_path = f"s3://{self.processed_bucket}/rejected/order-items/"
        self.archived_path = f"s3://{self.raw_bucket}/archived/order-items/"

        self.orders_path = f"s3://{self.processed_bucket}/processed-zone/orders_delta/"
        self.products_path = f"s3://{self.processed_bucket}/processed-zone/products_delta/"
        
        self.schema = StructType([
            StructField("id", StringType(), False),
            StructField("order_id", StringType(), False),
            StructField("user_id", StringType(), False),
            StructField("days_since_prior_order", StringType(), True),
            StructField("product_id", StringType(), False),
            StructField("add_to_cart_order", StringType(), False),
            StructField("reordered", StringType(), False),
            StructField("order_timestamp", StringType(), False),
            StructField("date", StringType(), False)
        ])

        self.s3_client = boto3.client('s3')

    def read_raw_data(self, file_path):
        try:
            logger.info(f"Reading raw data from: {file_path}")
            df_raw = self.spark.read.format("csv") \
                .option("header", "true") \
                .load(file_path)
            logger.info(f"CSV columns: {df_raw.columns}")
            df_raw.show(3, False)
            return df_raw
        except Exception as e:
            logger.error(f"Error reading raw data: {str(e)}")
            raise

    def list_files_in_s3_folder(self, s3_path):
        """List all files in S3 folder"""
        try:
            parsed_url = urlparse(s3_path)
            bucket = parsed_url.netloc
            prefix = parsed_url.path.lstrip('/')
            
            response = self.s3_client.list_objects_v2(
                Bucket=bucket,
                Prefix=prefix
            )
            
            files = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    if not obj['Key'].endswith('/'):
                        files.append(f"s3://{bucket}/{obj['Key']}")
            
            return files
        except Exception as e:
            logger.error(f"Error listing files in {s3_path}: {str(e)}")
            return []

    def move_s3_file(self, source_s3_path, destination_s3_path):
        """Move file from source to destination in S3"""
        try:
            source_parsed = urlparse(source_s3_path)
            dest_parsed = urlparse(destination_s3_path)
            
            source_bucket = source_parsed.netloc
            source_key = source_parsed.path.lstrip('/')
            dest_bucket = dest_parsed.netloc
            dest_key = dest_parsed.path.lstrip('/')
            
            self.s3_client.copy_object(
                Bucket=dest_bucket,
                CopySource={'Bucket': source_bucket, 'Key': source_key},
                Key=dest_key
            )
            
            self.s3_client.delete_object(Bucket=source_bucket, Key=source_key)
            logger.info(f"Moved {source_s3_path} to {destination_s3_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error moving file {source_s3_path}: {str(e)}")
            return False

    def move_to_archived(self, source_file_path):
        """Move processed file to archived zone"""
        try:
            filename = os.path.basename(urlparse(source_file_path).path)
            archived_file_path = f"{self.archived_path}{filename}"
            
            return self.move_s3_file(source_file_path, archived_file_path)
            
        except Exception as e:
            logger.error(f"Error moving to archived: {str(e)}")
            return False


def main():
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME',
        'raw_bucket',
        'processed_bucket',
        'database_name',
        'table_name'
    ])
    
    input_path = None
    try:
        if '--input_path' in sys.argv:
            input_path_args = getResolvedOptions(sys.argv, ['input_path'])
            input_path = input_path_args.get('input_path')
    except Exception:
        logger.info("No input_path provided, will process all files in raw zone")
    
    spark = SparkSession.builder \
        .appName("OrderItemsETL") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \
        .getOrCreate()
        
    glue_context = GlueContext(spark)
    job = Job(glue_context)
    job.init(args['JOB_NAME'], args)
    
    etl = OrderItemsETL(
        spark,
        glue_context,
        args['JOB_NAME'],
        args['raw_bucket'],
        args['processed_bucket'],
        args['database_name'],
        args['table_name']
    )
    
    result = etl.run_etl(input_path)
    logger.info(result.get("message"))
    
    if "processed_files" in result:
        logger.info(f"Files processed: {result['processed_files']}")
        logger.info(f"Files failed: {result['failed_files']}")
    
    job.commit()
    spark.stop()

if __name__ == "__main__":
    main()