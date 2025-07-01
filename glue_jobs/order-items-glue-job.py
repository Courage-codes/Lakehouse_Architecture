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

    def move_to_rejected(self, source_file_path):
        """Move failed file to rejected zone"""
        try:
            filename = os.path.basename(urlparse(source_file_path).path)
            rejected_file_path = f"{self.quarantine_path}{filename}"
            
            return self.move_s3_file(source_file_path, rejected_file_path)
            
        except Exception as e:
            logger.error(f"Error moving to rejected: {str(e)}")
            return False

    def load_reference_data(self):
        try:
            logger.info("Loading reference data")
            orders_df = products_df = None
            if DeltaTable.isDeltaTable(self.spark, self.orders_path):
                orders_df = DeltaTable.forPath(self.spark, self.orders_path).toDF().select("order_id", "user_id").distinct()
            if DeltaTable.isDeltaTable(self.spark, self.products_path):
                products_df = DeltaTable.forPath(self.spark, self.products_path).toDF().select("product_id").distinct()
            return orders_df, products_df
        except Exception as e:
            logger.error(f"Error loading reference data: {str(e)}")
            return None, None


    def validate_and_clean_data(self, df_raw, source_file, orders_ref=None, products_ref=None):
        logger.info("Validating and cleaning data ")
        
        df = df_raw.withColumn("ingestion_timestamp", current_timestamp()) \
                .withColumn("source_file", lit(source_file))
        
        try:
            df = df.withColumn("days_since_prior_order_int", 
                            when(col("days_since_prior_order").rlike("^[0-9]+$"), 
                                col("days_since_prior_order").cast("int")))
            
            df = df.withColumn("add_to_cart_order_int", 
                            when(col("add_to_cart_order").rlike("^[0-9]+$"), 
                                col("add_to_cart_order").cast("int")))
            
            df = df.withColumn("reordered_bool", 
                            when(lower(col("reordered")).isin("true", "1", "yes"), lit(True))
                            .when(lower(col("reordered")).isin("false", "0", "no"), lit(False)))
            
            df = df.withColumn("order_timestamp_ts", 
                            coalesce(
                                to_timestamp("order_timestamp", "yyyy-MM-dd'T'HH:mm:ss"),
                                to_timestamp("order_timestamp", "yyyy-MM-dd'T'HH:mm"),
                                to_timestamp("order_timestamp", "yyyy-MM-dd HH:mm:ss"),
                                to_timestamp("order_timestamp", "yyyy-MM-dd HH:mm")
                            ))
            
            df = df.withColumn("date_dt", to_date("date", "yyyy-MM-dd"))
            
            logger.info("Sample timestamp transformations:")
            df.select("order_timestamp", "order_timestamp_ts").show(3, False)
            
            valid_df = df
            
            logger.info("Applying required field filters...")
            valid_df = valid_df.filter(
                col("id").isNotNull() & (trim(col("id")) != "") &
                col("order_id").isNotNull() & (trim(col("order_id")) != "") &
                col("user_id").isNotNull() & (trim(col("user_id")) != "") &
                col("product_id").isNotNull() & (trim(col("product_id")) != "")
            )
            logger.info(f"After required field validation: {valid_df.count()} records")
            
            logger.info("Applying numeric field filters...")
            valid_df = valid_df.filter(
                col("add_to_cart_order_int").isNotNull() &
                (col("add_to_cart_order_int") > 0)
            )
            logger.info(f"After numeric validation: {valid_df.count()} records")
            
            logger.info("Applying timestamp filters...")
            logger.info(f"Records with null timestamps: {valid_df.filter(col('order_timestamp_ts').isNull()).count()}")
            
            valid_df = valid_df.filter(col("order_timestamp_ts").isNotNull())
            logger.info(f"After timestamp validation: {valid_df.count()} records")
            
            start_date = lit("2020-01-01").cast("date")
            today = current_date()
            
            logger.info(f"Records with null dates: {valid_df.filter(col('date_dt').isNull()).count()}")
            
            valid_df = valid_df.filter(
                col("date_dt").isNotNull() &
                (col("date_dt") >= start_date) &
                (col("date_dt") <= today)
            )
            logger.info(f"After date validation: {valid_df.count()} records")
            
            logger.info("Applying days_since_prior_order filters...")
            valid_df = valid_df.filter(
                col("days_since_prior_order_int").isNull() |
                (col("days_since_prior_order_int") >= 0) & (col("days_since_prior_order_int") <= 365)
            )
            logger.info(f"After days_since_prior_order validation: {valid_df.count()} records")
            
            if orders_ref is not None and orders_ref.count() > 0:
                valid_df = valid_df.join(orders_ref, ["order_id", "user_id"], "inner")
                logger.info(f"Records after order validation: {valid_df.count()}")
            else:
                logger.warning("Skipping order validation - no reference data or empty table.")
                
            if products_ref is not None and products_ref.count() > 0:
                valid_df = valid_df.join(products_ref, ["product_id"], "inner")
                logger.info(f"Records after product validation: {valid_df.count()}")
            else:
                logger.warning("Skipping product validation - no reference data or empty table.")

            invalid_df = df.join(valid_df.select(*df.columns), df.columns, "left_anti")
            if not invalid_df.rdd.isEmpty():
                self.quarantine_invalid_records(invalid_df, source_file)

            return valid_df.select(
                trim(upper(col("id"))).alias("id"),
                trim(upper(col("order_id"))).alias("order_id"),
                trim(col("user_id")).alias("user_id"),
                col("days_since_prior_order_int").alias("days_since_prior_order"),
                trim(upper(col("product_id"))).alias("product_id"),
                col("add_to_cart_order_int").alias("add_to_cart_order"),
                col("reordered_bool").alias("reordered"),
                col("order_timestamp_ts").alias("order_timestamp"),
                col("date_dt").alias("date"),
                col("ingestion_timestamp"),
                col("source_file")
            )
            
        except Exception as e:
            logger.error(f"Error in defensive validation: {str(e)}")
            raise

    def quarantine_invalid_records(self, df, source_file):
        """Updated to not duplicate rejected files"""
        try:
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            file_name = os.path.basename(urlparse(source_file).path) if source_file else "unknown"
            path = f"{self.quarantine_path}validation_failures_{timestamp}_{file_name}"
            
            df.withColumn("quarantine_timestamp", current_timestamp()) \
              .withColumn("quarantine_reason", lit("validation_failure")) \
              .write.mode("overwrite") \
              .option("header", "true") \
              .csv(path)
            
            logger.info(f"Invalid records quarantined to: {path}")
            
        except Exception as e:
            logger.error(f"Error quarantining invalid records: {str(e)}")

    def upsert_to_delta_table(self, df):
        window_spec = Window.partitionBy("id").orderBy(col("ingestion_timestamp").desc())
        df = df.withColumn("row_num", row_number().over(window_spec)).filter(col("row_num") == 1).drop("row_num")
        if DeltaTable.isDeltaTable(self.spark, self.processed_path):
            delta_table = DeltaTable.forPath(self.spark, self.processed_path)
            delta_table.alias("target").merge(
                df.alias("source"),
                "target.id = source.id"
            ).whenMatchedUpdateAll(
                condition="source.ingestion_timestamp > target.ingestion_timestamp"
            ).whenNotMatchedInsertAll().execute()
        else:
            df.repartition("date").write.format("delta") \
              .mode("overwrite") \
              .partitionBy("date") \
              .option("mergeSchema", "true") \
              .option("delta.autoOptimize.optimizeWrite", "true") \
              .option("delta.autoOptimize.autoCompact", "true") \
              .save(self.processed_path)
        self.optimize_delta_table(df)

    def optimize_delta_table(self, df):
        if DeltaTable.isDeltaTable(self.spark, self.processed_path):
            delta_table = DeltaTable.forPath(self.spark, self.processed_path)
            partitions = df.select("date").distinct().collect()
            for row in partitions:
                logger.info(f"Optimizing partition: date='{row['date']}'")
                delta_table.optimize().where(f"date = '{row['date']}'").executeZOrderBy("order_id", "user_id", "product_id")
            logger.info("Running VACUUM on the delta table.")
            delta_table.vacuum()



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