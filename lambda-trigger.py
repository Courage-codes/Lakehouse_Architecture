import json
import boto3
import pandas as pd
from io import BytesIO
import logging
import os
from datetime import datetime, timedelta

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize boto3 clients
s3_client = boto3.client('s3')
stepfunctions_client = boto3.client('stepfunctions')

# Configuration
BUCKET_NAME = os.environ.get('BUCKET_NAME', 'ecommerce-lakehouse--bucket')
RAW_PREFIX = os.environ.get('RAW_PREFIX', 'raw/')
RAW_ZONE_PREFIX = os.environ.get('RAW_ZONE_PREFIX', 'raw-zone/')
SUBFOLDERS = os.environ.get('SUBFOLDERS', 'products,orders,order-items').split(',')
STEP_FUNCTION_ARN = 'arn:aws:states:us-east-1:814724283777:stateMachine:Lakehouse'

# Required columns for each subfolder
REQUIRED_COLUMNS = {
    'products': ['product_id', 'department_id', 'department', 'product_name'],
    'order-items': ['id', 'order_id', 'user_id', 'days_since_prior_order', 'product_id', 
                   'add_to_cart_order', 'reordered', 'order_timestamp', 'date'],
    'orders': ['order_num', 'order_id', 'user_id', 'order_timestamp', 'total_amount', 'date']
}

# Glue job mapping
GLUE_JOB_MAPPING = {
    'products': 'products-glue-job.py',
    'order-items': 'order-items-glue-job.py',
    'orders': 'orders-glue-job.py'
}

def lambda_handler(event, context):
    """
    AWS Lambda handler function - triggered by S3 events only
    """
    
    start_time = datetime.now()
    logger.info(f"Lambda function started at {start_time}")
    
    try:
        # Only process S3 events
        if 'Records' not in event:
            logger.warning("Function not triggered by S3 event. Exiting.")
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'message': 'Function only processes S3 events',
                    'timestamp': datetime.now().isoformat()
                })
            }
        
        # Process S3 events
        results = process_s3_events(event['Records'])
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        # Get successfully processed subfolders for Step Functions
        successful_results = [
            result for result in results 
            if result.get('status') == 'success' and result.get('result', {}).get('files_processed', 0) > 0
        ]
        
        # Trigger Step Functions if we have successfully processed data
        step_function_result = None
        if successful_results:
            step_function_result = trigger_step_function(successful_results, event, context)
        
        response = {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'S3 event processing completed',
                'results': results,
                'successful_processing': successful_results,
                'step_function_execution': step_function_result,
                'duration_seconds': duration,
                'timestamp': end_time.isoformat()
            })
        }
        
        logger.info(f"Lambda function completed successfully in {duration} seconds")
        return response
        
    except Exception as e:
        error_msg = f"Lambda function failed: {str(e)}"
        logger.error(error_msg)
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': error_msg,
                'timestamp': datetime.now().isoformat()
            })
        }

def validate_columns(dataframe, subfolder):
    """
    Validate that the dataframe contains all required columns for the subfolder
    """
    required_cols = REQUIRED_COLUMNS.get(subfolder, [])
    if not required_cols:
        logger.warning(f"No required columns defined for subfolder: {subfolder}")
        return True, []
    
    # Get actual columns (case-insensitive comparison)
    actual_cols = [col.lower().strip() for col in dataframe.columns]
    required_cols_lower = [col.lower().strip() for col in required_cols]
    
    # Check for missing columns
    missing_cols = []
    for req_col in required_cols_lower:
        if req_col not in actual_cols:
            missing_cols.append(req_col)
    
    if missing_cols:
        logger.error(f"Missing required columns for {subfolder}: {missing_cols}")
        logger.info(f"Available columns: {list(dataframe.columns)}")
        return False, missing_cols
    
    logger.info(f"Column validation passed for {subfolder}")
    return True, []

def trigger_step_function(successful_results, original_event, context):
    """Trigger Step Functions with Glue job information"""
    try:
        # Determine which Glue jobs to run
        glue_jobs_to_run = []
        processed_subfolders = []
        
        for result in successful_results:
            subfolder = result.get('subfolder')
            if subfolder and subfolder in GLUE_JOB_MAPPING:
                glue_job = GLUE_JOB_MAPPING[subfolder]
                glue_jobs_to_run.append({
                    'subfolder': subfolder,
                    'glue_job': glue_job,
                    'output_file': result.get('result', {}).get('output_file'),
                    'rows_processed': result.get('result', {}).get('rows_processed', 0)
                })
                processed_subfolders.append(subfolder)
        
        # Prepare input for Step Functions
        step_function_input = {
            'lambda_execution_id': context.aws_request_id,
            'timestamp': datetime.now().isoformat(),
            'bucket_name': BUCKET_NAME,
            'raw_zone_prefix': RAW_ZONE_PREFIX,
            'glue_jobs_to_run': glue_jobs_to_run,
            'processed_subfolders': processed_subfolders,
            'total_jobs': len(glue_jobs_to_run),
            'original_s3_event': original_event
        }
        
        # Start Step Functions execution
        execution_name = f"lakehouse-etl-{datetime.now().strftime('%Y%m%d-%H%M%S')}-{context.aws_request_id[:8]}"
        
        response = stepfunctions_client.start_execution(
            stateMachineArn=STEP_FUNCTION_ARN,
            name=execution_name,
            input=json.dumps(step_function_input)
        )
        
        logger.info(f"Started Step Functions execution: {response['executionArn']}")
        logger.info(f"Glue jobs to run: {[job['glue_job'] for job in glue_jobs_to_run]}")
        
        return {
            'execution_arn': response['executionArn'],
            'execution_name': execution_name,
            'start_date': response['startDate'].isoformat(),
            'status': 'STARTED',
            'glue_jobs_scheduled': glue_jobs_to_run
        }
        
    except Exception as e:
        logger.error(f"Failed to trigger Step Functions: {str(e)}")
        return {
            'status': 'FAILED',
            'error': str(e)
        }

def process_s3_events(records):
    """Process S3 event notifications"""
    results = []
    
    # Group files by subfolder
    subfolder_files = {}
    
    for record in records:
        if record.get('eventSource') == 'aws:s3':
            bucket = record['s3']['bucket']['name']
            key = record['s3']['object']['key']
            
            logger.info(f"Processing S3 event for: s3://{bucket}/{key}")
            
            # Determine which subfolder this file belongs to
            subfolder = determine_subfolder_from_key(key)
            if subfolder:
                if subfolder not in subfolder_files:
                    subfolder_files[subfolder] = []
                subfolder_files[subfolder].append(key)
            else:
                logger.warning(f"Could not determine subfolder for key: {key}")
    
    # Process each subfolder
    for subfolder, files in subfolder_files.items():
        try:
            # Check if this subfolder was recently processed to avoid duplicate processing
            if check_recent_processing(subfolder, timeframe_minutes=5):
                logger.info(f"Skipping {subfolder} - recently processed within 5 minutes")
                results.append({
                    'subfolder': subfolder,
                    'trigger_files': files,
                    'status': 'skipped',
                    'reason': 'Recently processed within 5 minutes',
                    'result': {'files_processed': 0, 'rows_processed': 0}
                })
                continue
            
            logger.info(f"Processing {len(files)} file(s) for subfolder: {subfolder}")
            result = process_subfolder_files(subfolder, files)
            results.append({
                'subfolder': subfolder,
                'trigger_files': files,
                'status': 'success',
                'result': result
            })
        except Exception as e:
            logger.error(f"Failed to process subfolder {subfolder}: {str(e)}")
            results.append({
                'subfolder': subfolder,
                'trigger_files': files,
                'status': 'error',
                'error': str(e)
            })
    
    return results

def determine_subfolder_from_key(key):
    """Determine subfolder from S3 key"""
    if key.startswith(RAW_PREFIX):
        path_parts = key.replace(RAW_PREFIX, '').split('/')
        if path_parts and path_parts[0] in SUBFOLDERS:
            return path_parts[0]
    return None

def check_recent_processing(subfolder, timeframe_minutes=5):
    """
    Check if this subfolder was recently processed by looking for recent output files
    Returns True if recent processing detected
    """
    try:
        raw_zone_prefix = f"{RAW_ZONE_PREFIX}{subfolder}/"
        
        # List recent files in raw-zone for this subfolder
        current_time = datetime.now()
        cutoff_time = current_time - timedelta(minutes=timeframe_minutes)
        
        response = s3_client.list_objects_v2(
            Bucket=BUCKET_NAME,
            Prefix=raw_zone_prefix,
            MaxKeys=10  # Only check recent files
        )
        
        if 'Contents' in response:
            for obj in response['Contents']:
                if obj['LastModified'].replace(tzinfo=None) > cutoff_time:
                    logger.info(f"Recent processing detected for {subfolder}: {obj['Key']} at {obj['LastModified']}")
                    return True
        
        return False
        
    except Exception as e:
        logger.error(f"Error checking recent processing for {subfolder}: {str(e)}")
        return False

def file_exists_in_s3(bucket, key):
    """Check if a file exists in S3"""
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except s3_client.exceptions.NoSuchKey:
        return False
    except Exception as e:
        logger.error(f"Error checking file existence {key}: {str(e)}")
        raise

def read_excel_from_s3(bucket, key):
    """Read Excel file from S3 with existence check"""
    try:
        logger.info(f"Reading Excel file: {key}")
        
        # Check if file exists first
        if not file_exists_in_s3(bucket, key):
            logger.warning(f"Excel file does not exist: {key}")
            return None
        
        # Download file from S3
        response = s3_client.get_object(Bucket=bucket, Key=key)
        excel_content = response['Body'].read()
        
        # Read all worksheets
        excel_file = pd.ExcelFile(BytesIO(excel_content))
        worksheet_names = excel_file.sheet_names
        
        logger.info(f"Found {len(worksheet_names)} worksheets: {worksheet_names}")
        
        # Combine all worksheets
        combined_df = pd.DataFrame()
        for sheet_name in worksheet_names:
            df = pd.read_excel(BytesIO(excel_content), sheet_name=sheet_name)
            logger.info(f"Worksheet '{sheet_name}' has {len(df)} rows")
            combined_df = pd.concat([combined_df, df], ignore_index=True)
        
        logger.info(f"Combined DataFrame has {len(combined_df)} rows")
        return combined_df
        
    except s3_client.exceptions.NoSuchKey:
        logger.warning(f"Excel file not found: {key}")
        return None
    except Exception as e:
        logger.error(f"Error reading Excel file {key}: {str(e)}")
        raise

def read_csv_from_s3(bucket, key):
    """Read CSV file from S3 with existence check"""
    try:
        logger.info(f"Reading CSV file: {key}")
        
        # Check if file exists first
        if not file_exists_in_s3(bucket, key):
            logger.warning(f"CSV file does not exist: {key}")
            return None
        
        # Download file from S3
        response = s3_client.get_object(Bucket=bucket, Key=key)
        csv_content = response['Body'].read()
        
        # Read CSV
        df = pd.read_csv(BytesIO(csv_content))
        logger.info(f"CSV file has {len(df)} rows")
        return df
        
    except s3_client.exceptions.NoSuchKey:
        logger.warning(f"CSV file not found: {key}")
        return None
    except Exception as e:
        logger.error(f"Error reading CSV file {key}: {str(e)}")
        raise

def upload_csv_to_s3(bucket, key, dataframe):
    """Upload DataFrame as CSV to S3"""
    try:
        logger.info(f"Uploading CSV to: {key}")
        
        # Convert DataFrame to CSV
        csv_buffer = BytesIO()
        dataframe.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        
        # Upload to S3
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=csv_buffer.getvalue(),
            ContentType='text/csv'
        )
        
        logger.info(f"Successfully uploaded {key}")
        
    except Exception as e:
        logger.error(f"Error uploading CSV {key}: {str(e)}")
        raise

def delete_file_from_s3(bucket, key):
    """Delete a file from S3"""
    try:
        s3_client.delete_object(Bucket=bucket, Key=key)
        logger.info(f"Deleted source file: {key}")
    except Exception as e:
        logger.error(f"Error deleting file {key}: {str(e)}")
        raise

def is_excel_file(filename):
    """Check if file is an Excel file"""
    return filename.lower().endswith(('.xlsx', '.xls'))

def is_csv_file(filename):
    """Check if file is a CSV file"""
    return filename.lower().endswith('.csv')

def process_subfolder_files(subfolder, files):
    """Process specific files for a subfolder with validation and file existence checks"""
    logger.info(f"Processing {len(files)} files for subfolder: {subfolder}")
    
    raw_zone_folder_prefix = f"{RAW_ZONE_PREFIX}{subfolder}/"
    
    # Separate Excel and CSV files
    excel_files = [f for f in files if is_excel_file(f)]
    csv_files = [f for f in files if is_csv_file(f)]
    
    logger.info(f"Found {len(excel_files)} Excel files and {len(csv_files)} CSV files")
    
    # Track successfully processed files for deletion
    successfully_processed_files = []
    files_not_found = []
    
    # Process Excel files - combine all Excel files
    combined_excel_df = pd.DataFrame()
    if excel_files:
        for excel_file in excel_files:
            try:
                df = read_excel_from_s3(BUCKET_NAME, excel_file)
                if df is not None:
                    combined_excel_df = pd.concat([combined_excel_df, df], ignore_index=True)
                    successfully_processed_files.append(excel_file)
                    logger.info(f"Processed Excel file: {excel_file}")
                else:
                    files_not_found.append(excel_file)
            except Exception as e:
                logger.error(f"Failed to process Excel file {excel_file}: {str(e)}")
                continue
    
    # Process CSV files - combine all CSV files
    combined_csv_df = pd.DataFrame()
    if csv_files:
        for csv_file in csv_files:
            try:
                df = read_csv_from_s3(BUCKET_NAME, csv_file)
                if df is not None:
                    combined_csv_df = pd.concat([combined_csv_df, df], ignore_index=True)
                    successfully_processed_files.append(csv_file)
                    logger.info(f"Processed CSV file: {csv_file}")
                else:
                    files_not_found.append(csv_file)
            except Exception as e:
                logger.error(f"Failed to process CSV file {csv_file}: {str(e)}")
                continue
    
    # Log files that were not found
    if files_not_found:
        logger.warning(f"Files not found (possibly already processed): {files_not_found}")
    
    # Combine all data
    final_df = pd.DataFrame()
    if not combined_excel_df.empty and not combined_csv_df.empty:
        # Both Excel and CSV files exist
        final_df = pd.concat([combined_excel_df, combined_csv_df], ignore_index=True)
        logger.info("Combined Excel and CSV data")
    elif not combined_excel_df.empty:
        # Only Excel files
        final_df = combined_excel_df
        logger.info("Using Excel data only")
    elif not combined_csv_df.empty:
        # Only CSV files
        final_df = combined_csv_df
        logger.info("Using CSV data only")
    
    if final_df.empty:
        logger.warning(f"No data to process for subfolder: {subfolder}")
        return {
            'files_processed': 0, 
            'rows_processed': 0,
            'files_not_found': len(files_not_found),
            'files_not_found_list': files_not_found,
            'reason': 'All files were either not found or contained no data'
        }
    
    # VALIDATION STEP - Check required columns
    is_valid, missing_columns = validate_columns(final_df, subfolder)
    
    if not is_valid:
        error_msg = f"Validation failed for {subfolder}. Missing columns: {missing_columns}"
        logger.error(error_msg)
        # Don't delete source files on validation failure
        raise ValueError(error_msg)
    
    # Upload final combined data only if validation passes
    try:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_key = f"{raw_zone_folder_prefix}{subfolder}_{timestamp}.csv"
        upload_csv_to_s3(BUCKET_NAME, output_key, final_df)
        logger.info(f"Successfully created: {output_key} with {len(final_df)} rows")
        
        # Delete source files only after successful upload and validation
        logger.info("Deleting source files after successful processing and validation...")
        deleted_files = 0
        for file_to_delete in successfully_processed_files:
            try:
                delete_file_from_s3(BUCKET_NAME, file_to_delete)
                deleted_files += 1
            except Exception as e:
                logger.error(f"Failed to delete {file_to_delete}: {str(e)}")
                continue
        
        result = {
            'files_processed': len(successfully_processed_files),
            'files_deleted': deleted_files,
            'files_not_found': len(files_not_found),
            'files_not_found_list': files_not_found,
            'rows_processed': len(final_df),
            'output_file': output_key,
            'subfolder': subfolder,
            'validation_passed': True,
            'required_columns_found': REQUIRED_COLUMNS.get(subfolder, [])
        }
        
        logger.info(f"Successfully processed {subfolder}: {deleted_files} files moved to raw-zone")
        return result
        
    except Exception as e:
        logger.error(f"Failed to upload combined CSV for {subfolder}: {str(e)}")
        logger.error("Source files will not be deleted due to upload failure")
        raise