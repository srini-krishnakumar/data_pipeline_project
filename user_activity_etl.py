import sys
from datetime import datetime
# AWS Glue specific imports
from awsglue.transforms import *  # Import all Glue transformations
from awsglue.utils import getResolvedOptions  # For getting job parameters
from pyspark.context import SparkContext  # Base Spark context
from awsglue.context import GlueContext  # Glue-specific context
from awsglue.job import Job  # Glue job wrapper
from awsglue.dynamicframe import DynamicFrame  # Glue's enhanced DataFrame

# Import required Spark SQL functions for data transformation
from pyspark.sql.functions import (
    from_unixtime,  # Convert Unix timestamp to datetime
    current_timestamp,  # Get current timestamp
    when,  # Conditional statements
    col,  # Column reference
    from_json,  # Parse JSON strings
    lit,  # Create literal values
    to_timestamp  # Convert string to timestamp
)
from pyspark.sql.types import StringType, FloatType  # Spark SQL data types

# Step 1: Initialize Spark and Glue contexts
# SparkContext is the entry point for Spark functionality
sc = SparkContext()
# GlueContext provides additional ETL capabilities on top of Spark
glueContext = GlueContext(sc)
# Get the SparkSession, which is the entry point for DataFrame operations
spark = glueContext.spark_session
# Create a Glue job instance
job = Job(glueContext)

# Step 2: Get job parameters passed from AWS Glue console or AWS CLI
# These parameters should be specified when creating/running the Glue job
args = getResolvedOptions(sys.argv, [
    'data_pipeline_job',  # Required by Glue
    'source_bucket',  # S3 bucket containing raw data
    'target_bucket'  # S3 bucket for processed data
])
job.init(args['data_pipeline_job'], args)

# Define S3 paths for source and target data
# The paths follow a hierarchical structure for better organization
source_path = f"s3://{args['source_bucket']}/raw_events/user_activity/"
target_path = f"s3://{args['target_bucket']}/processed_events/user_activity/"

def process_batch(dynamic_frame):
    """
    Process a batch of records from the source data.
    
    Args:
        dynamic_frame (DynamicFrame): Input DynamicFrame containing raw event data
    
    Returns:
        DynamicFrame: Processed data with transformed columns
    
    Transformations performed:
    1. Convert Unix timestamp to ISO format
    2. Add processing timestamp and date
    3. Extract price from event_properties for purchase events
    4. Select and rename required fields
    """
    # Convert DynamicFrame to DataFrame for more flexible transformations
    # DynamicFrames are Glue's extension of Spark DataFrames with additional capabilities
    df = dynamic_frame.toDF()
    
    # Transform event_timestamp from Unix milliseconds to ISO format string
    # Divide by 1000 to convert milliseconds to seconds
    df = df.withColumn(
        'event_timestamp_iso',
        from_unixtime(col('event_timestamp') / 1000).cast('string')
    )
    
    # Add processing timestamp for tracking when the record was processed
    df = df.withColumn('processing_timestamp', current_timestamp())
    
    # Extract date from processing timestamp for partitioning
    df = df.withColumn(
        'processing_date',
        to_timestamp(col('processing_timestamp')).cast('date')
    )
    
    # Extract price from event_properties for purchase events
    # For non-purchase events, price will be null
    df = df.withColumn(
        'price',
        when(
            col('event_type') == 'purchase',  # Condition
            col('event_properties.price').cast(FloatType())  # If true
        ).otherwise(None)  # If false
    )
    
    # Select final columns for output
    # This also serves as a way to enforce schema and remove unused columns
    result_df = df.select(
        'user_id',
        'event_timestamp_iso',
        'event_type',
        'product_id',
        'price',
        'processing_timestamp',
        'processing_date'
    )
    
    # Convert back to DynamicFrame for Glue-specific optimizations
    return DynamicFrame.fromDF(result_df, glueContext, "processed_records")

try:
    # Step 3: Read source data from S3
    # Create a DynamicFrame from the source Avro files
    source_dyf = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",  # Reading from S3
        connection_options={
            "paths": [source_path],  # S3 path to read from
            "recurse": True  # Read from all subdirectories
        },
        format="avro"  # Source data format
    )
    
    # Step 4: Process the data using our transformation function
    processed_dyf = process_batch(source_dyf)
    
    # Step 5: Write the processed data to S3
    # Save as Parquet files, partitioned by event_type and processing_date
    glueContext.write_dynamic_frame.from_options(
        frame=processed_dyf,
        connection_type="s3",  # Writing to S3
        connection_options={
            "path": target_path,
            "partitionKeys": ["event_type", "processing_date"]  # Partition columns
        },
        format="parquet"  # Target data format (columnar storage for better query performance)
    )
    
except Exception as e:
    # Log any errors that occur during processing
    print(f"Error processing data: {str(e)}")
    # In production, you would want to:
    # 1. Send notifications (e.g., SNS)
    # 2. Log to CloudWatch
    # 3. Handle specific types of exceptions differently
    raise  # Re-raise the exception to mark the job as failed

# Step 6: Commit the job
# This marks the job as successful and updates job metrics
job.commit() 