# Install Docker desktop

* Start Docker desktop

# install Kafka

* docker pull apache/kafka:4.0.0
* docker run -p 9092:9092 apache/kafka:4.0.0

# Run event_producer.py

* Produces the records in avro format by taking the sample records and puts it in Kafka Topic

# Run archiver.py

* Reads the data from the above generated Kafka topic and writes the data to an S3 bucket.
* Script requires S3 credentials (aws_access_key_id, aws_secret_access_key) to connect to S3 and drop the data file.

# user_activity_etl.py

* This is the AWS Glue ETL job that picks up the json data file and does the following transformations 
    1. Converts a unix-millisecods to a date timestamp
    2. Case statement to find price if the event_type='Purchase'
    3. Writes the data back to an S3 bucket.


# Lambda function to call the above AWS Glue script

1. We can build a Lambda function to call the AWS glue job to do the ETL transfrormation
2. Lambda function provides triggers to make sure Glue job can be run only when new files arrive in the S3 bucket
3. Lambda function will need an IAM role assigned to it with appropriate READ/WRITE access to S3 buckets and AWS console access
4. We can also add some pre-checks in the lambda function to make sure the files are authentic and complete and ready to be processed
