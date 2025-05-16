import boto3
import json
import datetime
from confluent_kafka import Consumer, KafkaException, KafkaError

"""
Initialize consumer line 10 to line 22
Initialize aws session for s3 line 27 to 33
Read data from kafka topic line 36 to 76
line 61 we are actually writing the kafka data to s3
"""

KAFKA_BROKER = 'localhost:9092'  # Change to your Kafka broker address
KAFKA_TOPIC = ['user_activity_avro_stream']  # Kafka topic

# Kafka configuration
conf = {
    'bootstrap.servers': KAFKA_BROKER,  # Kafka broker address
    'group.id': 'user-data-groups',        # Consumer group ID
    'auto.offset.reset': 'latest'
}

consumer = Consumer(conf)

consumer.subscribe(KAFKA_TOPIC)  # Subscribe to topic

# get the key id and access key from AWS administrators for a system account
# or use personal AWS access key and secret key
session = boto3.Session(
    aws_access_key_id='XXX',
    aws_secret_access_key='XXX',
    region_name='us-west-2'
)

s3 = session.client('s3')

# read every record from topic
try:
    while True:
        # Poll for a message
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            # No message received within the timeout
            continue
        if msg.error():
            # Handle error
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f"End of partition reached: {msg.topic()} [{msg.partition}] at offset {msg.offset()}")
            else:
                raise KafkaException(msg.error())
        else:
            # Successfully received a message

            message_value = msg.value().decode('utf-8')
            data = json.loads(message_value)

            # Convert the data back to a string to store in S3 (this can be customized)
            # Update the format to hive
            event_date=datetime.now()
            formatted = event_date.strftime("%Y-%m-%d/hour=%H/")
            s3_key = f'raw_events/user_activity/{formatted}kafka_messages/{msg.partition}/{msg.offset()}.json'

            # Upload to S3
            s3.put_object(
                Bucket="some_bucket",
                Key=s3_key,
                Body=json.dumps(data),  # Store the message as a JSON file in S3
                ContentType='application/json'
            )

            # Manually commit the offset after processing the message
            consumer.commit(msg)

except KeyboardInterrupt:
    print("Consumer interrupted")

finally:
    # Close the consumer
    consumer.close()







