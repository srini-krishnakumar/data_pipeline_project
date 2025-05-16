import random
import string
import avro.schema
import avro.io
from confluent_kafka import Producer
import io
import time


def generate_random_string(length=10):
    letters = string.ascii_letters  # a-z, A-Z
    return ''.join(random.choice(letters) for _ in range(length))


# on delivery to kafka topic you pretty print the output
def on_delivery(err, msg):
    """Delivery callback to check if the message was successfully delivered."""
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")


# writing to kafka
def write_to_kafka(avro_formatted_user_data):
    producer.produce(KAFKA_TOPIC, avro_formatted_user_data, callback=on_delivery)


# this utility method to serial a record into avro format

def serialize_avro(record, avro_schema):
    bytes_io = io.BytesIO()
    writer = avro.io.DatumWriter(avro_schema)
    encoder = avro.io.BinaryEncoder(bytes_io)
    writer.write(record, encoder)
    return bytes_io.getvalue()


# this is start of program

user_data_list = [
    {
        "user_id": "srini.k@gmail.com",
        "event_timestamp": int(time.time()),
        "event_type": "page_view",
        "product_id": None,
        "session_id": generate_random_string(),
        "event_properties": {
            "location": "US",
            "gender": "male",
            "price": "10"
        }
    },
    {
        "user_id": "srini123@gmail.com",
        "event_timestamp": int(time.time()),
        "event_type": "add_to_cart",
        "product_id": "ipad",
        "session_id": generate_random_string(),
        "event_properties": {
            "location": "US",
            "gender": "female",
            "price": "20"
        },
    {
        "user_id": "srini456@gmail.com",
        "event_timestamp": int(time.time()),
        "event_type": "purchase",
        "product_id": "ipad",
        "session_id": generate_random_string(),
        "event_properties": {
            "location": "US",
            "gender": "female",
            "price": "30"
        }        
    }
]

KAFKA_BROKER = 'localhost:9092'  # Change to your Kafka broker address
KAFKA_TOPIC = 'user_activity_avro_stream'  # Kafka topic

conf = {'bootstrap.servers': KAFKA_BROKER}

producer = Producer(conf)

# read section serializing
schema = avro.schema.parse(open("user_schema.avsc", "rb").read())

for user_data in user_data_list:
    avro_binary_data = serialize_avro(user_data, schema)
    print(avro_binary_data)
    write_to_kafka(avro_binary_data)
