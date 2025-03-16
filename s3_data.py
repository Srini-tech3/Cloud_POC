from confluent_kafka import Producer
import json
import time
import boto3
import csv
import io

# Kafka configuration
conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(conf)

# Kafka topic
TOPIC = 's3_csv_data'

# S3 configuration
s3_client = boto3.client('s3')
S3_BUCKET_NAME = 'poc-data-2025'

# To keep track of processed files
processed_files = set()

def list_s3_files():
    try:
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET_NAME)
        if 'Contents' in response:
            return [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.csv')]  # Return only CSV files
        else:
            return []
    except Exception as e:
        print(f"Failed to list files from S3: {e}")
        return []

def fetch_s3_data(file_key):
    try:
        response = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=file_key)
        content = response['Body'].read().decode('utf-8')
        csv_reader = csv.DictReader(io.StringIO(content))
        return list(csv_reader)  # Return CSV data as a list of dictionaries
    except Exception as e:
        print(f"Failed to fetch data from S3: {e}")
        return None

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def produce_to_kafka():
    while True:
        # List all files in the bucket
        s3_files = list_s3_files()

        # Process files that haven't been processed yet
        new_files = [file for file in s3_files if file not in processed_files]

        for file_key in new_files:
            print(f"Processing new file: {file_key}")
            s3_data = fetch_s3_data(file_key)
            if s3_data:
                for record in s3_data:
                    producer.produce(TOPIC, json.dumps(record), callback=delivery_report)
                    producer.poll(0)
                producer.flush()

            # Mark the file as processed
            processed_files.add(file_key)

        time.sleep(60)  # Check for new files every 60 seconds

if __name__ == '__main__':
    produce_to_kafka()
