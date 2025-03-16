#!/bin/bash
# Update the package list
sudo apt update -y

# Install unzip
sudo apt install unzip -y

# Install OpenJDK 11
sudo apt install openjdk-11-jdk -y

# Set Java environment variables and KAFKA_HEAP_OPTS directly in the script
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
export KAFKA_HEAP_OPTS="-Xmx256M -Xms128M"

# Set AWS credentials directly in the script (Consider using IAM roles or environment variables in a secure way)
export AWS_ACCESS_KEY_ID=<your key ID>
export AWS_SECRET_ACCESS_KEY=<your key>
export AWS_DEFAULT_REGION=ap-south-1

# Download and extract Kafka into /home/ubuntu/kafka_spark_aws
wget -P /home/ubuntu/kafka_spark_aws https://dlcdn.apache.org/kafka/3.7.2/kafka_2.12-3.7.2.tgz
tar -xvf /home/ubuntu/kafka_spark_aws/kafka_2.12-3.7.2.tgz -C /home/ubuntu/kafka_spark_aws
rm /home/ubuntu/kafka_spark_aws/kafka_2.12-3.7.2.tgz

# Download and extract Spark into /home/ubuntu/kafka_spark_aws
wget -P /home/ubuntu/kafka_spark_aws https://dlcdn.apache.org/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz
tar -xvf /home/ubuntu/kafka_spark_aws/spark-3.5.5-bin-hadoop3.tgz -C /home/ubuntu/kafka_spark_aws
rm /home/ubuntu/kafka_spark_aws/spark-3.5.5-bin-hadoop3.tgz

# Install AWS CLI
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "/home/ubuntu/kafka_spark_aws/awscliv2.zip"
unzip /home/ubuntu/kafka_spark_aws/awscliv2.zip -d /home/ubuntu/kafka_spark_aws/
sudo /home/ubuntu/kafka_spark_aws/aws/install
rm /home/ubuntu/kafka_spark_aws/awscliv2.zip

# Automatically configure AWS CLI with provided environment variables
aws configure set aws_access_key_id "$AWS_ACCESS_KEY_ID"
aws configure set aws_secret_access_key "$AWS_SECRET_ACCESS_KEY"
aws configure set default.region "$AWS_DEFAULT_REGION"
aws configure set output json

# Configure Spark environment
cp /home/ubuntu/kafka_spark_aws/spark-3.5.5-bin-hadoop3/conf/spark-env.sh.template /home/ubuntu/kafka_spark_aws/spark-3.5.5-bin-hadoop3/conf/spark-env.sh
echo "export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64" >> /home/ubuntu/kafka_spark_aws/spark-3.5.5-bin-hadoop3/conf/spark-env.sh

# Configure Spark defaults
cp /home/ubuntu/kafka_spark_aws/spark-3.5.5-bin-hadoop3/conf/spark-defaults.conf.template /home/ubuntu/kafka_spark_aws/spark-3.5.5-bin-hadoop3/conf/spark-defaults.conf
echo "spark.jars.packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5" >> /home/ubuntu/kafka_spark_aws/spark-3.5.5-bin-hadoop3/conf/spark-defaults.conf

# Configure Kafka
echo "listeners = PLAINTEXT://localhost:9092" >> /home/ubuntu/kafka_spark_aws/kafka_2.12-3.7.2/config/server.properties
echo "advertised.listeners = PLAINTEXT://localhost:9092" >> /home/ubuntu/kafka_spark_aws/kafka_2.12-3.7.2/config/server.properties

# Generate SSH key (Consider security implications)
ssh-keygen -t rsa -b 4096 -C "srinivas.sk.vm@gmail.com" -N "" -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 700 ~/.ssh
chmod 600 ~/.ssh/authorized_keys

# Install and configure Python virtual environment
sudo apt install python3-venv -y
python3 -m venv /home/ubuntu/myenv
source /home/ubuntu/myenv/bin/activate
pip install boto3 kafka-python confluent_kafka
deactivate

# Create scripts folder
mkdir -p /home/ubuntu/logs && chmod 777 /home/ubuntu/logs


# Start Zookeeper and Kafka
nohup /home/ubuntu/kafka_spark_aws/kafka_2.12-3.7.2/bin/zookeeper-server-start.sh /home/ubuntu/kafka_spark_aws/kafka_2.12-3.7.2/config/zookeeper.properties >> /home/ubuntu/logs/zookeeper_nohup.out 2>&1 &
sleep 20
nohup /home/ubuntu/kafka_spark_aws/kafka_2.12-3.7.2/bin/kafka-server-start.sh /home/ubuntu/kafka_spark_aws/kafka_2.12-3.7.2/config/server.properties >> /home/ubuntu/logs/kafka_nohup.out 2>&1 &
sleep 20

# Create Kafka topic
/home/ubuntu/kafka_spark_aws/kafka_2.12-3.7.2/bin/kafka-topics.sh --create --topic s3_csv_data --bootstrap-server localhost:9092 --replication-factor 1 --partitions 4

# Start Spark
/home/ubuntu/kafka_spark_aws/spark-3.5.5-bin-hadoop3/sbin/start-all.sh

# Create scripts folder
mkdir -p /home/ubuntu/scripts && chmod 777 /home/ubuntu/scripts

# Write the Python script to a file
cat << 'EOF' > /home/ubuntu/scripts/s3_data.py
#!/usr/bin/python3
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

EOF

# Write the Python script to a file
cat << 'EOF' > /home/ubuntu/scripts/spark_kafka_streaming.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import boto3

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaS3StreamToDynamoDB") \
    .getOrCreate()

# Define schema for the incoming Kafka data (based on your CSV structure)
schema = StructType([
    StructField('id', StringType(), True),
    StructField('name', StringType(), True),
    StructField('age', StringType(), True),
    StructField('gender', StringType(), True),
    StructField('department', StringType(), True),
    StructField('position', StringType(), True),
    StructField('salary', StringType(), True),
    StructField('joining_date', StringType(), True),
    StructField('experience_years', StringType(), True)
])

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "s3_csv_data") \
    .option("startingOffsets", "latest") \
    .load()

# Deserialize Kafka data (convert from binary to string and parse as JSON)
kafka_df = df.selectExpr("CAST(value AS STRING) as json_str")

# Convert joining_date from string to DateType
parsed_df = kafka_df.select(from_json(col("json_str"), schema).alias("data")).select("data.*")

parsed_df = parsed_df.withColumn("joining_date", to_date(col("joining_date"), "yyyy-MM-dd"))

# Apply transformations: Drop duplicates and fill missing values
transformed_df = parsed_df.dropDuplicates(subset=['id'])

# Write to DynamoDB
def write_to_dynamodb(batch_df, epoch_id):
    dynamodb = boto3.resource('dynamodb', region_name='ap-south-1')
    table = dynamodb.Table('emp_details')

    for row in batch_df.collect():
        table.put_item(
            Item={
                'id': int(row['id']),
                'name': row['name'],
                'age': int(row['age']),
                'gender': row['gender'],
                'department': row['department'],
                'position': row['position'],
                'salary': int(row['salary']),
                'joining_date': str(row['joining_date']),
                'experience_years': int(row['experience_years'])
            }
        )
    print(f"Batch {epoch_id} inserted into DynamoDB.")

# Start streaming query with transformations
query = transformed_df.writeStream \
    .foreachBatch(write_to_dynamodb) \
    .outputMode("update") \
    .start()

query.awaitTermination()

EOF

chmod +x /home/ubuntu/scripts/*

# Activate the virtual environment and run Python and Spark scripts in the background
source /home/ubuntu/myenv/bin/activate
nohup python /home/ubuntu/scripts/s3_data.py >> /home/ubuntu/logs/s3_data_nohup.out 2>&1 &
nohup /home/ubuntu/kafka_spark_aws/spark-3.5.5-bin-hadoop3/bin/spark-submit /home/ubuntu/scripts/spark_kafka_streaming.py >> /home/ubuntu/logs/spark_kafka_streaming_nohup.out 2>&1 &

echo "Producer and Spark consumer scripts are running in the background."
 
