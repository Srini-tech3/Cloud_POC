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
