import boto3
import logging

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    # Create an SSM client
    ssm_client = boto3.client('ssm')
    
    # Define your EC2 instance ID
    instance_id = 'i-064c03487799a4847'  # Replace with your EC2 instance ID
    
    # Define the log folder path
    log_folder = "/home/ubuntu/logs"
    log_file = f"{log_folder}/lambda_kafka_logs.out"
    
    # Commands to execute
    commands = [
        # Step 0: Create the log folder if it doesn't exist
        f"mkdir -p {log_folder}",
        
        # Step 1: Start Zookeeper
        f"echo 'Starting Zookeeper...' >> {log_file}",
        f"nohup /home/ubuntu/kafka_2.12-3.7.2/bin/zookeeper-server-start.sh /home/ubuntu/kafka_2.12-3.7.2/config/zookeeper.properties >> {log_file} 2>&1 &",
        "sleep 20",  # Wait for Zookeeper to be up

        # Step 2: Start Kafka with retry mechanism
        f"echo 'Starting Kafka...' >> {log_file}",
        f"for i in {{1..3}}; do nohup /home/ubuntu/kafka_2.12-3.7.2/bin/kafka-server-start.sh /home/ubuntu/kafka_2.12-3.7.2/config/server.properties >> {log_file} 2>&1 && break || sleep 10; done",
        "sleep 20",  # Wait for Kafka to be up

        # Step 3: Create Kafka topic
        f"echo 'Creating Kafka topic s3_csv_data...' >> {log_file}",
        f"/home/ubuntu/kafka_2.12-3.7.2/bin/kafka-topics.sh --create --topic s3_csv_data --bootstrap-server localhost:9092 --replication-factor 1 --partitions 4 >> {log_file} 2>&1",
        
        # Step 4: Start Spark after Kafka is up
        f"echo 'Starting Spark services...' >> {log_file}",
        f"/home/ubuntu/spark-3.5.5-bin-hadoop3/sbin/start-all.sh >> {log_file} 2>&1",
        "sleep 20",  # Wait for Spark services to be up
        
        # Step 5: Activate virtual environment
        f"echo 'Activating virtual environment...' >> {log_file}",
        f". /home/ubuntu/myenv/bin/activate >> {log_file} 2>&1",
        
        # Step 6: Run both s3_data.py and spark_kafka_streaming_s3.py in parallel
        f"echo 'Running s3_data.py and Spark streaming in parallel...' >> {log_file}",
        f"python /home/ubuntu/scripts/s3_data.py >> {log_file} 2>&1 &",  # Run in background
        f"/home/ubuntu/spark-3.5.5-bin-hadoop3/bin/spark-submit /home/ubuntu/scripts/spark_kafka_streaming_s3.py >> {log_file} 2>&1 &"  # Run in background
    ]

    # Log the command being executed
    logger.info(f"Sending commands to EC2 instance {instance_id}")

    # Send the command using SSM Run Command
    try:
        response = ssm_client.send_command(
            InstanceIds=[instance_id],
            DocumentName="AWS-RunShellScript",
            Parameters={'commands': commands},
        )
        
        # Log the Command ID for further tracking
        command_id = response['Command']['CommandId']
        logger.info(f"Successfully triggered commands. Command ID: {command_id}")
        
        return {
            'statusCode': 200,
            'body': f'Successfully triggered commands. Command ID: {command_id}'
        }
    
    except Exception as e:
        logger.error(f"Failed to execute commands: {str(e)}")
        return {
            'statusCode': 500,
            'body': f'Error triggering commands: {str(e)}'
        }
