from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
import boto3

# Define AWS region
AWS_REGION = 'ap-south-1'  # Change to your desired AWS region

# Define the Launch Template ID
LAUNCH_TEMPLATE_ID = 'lt-020034eb65fbdea4a'  # Replace with your actual Launch Template ID

# Define the name for the EC2 instance
EC2_INSTANCE_NAME = 'Kafka-Spark-Server'  # Replace with your desired instance name

# Define a function to launch EC2 using the Launch Template and Airflow's AWS connection
def launch_ec2_instance():
    # Use Airflow's AWS connection
    aws_hook = AwsBaseHook(aws_conn_id='AWS_Connection', client_type='ec2')  # Ensure 'AWS_Connection' matches your AWS connection in Airflow

    # Get the session from the Airflow hook and set the region explicitly
    session = aws_hook.get_session()

    # Create EC2 client with the region explicitly passed
    ec2_client = session.client('ec2', region_name=AWS_REGION)

    # Launch an EC2 instance using the launch template and set a name tag
    response = ec2_client.run_instances(
        LaunchTemplate={
            'LaunchTemplateId': LAUNCH_TEMPLATE_ID,
            'Version': '$Latest'  # You can specify the version here, or use $Latest
        },
        MinCount=1,  # Minimum number of instances to launch
        MaxCount=1,  # Maximum number of instances to launch
        TagSpecifications=[
            {
                'ResourceType': 'instance',  # Tag the EC2 instance
                'Tags': [
                    {
                        'Key': 'Name',
                        'Value': EC2_INSTANCE_NAME  # Set the Name tag for the EC2 instance
                    }
                ]
            }
        ]
    )

    # Get instance details
    instance_id = response['Instances'][0]['InstanceId']
    print(f"✅ EC2 Instance with ID {instance_id} has been launched with Name: {EC2_INSTANCE_NAME}.")
    return instance_id

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 16),  # Replace with your DAG start date
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'launch_ec2_instance_dag',
    default_args=default_args,
    description='A simple DAG to launch EC2 using Launch Template ID with a Name tag',
    schedule_interval=None,  # You can define a schedule or trigger manually
    catchup=False,
) as dag:

    # Define the PythonOperator task to launch EC2
    launch_ec2_task = PythonOperator(
        task_id='launch_ec2',
        python_callable=launch_ec2_instance,
    )

    # Set the task in the DAG
    launch_ec2_task
