from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from io import StringIO
import logging
import pandas as pd
from airflow.utils.dates import days_ago
import os

# Default arguments
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

# File to store the last processed timestamp
LAST_TIMESTAMP_FILE = '/tmp/mysql_last_timestamp.txt'

# Define DAG
dag = DAG(
    'mysql_to_s3_export_with_timestamp',
    default_args=default_args,
    schedule_interval='*/2 * * * *',  # Runs every 2 minutes
    catchup=False,
)

# Task 1: Check for new rows based on the timestamp
def check_new_rows(**context):
    mysql_hook = MySqlHook(mysql_conn_id='MYSQL_Connection')

    # Read last processed timestamp from file
    if os.path.exists(LAST_TIMESTAMP_FILE):
        with open(LAST_TIMESTAMP_FILE, 'r') as file:
            last_timestamp = file.read().strip()
    else:
        # If no file exists, consider a default (old) timestamp or load the earliest data
        last_timestamp = '1970-01-01 00:00:00'

    logging.info(f"Last processed timestamp: {last_timestamp}")
    
    # Fetch the count of new rows added after the last timestamp
    query = f"SELECT COUNT(*) FROM employee_data WHERE timestamp > '{last_timestamp}';"
    result = mysql_hook.get_first(query)
    new_row_count = result[0] if result else 0

    logging.info(f"New rows found: {new_row_count}")

    # Return True if there are new rows, otherwise False
    if new_row_count > 0:
        return True
    else:
        return False

check_new_rows_task = PythonOperator(
    task_id='check_new_rows',
    python_callable=check_new_rows,
    provide_context=True,
    dag=dag,
)

# Task 2: Export new data to AWS S3
def export_new_data_to_s3(**context):
    if context['ti'].xcom_pull(task_ids='check_new_rows'):
        mysql_hook = MySqlHook(mysql_conn_id='MYSQL_Connection')
        s3_hook = S3Hook(aws_conn_id='AWS_Connection')

        # Read last processed timestamp from file
        if os.path.exists(LAST_TIMESTAMP_FILE):
            with open(LAST_TIMESTAMP_FILE, 'r') as file:
                last_timestamp = file.read().strip()
        else:
            last_timestamp = '1970-01-01 00:00:00'

        # Fetch new data based on the timestamp
        query = f"SELECT * FROM employee_data WHERE timestamp > '{last_timestamp}';"
        new_records = mysql_hook.get_pandas_df(query)
        
        if new_records.empty:
            logging.info("⚠️ No new data found to export!")
            return

        # Convert data to CSV
        csv_buffer = StringIO()
        new_records.to_csv(csv_buffer, index=False)
        csv_data = csv_buffer.getvalue()

        # Generate timestamped filename
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        s3_bucket = "poc-data-2025"
        s3_key = f"employee_data_{timestamp}.csv"

        # Upload CSV to S3
        s3_hook.load_string(
            string_data=csv_data,
            key=s3_key,
            bucket_name=s3_bucket,
            replace=True
        )

        logging.info(f"✅ New data successfully exported to S3: s3://{s3_bucket}/{s3_key}")

        # Get the latest timestamp from the new records and store it for future runs
        max_timestamp = new_records['timestamp'].max()
        with open(LAST_TIMESTAMP_FILE, 'w') as file:
            file.write(str(max_timestamp))
        
        logging.info(f"✅ Updated last processed timestamp: {max_timestamp}")
    else:
        logging.info("⚠️ No new rows found. Skipping export.")

export_new_data_to_s3_task = PythonOperator(
    task_id='export_new_data_to_s3',
    python_callable=export_new_data_to_s3,
    provide_context=True,
    dag=dag,
)

# DAG Flow
check_new_rows_task >> export_new_data_to_s3_task
