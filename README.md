Data Pipeline Setup with MySQL, EC2, Kafka, Spark, and DynamoDB
This guide will walk you through the process of setting up a data pipeline using AWS EC2, MySQL, Kafka, Spark, and DynamoDB. Additionally, it automates the installation of necessary applications and the migration of data.

Table of Contents
MySQL Installation and Setup
AWS EC2 Launch Template
Create AWS DynamoDB Table
Docker Installation and Airflow Configuration
Data Upload to MySQL & Migration Workflow

1. MySQL Installation and Setup
2. AWS EC2 Launch Template
    Create an EC2 launch template and include the following shell script to automate the installation of Kafka, Spark, and Python/PySpark script execution.
     <<EC2_Shell.sh>> <<s3_data.py>><<spark_kafka_streaming_s3.py>>
   
3. Create AWS DynamoDB Table
To create a DynamoDB table:
Go to the DynamoDB Console.
Click on Create Table.
Enter the table name and primary key details:
Table Name: YourTableName
Partition Key: id (Type: Number)
Configure other settings as needed and click Create.

4. Docker Installation and Airflow Configuration
   <<Airflow setup on docker (Windows)..docx>>
   <<docker-compose.YAML>>
   <<EC2_Launch.py>>
   <<mysql_to_s3_v1.py>>

5. Data Upload to MySQL & Migration Workflow

Migration Workflow:
Once all components (Kafka, Spark, DynamoDB) are running, the migration process starts. 
Follow these steps:
Kafka Producer sends data to Kafka Topic.
PySpark consumes the data from Kafka and processes it.
Processed data is inserted into the DynamoDB table.
Airflow DAG orchestrates the entire pipeline and automates the workflow.

Repository Structure:
├── dags
│   └── my_airflow_dag.py
├── scripts
│   ├── your_kafka_producer.py
│   └── your_pyspark_consumer.py
├── Dockerfile
├── docker-compose.yml
└── README.md

Conclusion
This pipeline allows you to automate the process of data extraction, transformation, and loading (ETL) from MySQL to DynamoDB using AWS EC2, Kafka, PySpark, and Airflow. 
For further improvements, you can extend this setup to include real-time monitoring and alerting.
