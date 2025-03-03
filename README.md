# E-Commerce_transaction
E-Commerce Transaction Data Pipeline
This project implements an end-to-end data pipeline for e-commerce transaction data. The pipeline downloads data from Kaggle, processes and sorts transactions, and streams the data to AWS Kinesis for real-time analytics.

You can find more information in : https://regular-literature-569.notion.site/End-to-End-Data-Pipeline-for-E-Commerce-Analytics-1661b0d3d7f080fdaa53f7dd9607e151

📌 Table of Contents
    - Project Overview
    - Requirements
    - Setup
    - How to Run
    - File Descriptions
    - Outputs
    - Notes
    - Future Enhancements
📌 Project Overview
"This project focuses on automating data collection, preprocessing, and real-time streaming of e-commerce transactions."
The pipeline follows these key steps:
    - Download synthetic e-commerce dataset from Kaggle.
    - Process and sort transaction data from oldest to newest.
    - Stream sorted transactions to AWS Kinesis for further analytics.

🔹 Technologies Used:
    - Kaggle API for dataset retrieval
    - Pandas for data preprocessing
    - Boto3 for AWS integration
    Cloud platform
    - AWS Kinesis for real-time streaming
    - AWS S3 for storage data
    - AWS Glue for ETL
    - AWS Athena for data query

📌 Requirements
    ✅ Python 3.9 or higher
    ✅ AWS CLI configured with IAM permissions
    ✅ Kaggle API credentials (kaggle.json)
    ✅ Dependencies installed via requirements.txt

📌 Setup
1. Clone this repository
2. Install dependencies:
    pip install -r requirements.txt
3. Configure Kaggle API:
    Download your kaggle.json from Kaggle Account Settings.
    Place it in ~/.kaggle/ (Linux/Mac) or %USERPROFILE%/.kaggle/ (Windows).
4. Configure AWS Credentials:
    Ensure your AWS CLI is configured with IAM credentials
5. Setup AWS service such as Kinesis, S3, Glue and Athena

📌 How to Run
Step 0: Setup Kinesis, S3 Glue and Athena on AWS console (This section you can find how to create on the internet)
    - Copy code in aws_console_scripts 
Step 1: Run the Full Pipeline
The main.py script orchestrates the full workflow:
    - Downloads dataset from Kaggle
    - Prepares and sorts data
    - Streams transactions to AWS Kinesis
To execute the pipeline, run:
    python main.py

Expected Output:
    - Sorted dataset saved at data/prepared_data/sorted_transactions.csv
    - Streaming transactions to AWS Kinesis
    - Logs printed for each streamed record

📌 File Descriptions
1. Data Folder
    - data/raw_data/ → Stores the downloaded dataset from Kaggle.
    - data/prepared_data/ → Stores processed & sorted transaction data.

2. Local Scripts
    - local_scripts/kaggle_data_handler.py
    - Automates dataset download, sorting, and saving.
    - local_scripts/kinesis_producer.py
    - Streams transactions to AWS Kinesis.

3. Configuration
    - config/config.yaml → YAML file for storing pipeline configurations.

4. Other Files
    - requirements.txt → Python dependencies.
    - .gitignore → Prevents sensitive files from being committed.
    - README.md → Documentation for this project.

📌 Outputs
    ✅ Sorted dataset saved in data/prepared_data/sorted_transactions.csv
    ✅ Live streaming transactions to AWS Kinesis
    ✅ Console logs showing each streamed record

📌 Notes
Ensure you have valid AWS credentials before running the Kinesis streaming step.
Modify dataset_name and stream_name in main.py for different datasets or AWS services.
Adjust the delay in kinesis_producer.py to control streaming speed.