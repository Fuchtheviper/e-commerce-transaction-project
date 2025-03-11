import json
from io import StringIO
import pandas as pd
from datetime import datetime
import boto3
import base64
import uuid

# Initialize S3 Client
s3 = boto3.client('s3')

# S3 Bucket and Prefix
BUCKET_NAME = "e-commerce-datalake"
S3_PREFIX = "streaming/batched_data/"

# Memory for batching
batch_data = {}
current_date = None

def lambda_handler(event, context):
    """
    Lambda function to get data from stream and grouping data by Transaction date
    to append in batch_data before save batch to s3
    """

    global batch_data, current_date

    # Process each record in the Kinesis stream
    for record in event["Records"]:
        try:
            # Decode base64 Kinesis data
            payload = base64.b64decode(record["kinesis"]["data"]).decode('utf-8')
            # Parse the JSON payload
            data = json.loads(payload)

            # Extract Transaction_Date from the payload
            transaction_date = data.get("Transaction_Date")
            if not transaction_date:
                print("Skipping record with missing Transaction_Date")
                continue

            # Convert to datetime.date for comparison
            transaction_date = pd.to_datetime(transaction_date).date()

            # Check if the date has changed
            if current_date is None:
                current_date = transaction_date

            if transaction_date != current_date:
                # Date has changed, save the previous batch to S3
                save_batch_to_s3(current_date, batch_data)
                # Reset for the new date
                batch_data = {}
                current_date = transaction_date

            # Append record to batch data
            if transaction_date not in batch_data:
                batch_data[transaction_date] = []
            batch_data[transaction_date].append(data)

        except Exception as e:
            print(f"Error processing record: {record}. Error: {e}")

    # Ensure any remaining records are saved at the end
    if batch_data:
        save_batch_to_s3(current_date, batch_data)
        batch_data = {}

def save_batch_to_s3(batch_date, batch_data):
    """
    Save batched data to S3 in a structured format.
    """
    if batch_date not in batch_data or not batch_data[batch_date]:
        print(f"No data to save for {batch_date}")
        return
    
    # Generate a unique ID for this batch
    unique_id = str(uuid.uuid4())

    # Convert batch to Pandas DataFrame
    df = pd.DataFrame(batch_data[batch_date])

    # S3 key structure: year=YYYY/month=MM/day=DD/
    s3_key = f"{S3_PREFIX}year={batch_date.year}/month={batch_date.month:02d}/day={batch_date.day:02d}/transactions_{batch_date.year}_{batch_date.month:02d}_{batch_date.day:02d}_{unique_id}.jsonl"

    # Convert batch data to JSON
    json_data = "\n".join(json.dumps(record) for record in batch_data[batch_date])

    # Upload JSON to S3
    try:
        s3.put_object(Bucket=BUCKET_NAME, Key=s3_key, Body=json_data, ContentType="application/json")
        print(f"Uploaded JSON batch for {batch_date} to S3: {s3_key}")
    except Exception as e:
        print(f"Error uploading to S3: {e}")