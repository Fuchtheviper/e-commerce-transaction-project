import boto3
import pandas as pd
import json
import time

class KinesisProducer:
    """
    Simulate stream to kinesis from sorted data
    """
    def __init__(self, stream_name, region):
        """
        :param stream_name: kinesis stream name
        :param region: kinesis region
        """
        self.client = boto3.client("kinesis", region_name = region)
        self.stream_name = stream_name

    def stream_data(self, file_path, delay):
        """
        :param file_path : path to source file for stream
        :param delay : timeing for stream data
        """
        stream_data = pd.read_csv(file_path)
        print("Streaming data to kinesis...")

        for _, row in stream_data.iterrows():
            record = row.to_dict()
            self.client.put_record(
                StreamName=self.stream_name,
                Data=json.dumps(record).encode('utf-8'),
                PartitionKey = str(record["Transaction_ID"])
            )
            print(f"Streamed: {record}")
            time.sleep(delay)

if __name__ == "__main__":
    # Initialize the handler with directories and dataset name
    kinesis_producer = KinesisProducer(
        stream_name="e-commerce-transaction-stream",
        region="us-east-1"
    )

    # Stream data from a CSV file to Kinesis
    kinesis_producer.stream_data(
        file_path="./data/prepared_data/sorted_transactions.csv",
        delay=0.1
    )
