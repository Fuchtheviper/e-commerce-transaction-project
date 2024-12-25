import os
from local_scripts.kaggle_data_handler import KaggleDataHandler
from local_scripts.kinesis_producer import KinesisProducer

def main():
    """
    Orchestrates the entire data pipeline:
    1. Download and prepare data from Kaggle.
    2. Stream data to AWS Kinesis.
    """
    # Define directories and dataset details
    dataset_name = "imranalishahh/comprehensive-synthetic-e-commerce-dataset"
    raw_data_dir = "./data/raw_data"
    prepared_data_dir = "./data/prepared_data"
    sorted_file_path = os.path.join(prepared_data_dir, "sorted_transactions.csv")

    # Step 1: Download and Prepare Data
    dataset_handler = KaggleDataHandler(
        dataset_name=dataset_name,
        raw_data_dir=raw_data_dir,
        prepared_data_dir=prepared_data_dir
    )
    print("Starting data preparation...")
    dataset_handler.process()

    # Step 2: Stream Data to Kinesis
    kinesis_stream_name = "e-commerce-transaction-stream"
    kinesis_region = "us-east-1"
    delay = 0.05  # Delay between records

    kinesis_producer = KinesisProducer(
        stream_name=kinesis_stream_name,
        region=kinesis_region
    )
    print("Starting data streaming to Kinesis...")
    kinesis_producer.stream_data(file_path=sorted_file_path, delay=delay)

    print("Data pipeline completed successfully!")

if __name__ == "__main__":
    main()