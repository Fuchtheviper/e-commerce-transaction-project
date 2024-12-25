import os
import subprocess
import pandas as pd

class KaggleDataHandler:
    """
    Download dataset from kaggle then transform into dataframe and sort oldest to newest to target directory
    """
    def __init__(self, dataset_name, raw_data_dir, prepared_data_dir):
        """
        Constructor for kaggle api

        :param dataset_name: kaggle dataset name
        :param raw_data_dir: directory to save raw data from kaggle
        :param prepared_data_dir: directory to save sorted data
        """
        self.dataset_name = dataset_name
        self.raw_data_dir = raw_data_dir
        self.prepared_data_dir = prepared_data_dir
        self.df = None

        # Ensure directories exist
        os.makedirs(self.raw_data_dir, exist_ok=True)
        os.makedirs(self.prepared_data_dir, exist_ok=True)

    def download_dataset(self):
        """Download dataset from Kaggle."""
        print("Downloading dataset...")
        subprocess.run([
            "kaggle", "datasets", "download", "-d", self.dataset_name,
            "-p", self.raw_data_dir, "--unzip"
        ])
        print("Dataset downloaded successfully!")

    def load_data(self):
        """Load the first CSV file into a DataFrame."""
        # List all CSV files in the raw data directory
        csv_files = [file for file in os.listdir(self.raw_data_dir) if file.endswith('.csv')]
        if not csv_files:
            raise FileNotFoundError("No CSV files found in the directory!")

        file_path = os.path.join(self.raw_data_dir, csv_files[0])
        self.df = pd.read_csv(file_path)
        print(f"Data loaded from {file_path}")

        # Ensure 'Transaction_Date' is in datetime format
        self.df['Transaction_Date'] = pd.to_datetime(self.df['Transaction_Date'])
        print("Transaction_Date column converted to datetime format.")

    def sort_and_save(self):
        """Sort the data by date from oldest to latest and save as a single CSV."""
        print("Sorting data by date...")
        
        # Sort the DataFrame by Transaction_Date
        sorted_df = self.df.sort_values(by="Transaction_Date")
        
        # Save the sorted DataFrame to a single CSV file
        output_path = os.path.join(self.prepared_data_dir, "sorted_transactions.csv")
        sorted_df.to_csv(output_path, index=False)
        print(f"Data sorted and saved locally to {output_path}")

    def process(self):
        """Complete pipeline: download, load, split, and save data."""
        self.download_dataset()
        self.load_data()
        self.sort_and_save()

# Main script to use the class
if __name__ == "__main__":
    # Initialize the handler with directories and dataset name
    dataset_handler = KaggleDataHandler(
        dataset_name="imranalishahh/comprehensive-synthetic-e-commerce-dataset",
        raw_data_dir="./data/raw_data",
        prepared_data_dir="./data/prepared_data"
    )

    # Execute the pipeline
    dataset_handler.process()