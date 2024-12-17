import os
import subprocess
import pandas as pd

class KaggleDataHandler:
    def __init__(self, dataset_name, raw_data_dir, chunk_data_dir):
        self.dataset_name = dataset_name
        self.raw_data_dir = raw_data_dir
        self.chunk_data_dir = chunk_data_dir
        self.df = None

        # Ensure directories exist
        os.makedirs(self.raw_data_dir, exist_ok=True)
        os.makedirs(self.chunk_data_dir, exist_ok=True)

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

    def split_and_save_chunks(self):
        """Split the data by year and month and save as separate CSV files."""
        print("Splitting data into chunks...")
        for (year, month), group in self.df.groupby(
            [self.df['Transaction_Date'].dt.year,
             self.df['Transaction_Date'].dt.month]):
            filename = f"transactions_{year}_{month:02d}.csv"
            output_path = os.path.join(self.chunk_data_dir, filename)
            group.to_csv(output_path, index=False)
            print(f"Saved {len(group)} rows to {output_path}")

    def process(self):
        """Complete pipeline: download, load, split, and save data."""
        self.download_dataset()
        self.load_data()
        self.split_and_save_chunks()

# Main script to use the class
if __name__ == "__main__":
    # Initialize the handler with directories and dataset name
    dataset_handler = KaggleDataHandler(
        dataset_name="imranalishahh/comprehensive-synthetic-e-commerce-dataset",
        raw_data_dir="./data/raw_data",
        chunk_data_dir="./data/chunk_data"
    )

    # Execute the pipeline
    dataset_handler.process()