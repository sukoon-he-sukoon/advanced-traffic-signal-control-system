import pandas as pd
import time

class RealTimeDataIngestion:
    def __init__(self, file_path, batch_size=5, sleep_time=2):
        """
        Simulates real-time data ingestion from a CSV file.
        :param file_path: Path to the CSV file.
        :param batch_size: Number of rows to ingest at a time.
        :param sleep_time: Time interval (seconds) between data fetches.
        """
        self.file_path = file_path
        self.batch_size = batch_size
        self.sleep_time = sleep_time
        self.df = pd.read_csv(self.file_path)  # Load data initially
        self.current_index = 0  # Start at the beginning

    def get_next_batch(self):
        """
        Fetches the next batch of traffic data.
        :return: A DataFrame containing the next set of rows.
        """
        if self.current_index >= len(self.df):
            print("No more data to fetch. Restarting stream.")
            self.current_index = 0  # Reset to beginning for looping simulation

        next_batch = self.df.iloc[self.current_index:self.current_index + self.batch_size]
        self.current_index += self.batch_size  # Move the pointer
        return next_batch

    def start_stream(self):
        """
        Continuously fetches real-time data in a loop.
        """
        while True:
            batch = self.get_next_batch()
            print(batch)  # In real applications, send this data to storage or processing
            time.sleep(self.sleep_time)  # Wait before fetching the next batch

if __name__ == "__main__":
    # Simulate real-time ingestion from the dataset
    ingestor = RealTimeDataIngestion("C:/Users/iamwa/Desktop/adv-ai-project/advanced-traffic-signal-control-system/data/traffic.csv", batch_size=5, sleep_time=3)
    ingestor.start_stream()
