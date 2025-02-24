import pandas as pd

class SP500Filter:
    """
    A class that handles loading a CSV file of S&P 500 data,
    prompting the user for a date range, and filtering the data
    accordingly before saving to a new CSV file.
    """

    def __init__(self, input_csv, output_csv):
        """
        Initialize with paths to the input and output CSV files.
        """
        self.input_csv = input_csv
        self.output_csv = output_csv
        self.df = None

    def load_data(self):
        """
        Load the CSV data and parse the 'date' column as datetime.
        """
        self.df = pd.read_csv(self.input_csv, parse_dates=['date'])

    def get_user_date_range(self):
        """
        Prompt the user for start and end dates, returning them as datetime objects.
        """
        start_date_str = input("Enter start date (YYYY-MM-DD): ")
        end_date_str = input("Enter end date (YYYY-MM-DD): ")

        start_date = pd.to_datetime(start_date_str)
        end_date = pd.to_datetime(end_date_str)

        return start_date, end_date

    def filter_data_by_date(self, start_date, end_date):
        """
        Filter the DataFrame based on the provided date range.
        """
        if self.df is not None:
            self.df = self.df[(self.df['date'] >= start_date) & (self.df['date'] <= end_date)]
        else:
            raise ValueError("Data has not been loaded. Please call load_data() first.")

    def save_filtered_data(self):
        """
        Save the filtered data to the output CSV file.
        """
        if self.df is not None:
            self.df.to_csv(self.output_csv, index=False)
            print(f"Filtered data saved to: {self.output_csv}")
        else:
            raise ValueError("No data to save. Please filter the data first.")

    def run(self):
        """
        Execute the complete workflow:
        1. Load data
        2. Get user-specified date range
        3. Filter data
        4. Save the filtered data
        """
        self.load_data()
        start_date, end_date = self.get_user_date_range()
        self.filter_data_by_date(start_date, end_date)
        self.save_filtered_data()

if __name__ == '__main__':
    input_csv_path = r'C:\Users\Wyuzh\Desktop\Capstone\biasafe\archive\sp500_historical_filled.csv'  # Path to your input CSV
    output_csv_path = r'C:\Users\Wyuzh\Desktop\Capstone\biasafe\archive\sp500_filtered.csv'          # Path to your output CSV

    filter_instance = SP500Filter(input_csv_path, output_csv_path)
    filter_instance.run()
