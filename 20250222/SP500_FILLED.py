import pandas as pd
import pandas_market_calendars as mcal

class SP500_FILLED:
    def __init__(self, input_path, output_path, start_date, end_date):
        """
        Initialize the processor with file paths and date range.
        """
        self.input_path = input_path
        self.output_path = output_path
        self.start_date = pd.to_datetime(start_date)
        self.end_date = pd.to_datetime(end_date)
        self.df = None
        self.result_df = None
        self.added_dates = None
        self.all_dates = None

    def load_data(self):
        """
        Load the CSV data and parse the 'date' column.
        """
        self.df = pd.read_csv(self.input_path, parse_dates=['date'])

    def generate_trading_calendar(self):
        """
        Generate a precise trading calendar using the NYSE calendar.
        """
        nyse = mcal.get_calendar('NYSE')
        trading_days = nyse.valid_days(self.start_date, self.end_date)
        self.all_dates = trading_days.tz_localize(None)

    def preprocess_data(self):
        """
        Filter the data to include only dates starting from the start_date.
        """
        self.df = self.df[self.df['date'] >= self.start_date].copy()

    def fill_missing_dates(self):
        """
        Fill missing dates by aligning the data with the full trading calendar.
        """
        # Group data by date and convert to dictionary format
        date_groups = self.df.groupby('date').apply(lambda x: x.to_dict('records'))
        
        # Create a DataFrame using the complete set of trading dates as the index
        full_df = pd.DataFrame(index=self.all_dates)
        full_df = full_df.join(date_groups.rename('data'), how='left')
        
        # Backfill missing data
        full_df['data'] = full_df['data'].bfill()
        
        # Expand the nested data into a flat structure
        result = []
        for date, rows in full_df.itertuples():
            if isinstance(rows, list):  # Ensure valid data exists
                for row in rows:
                    new_row = dict(row)  # Convert directly to a dictionary
                    new_row['date'] = date
                    result.append(new_row)
                    
        self.result_df = pd.DataFrame(result)

    def detect_new_dates(self):
        """
        Identify the dates that were added (filled) during processing.
        """
        original_dates = self.df['date'].dt.date.unique()
        all_dates_dates = [d.date() for d in self.all_dates]
        self.added_dates = sorted(list(set(all_dates_dates) - set(original_dates)))

    def save_results(self):
        """
        Save the processed data to a CSV file.
        """
        self.result_df.to_csv(self.output_path, index=False)
        print("Processing complete! Results saved to", self.output_path)

    def process(self):
        """
        Execute the complete data processing workflow.
        """
        self.load_data()
        self.generate_trading_calendar()
        self.preprocess_data()
        self.fill_missing_dates()
        self.detect_new_dates()
        print(f"Total number of filled dates: {len(self.added_dates)}")
        print("First 20 new dates example:", self.added_dates[:20])
        self.save_results()


if __name__ == '__main__':
    input_path = r"C:\Users\Wyuzh\Desktop\Capstone\biasafe\archive\sp500_historical.csv"
    output_path = r"C:\Users\Wyuzh\Desktop\Capstone\biasafe\archive\sp500_historical_filled.csv"
    start_date = '2012-04-30'
    end_date = '2024-10-30'

    processor = SP500_FILLED(input_path, output_path, start_date, end_date)
    processor.process()
