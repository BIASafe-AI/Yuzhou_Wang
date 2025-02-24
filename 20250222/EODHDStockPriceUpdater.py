import requests
import pandas as pd

class EODHDStockPriceUpdater:
    """
    This class updates stock prices by fetching historical data from the EODHD.com API.
    It uses an input CSV file containing 'ticker' and 'date' columns, retrieves the specified
    price (default: closing price) for each stock on the specified dates, and outputs an updated
    CSV file with the price information merged in.
    """

    def __init__(self, api_token, input_file, output_file, price_field="close"):
        """
        :param api_token: Your EODHD API token.
        :param input_file: Path to the input CSV file (must contain 'ticker' and 'date' columns).
        :param output_file: Path to save the updated CSV file.
        :param price_field: The price field to fetch (default is "close").
        """
        self.api_token = api_token
        self.input_file = input_file
        self.output_file = output_file
        self.price_field = price_field
        self.data = None  # To store the original data

    def load_data(self):
        """
        Load the input CSV file and convert the 'date' column to datetime.
        """
        self.data = pd.read_csv(self.input_file, parse_dates=['date'])
        print("Input data loaded.")

    def fetch_data_for_ticker(self, ticker, start_date, end_date):
        """
        Fetch historical data for a single ticker from the EODHD API.
        
        :param ticker: Stock ticker (e.g., "AAPL.US"). Note that the ticker passed to this function
                       should not contain any invalid characters.
        :param start_date: Start date (datetime object).
        :param end_date: End date (datetime object).
        :return: A DataFrame containing the 'date' and specified price_field columns.
        """
        url = f"https://eodhistoricaldata.com/api/eod/{ticker}"
        params = {
            "api_token": self.api_token,
            "from": start_date.strftime("%Y-%m-%d"),
            "to": end_date.strftime("%Y-%m-%d"),
            "fmt": "json"
        }
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            if data:
                df = pd.DataFrame(data)
                df['date'] = pd.to_datetime(df['date'])
                # Keep only the 'date' and the specified price field
                df = df[['date', self.price_field]]
                return df
            else:
                print(f"No data returned for {ticker}.")
                return pd.DataFrame()
        else:
            print(f"Error fetching data for {ticker}: HTTP {response.status_code}")
            return pd.DataFrame()

    def update_prices(self):
        """
        For each ticker in the input file, determine the required date range, remove any
        invalid characters (e.g., asterisks), fetch historical prices via the API, and merge
        the prices into the original data.
        """
        # Get all unique ticker symbols from the data
        tickers = self.data['ticker'].unique()

        # DataFrame to store the updated data
        updated_data = pd.DataFrame()

        for ticker in tickers:
            # Filter records for the current ticker
            ticker_data = self.data[self.data['ticker'] == ticker].copy()
            # Determine the required date range for this ticker
            min_date = ticker_data['date'].min()
            max_date = ticker_data['date'].max()
            
            # Remove asterisks or other unwanted characters from the ticker for API request
            api_ticker = ticker.replace('*', '')
            print(f"Fetching data for {ticker} (API ticker: {api_ticker}) from {min_date.date()} to {max_date.date()}...")
            
            # Call the API to get historical data for this ticker in the date range
            price_df = self.fetch_data_for_ticker(api_ticker, min_date, max_date)
            if not price_df.empty:
                # Merge the original data with the fetched price data on the 'date' column
                merged = pd.merge(ticker_data, price_df, on='date', how='left')
                updated_data = pd.concat([updated_data, merged], ignore_index=True)
            else:
                print(f"Skipping ticker {ticker} due to missing price data.")

        # Optional: sort the data by date and ticker
        updated_data.sort_values(by=['date', 'ticker'], inplace=True)
        self.data = updated_data

    def save_updated_data(self):
        """
        Save the updated data with price information to the output CSV file.
        """
        self.data.to_csv(self.output_file, index=False)
        print(f"Updated data saved to {self.output_file}.")

    def run(self):
        """
        Execute the full update process:
        1. Load input data.
        2. Update the stock prices by fetching data from the API.
        3. Save the updated data.
        """
        self.load_data()
        self.update_prices()
        self.save_updated_data()


if __name__ == '__main__':
    # Replace the following parameters with your actual information
    api_token = "67b63d35003176.39454097"  # Your EODHD API Token
    input_file = r'C:\Users\Wyuzh\Desktop\Capstone\biasafe\archive\sp500_filtered.csv'  # Path to input CSV file (should contain 'ticker' and 'date' columns)
    output_file = r'C:\Users\Wyuzh\Desktop\Capstone\biasafe\archive\updated_stock_prices.csv'  # Path to output CSV file

    updater = EODHDStockPriceUpdater(api_token, input_file, output_file, price_field="close")
    updater.run()
