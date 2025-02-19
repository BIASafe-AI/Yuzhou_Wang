import os
import requests
import pandas as pd
import logging
from tenacity import retry, stop_after_attempt, wait_exponential

class DataLoader:
    def __init__(self):
        self.api_key = os.environ.get("EODHD_API_KEY")  # Read from environment variable
        self.base_url = "https://eodhd.com/api"

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        reraise=True
    )
    def get_SP_500_constituents_data(self, start_date, end_date):
        endpoint = f"{self.base_url}/snp500-constituents?api_token={self.api_key}&fmt=json"
        response = requests.get(endpoint)
        
        # Log detailed error messages
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            logging.error(f"API request failed: {e}\nResponse content: {response.text}")
            raise

        data = response.json()
        sp500_stocks = []
        added_stocks = []
        removed_stocks = []

        if 'HistoricalTickerComponents' in data:
            historical_components = data['HistoricalTickerComponents']
            
            # +++ Fix logic for filtering constituent stocks +++
            added_stocks = [
                {'Symbol': h['Code'], 'StartDate': h['StartDate']} 
                for h in historical_components.values() 
                if h.get('StartDate') and (pd.to_datetime(start_date) <= pd.to_datetime(h['StartDate']) <= pd.to_datetime(end_date))
            ]
            
            removed_stocks = [
                {'Symbol': h['Code'], 'EndDate': h['EndDate']}
                for h in historical_components.values() 
                if h.get('IsDelisted') == 1 and (pd.to_datetime(start_date) <= pd.to_datetime(h['EndDate']) <= pd.to_datetime(end_date))
            ]

        # +++ Handle special stock codes (e.g., BRK.B → BRK-B) +++
        sp500_symbols = [s.replace('.', '-') for s in data.get('Components', {}).keys()]
        return sp500_symbols, pd.DataFrame(added_stocks), pd.DataFrame(removed_stocks)
