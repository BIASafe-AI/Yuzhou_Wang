import yfinance as yf
import pandas as pd
from tqdm import tqdm  # +++ Added progress bar library +++

class FeatureEngine:
    def __init__(self, sp500_symbols, added_stocks, removed_stocks):
        self.sp500_symbols = [s.replace('.', '-') for s in sp500_symbols]  # +++ Standardize symbol format +++
        self.added_stocks = added_stocks
        self.removed_stocks = removed_stocks

    def remove_survivorship_bias(self, start_date, end_date):
        # +++ Download data in batches (50 stocks per batch) +++
        batch_size = 50
        all_data = []
        for i in tqdm(range(0, len(self.sp500_symbols), batch_size)):
            batch = self.sp500_symbols[i:i+batch_size]
            df = yf.download(batch, start=start_date, end=end_date, interval="1mo")['Close']
            if not df.empty:
                all_data.append(df)
        
        df = pd.concat(all_data, axis=1).ffill()
        
        # +++ Survivorship bias correction (strict time filtering) +++
        for _, row in self.removed_stocks.iterrows():
            symbol = row['Symbol'].replace('.', '-')
            if symbol in df.columns:
                df[symbol] = df[symbol][df.index <= row['EndDate']]
        
        for _, row in self.added_stocks.iterrows():
            symbol = row['Symbol'].replace('.', '-')
            if symbol in df.columns:
                df[symbol] = df[symbol][df.index >= row['StartDate']]
                
        return df.dropna(how='all', axis=1)  # +++ Remove columns that are entirely NaN +++
