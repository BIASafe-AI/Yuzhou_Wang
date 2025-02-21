import yfinance as yf
import pandas as pd

wiki_url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
sp500_table = pd.read_html(wiki_url)
sp500_tickers = sp500_table[0]["Symbol"].tolist()  

sp500_data = yf.download(sp500_tickers, start="2010-01-01", end="2024-01-01", group_by="ticker")

file_path = "sp500_stocks_data.csv"
sp500_data.to_csv(file_path)

print(f"S&P 500 data saved as {file_path}")
