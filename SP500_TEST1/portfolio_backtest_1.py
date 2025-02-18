import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# Load data
file_path = "cleaned_sp500_data_test.csv"
df = pd.read_csv(file_path)

# Ensure column names are correct
df.columns = df.columns.str.strip()

# Check if the 'Date' column exists
if "Date" not in df.columns:
    raise KeyError("The 'Date' column was not found in the CSV file. Please check the file format!")

# Convert date format
df['Date'] = pd.to_datetime(df['Date'])

# Sort by date and ticker
df = df.sort_values(by=['Ticker', 'Date']).reset_index(drop=True)

# Get all unique stock tickers
unique_tickers = df["Ticker"].unique()
print(f"Available stock tickers: {list(unique_tickers)}")

# Prompt the user to select a stock
while True:
    ticker_to_plot = input("Enter the stock ticker for backtesting (e.g., AAPL): ").upper()
    if ticker_to_plot in unique_tickers:
        break
    print("Invalid stock ticker. Please enter a valid one!")

# Filter data for the selected stock
df_ticker = df[df["Ticker"] == ticker_to_plot].copy().reset_index(drop=True)

# Ensure the 'Date' column still exists
if "Date" not in df_ticker.columns:
    raise KeyError(f"Missing 'Date' column in data for {ticker_to_plot}!")

# Prompt the user to input the moving window size
while True:
    try:
        window_size = int(input("Enter the moving window size (e.g., 50, 100, 200): "))
        if window_size > 0:
            break
        else:
            print("Please enter an integer greater than 0!")
    except ValueError:
        print("Invalid input. Please enter an integer!")

# Calculate daily returns
df_ticker["Daily Return"] = df_ticker["Close"].pct_change()

# Compute rolling volatility (annualized standard deviation)
df_ticker['Rolling Volatility'] = df_ticker['Daily Return'].rolling(window=window_size).std() * (252 ** 0.5)

# Compute Simple Moving Average (SMA)
df_ticker[f"SMA_{window_size}"] = df_ticker["Close"].rolling(window=window_size).mean()

# Generate trading signals (SMA crossover)
df_ticker["Signal"] = 0
df_ticker.loc[df_ticker["Close"] > df_ticker[f"SMA_{window_size}"], "Signal"] = 1  # Buy signal
df_ticker.loc[df_ticker["Close"] < df_ticker[f"SMA_{window_size}"], "Signal"] = -1  # Sell signal

# Compute strategy returns
df_ticker["Strategy Return"] = df_ticker["Daily Return"] * df_ticker["Signal"].shift(1)
df_ticker["Cumulative Market Return"] = (1 + df_ticker["Daily Return"]).cumprod()
df_ticker["Cumulative Strategy Return"] = (1 + df_ticker["Strategy Return"]).cumprod()

# Compute maximum drawdown
rolling_max = df_ticker["Cumulative Strategy Return"].cummax()
df_ticker["Drawdown"] = df_ticker["Cumulative Strategy Return"] / rolling_max - 1
max_drawdown = df_ticker["Drawdown"].min()

# Compute Sharpe Ratio (handle NaN errors)
try:
    sharpe_ratio = df_ticker["Strategy Return"].mean() / df_ticker["Strategy Return"].std() * np.sqrt(252)
except ZeroDivisionError:
    sharpe_ratio = np.nan

# Visualization: Strategy vs. Market Cumulative Returns
plt.figure(figsize=(14, 6))
plt.plot(df_ticker["Date"], df_ticker["Cumulative Market Return"], label="Market Return", linestyle="--")
plt.plot(df_ticker["Date"], df_ticker["Cumulative Strategy Return"], label="Strategy Return", color="blue")
plt.title(f"Backtest Performance for {ticker_to_plot} (Window: {window_size})\nSharpe Ratio: {sharpe_ratio:.2f}, Max Drawdown: {max_drawdown:.2%}")
plt.xlabel("Date")
plt.ylabel("Cumulative Returns")
plt.legend()
plt.show(block=True)

# Visualization: Trading Signals
plt.figure(figsize=(14, 6))
plt.plot(df_ticker["Date"], df_ticker["Close"], label="Close Price", color="black", alpha=0.6)
plt.plot(df_ticker["Date"], df_ticker[f"SMA_{window_size}"], label=f"SMA {window_size}", linestyle="--")

# Mark buy/sell signals
buy_signals = df_ticker[df_ticker["Signal"] == 1]
sell_signals = df_ticker[df_ticker["Signal"] == -1]

plt.scatter(buy_signals["Date"], buy_signals["Close"], marker="^", color="green", label="Buy Signal", alpha=1, zorder=3)
plt.scatter(sell_signals["Date"], sell_signals["Close"], marker="v", color="red", label="Sell Signal", alpha=1, zorder=3)

plt.title(f"Trading Signals for {ticker_to_plot} (Window: {window_size})")
plt.xlabel("Date")
plt.ylabel("Stock Price")
plt.legend()
plt.show(block=True)

# Save backtest results
output_file = f"{ticker_to_plot}_strategy_backtest.csv"
df_ticker.to_csv(output_file, index=False)
print(f"Backtest results saved as {output_file}!")
