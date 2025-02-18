import pandas as pd
import os

# Read the original CSV file
current_dir = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(current_dir, 'sp500_stocks_data_test.csv')
df = pd.read_csv(file_path, header=None)

# Extract column names (first row contains stock tickers, second row contains metrics)
tickers = df.iloc[0, 1:].ffill().values  # Stock tickers, forward-filled to match all columns
metrics = df.iloc[1, 1:].values  # Corresponding metric names (Open, High, Low, Close, Volume)

# Create new column names
new_columns = ["Date"] + [f"{ticker}_{metric}" for ticker, metric in zip(tickers, metrics)]

# Process data section
df_cleaned = df.iloc[2:].copy()  # Remove the first two rows
df_cleaned.columns = new_columns  # Rename columns
df_cleaned["Date"] = pd.to_datetime(df_cleaned["Date"], format="%Y/%m/%d", errors="coerce")  
# Convert date format

# Convert to long format
long_df = df_cleaned.melt(id_vars=["Date"], var_name="Ticker_Metric", value_name="Value")

# Split Ticker and Metric
long_df["Ticker"] = long_df["Ticker_Metric"].str.extract(r"([A-Z]+)")
long_df["Metric"] = long_df["Ticker_Metric"].str.extract(r"(Open|High|Low|Close|Volume)")

# Rearrange into final format
final_df = long_df.pivot_table(index=["Date", "Ticker"], columns="Metric", values="Value", aggfunc="first").reset_index()

# Ensure numeric columns are of numeric type
final_df[["Open", "High", "Low", "Close", "Volume"]] = final_df[["Open", "High", "Low", "Close", "Volume"]].apply(pd.to_numeric, errors="coerce")

# **Save cleaned data**
final_df.to_csv("cleaned_sp500_data_test.csv", index=False)  # Save as CSV
# final_df.to_excel("cleaned_sp500_data.xlsx", index=False)  # Optionally save as Excel

print("Data cleaning completed. Saved as cleaned_sp500_data_test.csv")
