import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime

# ===============================
# 1. Data Reading and Preprocessing
# ===============================
# Assume your CSV file is named "my_stock_data.csv", containing the following columns:
# Date, Ticker, Open, High, Low, Close, Volume
# The Date format can be either 2010/1/4 or 2010-01-04, parse_dates will automatically parse it
df = pd.read_csv("cleaned_sp500_data_test.csv", parse_dates=["Date"])

# Sort to ensure the time series order
df = df.sort_values(["Ticker", "Date"]).reset_index(drop=True)

# Calculate daily returns: Return = (Today's Close - Yesterday's Close) / Yesterday's Close
df["Daily_Return"] = df.groupby("Ticker")["Close"].pct_change()

# The first day's return cannot be calculated, resulting in NaN, which should be removed if needed
df = df.dropna(subset=["Daily_Return"])

# ===============================
# 2. Factor Calculation: Short-term Momentum, Long-term Momentum, and Volatility
# ===============================
# Example: Using 20-day and 60-day momentum, and calculating 20-day historical volatility
# Note: pct_change(N) calculates the change from N+1 days ago to the current day, this is just a demonstration
df["Momentum_20d"] = df.groupby("Ticker")["Close"].pct_change(20)
df["Momentum_60d"] = df.groupby("Ticker")["Close"].pct_change(60)

# Calculate 20-day rolling volatility: Rolling standard deviation of daily returns
df["Volatility_20d"] = df.groupby("Ticker")["Daily_Return"].rolling(20).std().values

# Since rolling operations produce NaN values, they need to be removed
df = df.dropna(subset=["Momentum_20d", "Momentum_60d", "Volatility_20d"])

# ===============================
# 3. Backtesting Framework: Monthly Data Segmentation
# ===============================
# Label each record with its corresponding "Year-Month" for monthly segmentation
df["YearMonth"] = df["Date"].dt.to_period("M")

# To select stocks at the end of each month and hold them for the next month, 
# we need to identify the last trading day of each month
# Method: Group by YearMonth and find the maximum date within each group
last_days = df.groupby("YearMonth")["Date"].max().reset_index()
last_days.columns = ["YearMonth", "MonthEnd"]

# Merge this table with the original data to mark records as "Month-End"
df = pd.merge(df, last_days, on="YearMonth", how="left")
df["IsMonthEnd"] = (df["Date"] == df["MonthEnd"])

# We only calculate factor rankings and select stocks at "Month-End"
month_end_data = df[df["IsMonthEnd"]].copy()

# ===============================
# 4. Long-Short Portfolio Construction: Stock Selection Logic
# ===============================
# Example: Standardize factors, then compute a composite factor score = Short-term momentum rank + Long-term momentum rank - Volatility rank
# (Higher volatility is less favorable, so we penalize it with a negative sign; you can adjust as needed)
# Finally, select the top 10% ranked stocks for long positions and the bottom 10% for short positions
# Note: This can be more complex, e.g., integrating other factors, using machine learning models, etc. This is just an illustration of multi-factor scoring

# To facilitate comparison, we create a simple benchmark: equal-weighting all stocks
# The portfolio is recalculated monthly and held for the next month

# List to store monthly strategy and benchmark returns
results = []

# Sort YearMonth
unique_months = sorted(month_end_data["YearMonth"].unique())

# Set trading cost assumption: 0.1% per turnover
transaction_cost_rate = 0.001

# Track previous long/short positions to calculate turnover rate
prev_long = set()
prev_short = set()

for i in range(len(unique_months) - 1):
    current_month = unique_months[i]
    next_month = unique_months[i + 1]
    
    # Extract data at month-end
    df_month = month_end_data[month_end_data["YearMonth"] == current_month].copy()
    
    # Skip if there are too few stocks
    if len(df_month) < 50:
        continue
    
    # Simple standardization
    df_month["Z_Mom20"] = (df_month["Momentum_20d"] - df_month["Momentum_20d"].mean()) / df_month["Momentum_20d"].std()
    df_month["Z_Mom60"] = (df_month["Momentum_60d"] - df_month["Momentum_60d"].mean()) / df_month["Momentum_60d"].std()
    df_month["Z_Vol20"] = (df_month["Volatility_20d"] - df_month["Volatility_20d"].mean()) / df_month["Volatility_20d"].std()
    
    # Compute composite score: Higher is better
    df_month["Factor_Score"] = df_month["Z_Mom60"]
    
    # Sort by factor score
    df_month = df_month.sort_values("Factor_Score", ascending=False)
    
    # Select the top 10% for long positions and bottom 10% for short positions
    n_stocks = len(df_month)
    top_n = int(n_stocks * 0.1)
    bottom_n = int(n_stocks * 0.1)
    
    long_stocks = set(df_month.head(top_n)["Ticker"].tolist())
    short_stocks = set(df_month.tail(bottom_n)["Ticker"].tolist())
    
    # Avoid overlap
    long_stocks = long_stocks - short_stocks
    short_stocks = short_stocks - long_stocks
    
    # Calculate turnover rate
    if i == 0:
        turnover = 0
    else:
        new_long = long_stocks - prev_long
        new_short = short_stocks - prev_short
        turnover = len(new_long) + len(new_short)
    
    prev_long = long_stocks
    prev_short = short_stocks
    
    # Transaction cost multiplier
    cost_multiplier = 1 - transaction_cost_rate * turnover
    
    # ===============================
    # 5. Calculate Next Month's Returns
    # ===============================
    df_next_month = df[(df["YearMonth"] == next_month)].copy()
    if df_next_month.empty:
        continue
    
    df_next_month["Strategy_Return"] = df_next_month.apply(
        lambda row: row["Daily_Return"] if row["Ticker"] in long_stocks, 
                   #else (-row["Daily_Return"] if row["Ticker"] in short_stocks else 0),
        axis=1
    )

    daily_strategy = df_next_month.groupby("Date")["Strategy_Return"].sum() / max(len(long_stocks), 1)
    
    if not daily_strategy.empty:
        first_day = daily_strategy.index[0]
        daily_strategy[first_day] *= cost_multiplier
    
    df_next_month["Benchmark_Return"] = df_next_month.apply(
        lambda row: row["Daily_Return"] if row["Ticker"] in df_month["Ticker"].tolist() else 0,
        axis=1
    )
    
    daily_benchmark = df_next_month.groupby("Date")["Benchmark_Return"].mean()
    
    merged = pd.concat([daily_strategy, daily_benchmark], axis=1)
    merged.columns = ["Strategy_Return", "Benchmark_Return"]
    
    results.append(merged)

df_results = pd.concat(results).sort_index()

# ===============================
# 6. Performance Evaluation and Visualization
# ===============================
df_results["Strategy_Cum"] = (1 + df_results["Strategy_Return"].fillna(0)).cumprod()
df_results["Benchmark_Cum"] = (1 + df_results["Benchmark_Return"].fillna(0)).cumprod()

plt.figure(figsize=(12, 6))
plt.plot(df_results.index, df_results["Strategy_Cum"], label="Strategy", color="blue")
plt.plot(df_results.index, df_results["Benchmark_Cum"], label="Benchmark (Equal Weight)", color="orange")
plt.title("Multi-Factor Long-Short Strategy vs. Equal-Weighted Benchmark")
plt.xlabel("Date")
plt.ylabel("Cumulative Return")
plt.legend()
plt.grid(True)
plt.show()
