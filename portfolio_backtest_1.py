import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# 读取数据
file_path = "cleaned_sp500_data_test.csv"
df = pd.read_csv(file_path)

# 确保列名无误
df.columns = df.columns.str.strip()

# 检查 Date 列是否存在
if "Date" not in df.columns:
    raise KeyError("CSV 文件中未找到 'Date' 列，请检查文件格式！")

# 转换日期格式
df['Date'] = pd.to_datetime(df['Date'])

# 按日期排序
df = df.sort_values(by=['Ticker', 'Date']).reset_index(drop=True)

# 获取所有股票代码
unique_tickers = df["Ticker"].unique()
print(f"可用股票代码：{list(unique_tickers)}")

# 让用户选择一个股票
while True:
    ticker_to_plot = input("请输入要回测的股票代码（例如 AAPL）：").upper()
    if ticker_to_plot in unique_tickers:
        break
    print("无效的股票代码，请重新输入！")

# 筛选该股票的数据
df_ticker = df[df["Ticker"] == ticker_to_plot].copy().reset_index(drop=True)

# 确保 Date 仍然存在
if "Date" not in df_ticker.columns:
    raise KeyError(f"股票 {ticker_to_plot} 的数据丢失 'Date' 列！")

# 让用户输入移动窗口大小
while True:
    try:
        window_size = int(input("请输入移动窗口大小（例如 50, 100, 200）："))
        if window_size > 0:
            break
        else:
            print("请输入一个大于 0 的整数！")
    except ValueError:
        print("无效输入，请输入一个整数！")

# 计算每日回报
df_ticker["Daily Return"] = df_ticker["Close"].pct_change()

# 计算滚动波动率（年化标准差）
df_ticker['Rolling Volatility'] = df_ticker['Daily Return'].rolling(window=window_size).std() * (252 ** 0.5)

# 计算简单移动平均（SMA）
df_ticker[f"SMA_{window_size}"] = df_ticker["Close"].rolling(window=window_size).mean()

# 生成交易信号（SMA Crossover）
df_ticker["Signal"] = 0
df_ticker.loc[df_ticker["Close"] > df_ticker[f"SMA_{window_size}"], "Signal"] = 1  # 买入
df_ticker.loc[df_ticker["Close"] < df_ticker[f"SMA_{window_size}"], "Signal"] = -1  # 卖出

# 计算策略回测收益
df_ticker["Strategy Return"] = df_ticker["Daily Return"] * df_ticker["Signal"].shift(1)
df_ticker["Cumulative Market Return"] = (1 + df_ticker["Daily Return"]).cumprod()
df_ticker["Cumulative Strategy Return"] = (1 + df_ticker["Strategy Return"]).cumprod()

# 计算最大回撤
rolling_max = df_ticker["Cumulative Strategy Return"].cummax()
df_ticker["Drawdown"] = df_ticker["Cumulative Strategy Return"] / rolling_max - 1
max_drawdown = df_ticker["Drawdown"].min()

# 计算夏普比率（防止 NaN 错误）
try:
    sharpe_ratio = df_ticker["Strategy Return"].mean() / df_ticker["Strategy Return"].std() * np.sqrt(252)
except ZeroDivisionError:
    sharpe_ratio = np.nan

# 可视化：策略 vs. 市场累计收益
plt.figure(figsize=(14, 6))
plt.plot(df_ticker["Date"], df_ticker["Cumulative Market Return"], label="Market Return", linestyle="--")
plt.plot(df_ticker["Date"], df_ticker["Cumulative Strategy Return"], label="Strategy Return", color="blue")
plt.title(f"Backtest Performance for {ticker_to_plot} (Window: {window_size})\nSharpe Ratio: {sharpe_ratio:.2f}, Max Drawdown: {max_drawdown:.2%}")
plt.xlabel("Date")
plt.ylabel("Cumulative Returns")
plt.legend()
plt.show(block = True)

# 可视化：交易信号
plt.figure(figsize=(14, 6))
plt.plot(df_ticker["Date"], df_ticker["Close"], label="Close Price", color="black", alpha=0.6)
plt.plot(df_ticker["Date"], df_ticker[f"SMA_{window_size}"], label=f"SMA {window_size}", linestyle="--")

# 标记买入/卖出信号
buy_signals = df_ticker[df_ticker["Signal"] == 1]
sell_signals = df_ticker[df_ticker["Signal"] == -1]

plt.scatter(buy_signals["Date"], buy_signals["Close"], marker="^", color="green", label="Buy Signal", alpha=1, zorder=3)
plt.scatter(sell_signals["Date"], sell_signals["Close"], marker="v", color="red", label="Sell Signal", alpha=1, zorder=3)

plt.title(f"Trading Signals for {ticker_to_plot} (Window: {window_size})")
plt.xlabel("Date")
plt.ylabel("Stock Price")
plt.legend()
plt.show(block = True)

# 保存回测结果
output_file = f"{ticker_to_plot}_strategy_backtest.csv"
df_ticker.to_csv(output_file, index=False)
print(f"回测结果已保存为 {output_file}！")
