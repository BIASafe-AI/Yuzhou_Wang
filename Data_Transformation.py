import pandas as pd
import os
# 读取原始 CSV 文件
current_dir = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(current_dir, 'sp500_stocks_data_test.csv')
df = pd.read_csv(file_path, header=None)

# 获取列名（第一行是股票代码，第二行是指标）
tickers = df.iloc[0, 1:].ffill().values  # 股票代码，向前填充以匹配所有列
metrics = df.iloc[1, 1:].values  # 对应的指标名称（Open, High, Low, Close, Volume）

# 创建新的列名
new_columns = ["Date"] + [f"{ticker}_{metric}" for ticker, metric in zip(tickers, metrics)]

# 处理数据部分
df_cleaned = df.iloc[2:].copy()  # 去掉前两行
df_cleaned.columns = new_columns  # 重新设置列名
df_cleaned["Date"] = pd.to_datetime(df_cleaned["Date"], format="%Y/%m/%d", errors="coerce")
  # 转换日期格式

# 转换成长格式（Long Format）
long_df = df_cleaned.melt(id_vars=["Date"], var_name="Ticker_Metric", value_name="Value")

# 拆分 Ticker 和 Metric
long_df["Ticker"] = long_df["Ticker_Metric"].str.extract(r"([A-Z]+)")
long_df["Metric"] = long_df["Ticker_Metric"].str.extract(r"(Open|High|Low|Close|Volume)")

# 重新排列成最终格式
final_df = long_df.pivot_table(index=["Date", "Ticker"], columns="Metric", values="Value", aggfunc="first").reset_index()

# 确保数值列为数值类型
final_df[["Open", "High", "Low", "Close", "Volume"]] = final_df[["Open", "High", "Low", "Close", "Volume"]].apply(pd.to_numeric, errors="coerce")

# **保存清理后的数据**
final_df.to_csv("cleaned_sp500_data_test.csv", index=False)  # 保存为 CSV
# final_df.to_excel("cleaned_sp500_data.xlsx", index=False)  # 也可以保存为 Excel

print("数据清理完成，已保存为 cleaned_sp500_data_test.csv")