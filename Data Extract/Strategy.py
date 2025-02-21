import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime

# ===============================
# 1. 数据读取与预处理
# ===============================
# 假设你的CSV文件名为 "my_stock_data.csv"，其中包含以下列：
# Date, Ticker, Open, High, Low, Close, Volume
# Date 格式如 2010/1/4 或 2010-01-04 都可以，parse_dates 会自动解析
df = pd.read_csv("cleaned_sp500_data_test.csv", parse_dates=["Date"])

# 排序，保证时间序列顺序
df = df.sort_values(["Ticker", "Date"]).reset_index(drop=True)

# 计算每日收益率：Return = (今日Close - 昨日Close) / 昨日Close
df["Daily_Return"] = df.groupby("Ticker")["Close"].pct_change()

# 由于首日无法计算收益率，会产生NaN，后面如果要继续使用则需剔除
df = df.dropna(subset=["Daily_Return"])

# ===============================
# 2. 计算多因子：短期动量、长期动量、波动率
# ===============================
# 以 20 日和 60 日动量为例，同时计算 20 日历史波动率
# 注意：pct_change(N) 计算 N+1 日前到当前日的变化，这里仅作示例
df["Momentum_20d"] = df.groupby("Ticker")["Close"].pct_change(20)
df["Momentum_60d"] = df.groupby("Ticker")["Close"].pct_change(60)

# 计算 20 日滚动波动率：对每日收益率做 rolling std
df["Volatility_20d"] = df.groupby("Ticker")["Daily_Return"].rolling(20).std().values

# 因为 rolling 操作会带来 NaN，需要剔除
df = df.dropna(subset=["Momentum_20d", "Momentum_60d", "Volatility_20d"])

# ===============================
# 3. 构建回测框架：按月划分数据
# ===============================
# 给每条记录标注所属“年月”，用来做月度切分
df["YearMonth"] = df["Date"].dt.to_period("M")

# 为了每个月底选股并持有下个月，需要先找到每个月最后一个交易日的数据
# 方法：对 YearMonth 分组，找到每个组里最大的日期
last_days = df.groupby("YearMonth")["Date"].max().reset_index()
last_days.columns = ["YearMonth", "MonthEnd"]

# 将这张表再与原数据合并，标记哪些记录是“月末”
df = pd.merge(df, last_days, on="YearMonth", how="left")
df["IsMonthEnd"] = (df["Date"] == df["MonthEnd"])

# 我们只在“月末”时计算因子排名并选股
month_end_data = df[df["IsMonthEnd"]].copy()

# ===============================
# 4. 多空组合构建：选股逻辑
# ===============================
# 这里示例：先对因子做标准化，然后计算一个综合因子分数 = 短期动量排名 + 长期动量排名 - 波动率排名
# （波动率越大越不利，所以这里以负号对其进行惩罚，也可以按自己需求调整）
# 最后根据这个综合分数选取排名前 10% 股票做多，排名后 10% 股票做空
# 注意：实际可以更复杂，比如结合其他因子、做机器学习模型等，这里只示范多因子打分方式

# 为了方便比较，我们再做一个简单基准：等权持有所有股票
# 我们每个月都重新计算组合，并在下个月持有

# 创建一个列表，用于存储每个月的策略收益、基准收益
results = []

# 将 YearMonth 排序
unique_months = sorted(month_end_data["YearMonth"].unique())

# 设定交易成本假设：换手时收 0.1%
transaction_cost_rate = 0.001

# 记录上期多头/空头，用来计算换手率
prev_long = set()
prev_short = set()

for i in range(len(unique_months) - 1):
    current_month = unique_months[i]
    next_month = unique_months[i + 1]
    
    # 取当月月末的数据
    df_month = month_end_data[month_end_data["YearMonth"] == current_month].copy()
    
    # 如果股票数量太少，就跳过
    if len(df_month) < 50:
        continue
    
    # 简单做一个标准化
    df_month["Z_Mom20"] = (df_month["Momentum_20d"] - df_month["Momentum_20d"].mean()) / df_month["Momentum_20d"].std()
    df_month["Z_Mom60"] = (df_month["Momentum_60d"] - df_month["Momentum_60d"].mean()) / df_month["Momentum_60d"].std()
    df_month["Z_Vol20"] = (df_month["Volatility_20d"] - df_month["Volatility_20d"].mean()) / df_month["Volatility_20d"].std()
    
    # 计算综合打分：越高越好
    # 例如：综合分 = Z_Mom20 + Z_Mom60 - Z_Vol20
    df_month["Factor_Score"] = df_month["Z_Mom20"] + df_month["Z_Mom60"] - df_month["Z_Vol20"]
    
    # 根据综合分数排序
    df_month = df_month.sort_values("Factor_Score", ascending=False)
    
    # 选出前 10% 作为多头，后 10% 作为空头
    n_stocks = len(df_month)
    top_n = int(n_stocks * 0.1)
    bottom_n = int(n_stocks * 0.1)
    
    long_stocks = set(df_month.head(top_n)["Ticker"].tolist())
    short_stocks = set(df_month.tail(bottom_n)["Ticker"].tolist())
    
    # 防止交叉
    long_stocks = long_stocks - short_stocks
    short_stocks = short_stocks - long_stocks
    
    # 计算换手率
    if i == 0:
        turnover = 0
    else:
        new_long = long_stocks - prev_long
        new_short = short_stocks - prev_short
        turnover = len(new_long) + len(new_short)
    # 记录当前作为下次循环的前值
    prev_long = long_stocks
    prev_short = short_stocks
    
    # 交易成本因子
    cost_multiplier = 1 - transaction_cost_rate * turnover
    
    # ===============================
    # 5. 计算下个月的收益
    # ===============================
    # 找到下个月所有交易日的数据
    df_next_month = df[(df["YearMonth"] == next_month)].copy()
    if df_next_month.empty:
        continue
    
    # 对于多头：直接用 Daily_Return；空头：用 -Daily_Return；其余 0
    # 这里简单用等权分配给所有多头/空头，分别求平均
    # 最终策略收益 = 多头收益 + 空头收益
    # （也可以在股票层面更细致地计算加权，示例仅展示核心逻辑）
    
    # 基准组合：等权持有本期所有股票
    # 先找出当月月末可交易的所有股票
    all_stocks = set(df_month["Ticker"].tolist())
    
    # 将 next_month 的每日收益拼接
    df_next_month["Strategy_Return"] = df_next_month.apply(
        lambda row: row["Daily_Return"] if row["Ticker"] in long_stocks 
                    else (-row["Daily_Return"] if row["Ticker"] in short_stocks else 0),
        axis=1
    )
    
    # 计算每日的平均策略收益
    # 注意：等权平均：对多头股票做平均，对空头股票做平均，再把它们合并
    # 这里简化处理：我们直接把多头和空头都加起来，再除以(多头数量+空头数量)
    # 如果你想区分多头和空头的资金占比，可以自行调整
    n_long = len(long_stocks)
    n_short = len(short_stocks)
    n_total = n_long + n_short if (n_long + n_short) != 0 else 1
    
    # 策略每日收益 = (股票层面的收益之和) / n_total
    # groupby 按 Date 分组后求平均
    daily_strategy = df_next_month.groupby("Date")["Strategy_Return"].sum() / n_total
    
    # 考虑交易成本：在下个月第一个交易日扣一次成本
    if not daily_strategy.empty:
        first_day = daily_strategy.index[0]
        daily_strategy[first_day] *= cost_multiplier
    
    # 基准：等权持有本期可交易的所有股票
    df_next_month["Benchmark_Return"] = df_next_month.apply(
        lambda row: row["Daily_Return"] if row["Ticker"] in all_stocks else 0,
        axis=1
    )
    # 等权平均
    daily_benchmark = df_next_month.groupby("Date")["Benchmark_Return"].mean()
    
    # 合并到一个 DataFrame，方便后续计算累计收益
    merged = pd.concat([daily_strategy, daily_benchmark], axis=1)
    merged.columns = ["Strategy_Return", "Benchmark_Return"]
    
    results.append(merged)

# 合并所有月份的数据
if len(results) == 0:
    raise ValueError("没有有效的回测结果，请检查数据范围或因子计算。")

df_results = pd.concat(results).sort_index()

# ===============================
# 6. 绩效评估与可视化
# ===============================
# 计算累计收益
df_results["Strategy_Cum"] = (1 + df_results["Strategy_Return"].fillna(0)).cumprod()
df_results["Benchmark_Cum"] = (1 + df_results["Benchmark_Return"].fillna(0)).cumprod()

# 画图对比
plt.figure(figsize=(12, 6))
plt.plot(df_results.index, df_results["Strategy_Cum"], label="Strategy", color="blue")
plt.plot(df_results.index, df_results["Benchmark_Cum"], label="Benchmark (Equal Weight)", color="orange")
plt.title("Multi-Factor Long-Short Strategy vs. Equal-Weighted Benchmark")
plt.xlabel("Date")
plt.ylabel("Cumulative Return")
plt.legend()
plt.grid(True)
plt.show()

# 计算年化收益、年化波动率、夏普比率、最大回撤
# 假设是日频数据，一年约252个交易日
daily_ret = df_results["Strategy_Return"].fillna(0)
benchmark_ret = df_results["Benchmark_Return"].fillna(0)

ann_factor = 252  # 如果是日频
annual_return = daily_ret.mean() * ann_factor
annual_vol = daily_ret.std() * np.sqrt(ann_factor)
sharpe_ratio = annual_return / annual_vol if annual_vol != 0 else np.nan
max_drawdown = (df_results["Strategy_Cum"] / df_results["Strategy_Cum"].cummax() - 1).min()

# 基准指标
annual_return_bmk = benchmark_ret.mean() * ann_factor
annual_vol_bmk = benchmark_ret.std() * np.sqrt(ann_factor)
sharpe_ratio_bmk = annual_return_bmk / annual_vol_bmk if annual_vol_bmk != 0 else np.nan
max_drawdown_bmk = (df_results["Benchmark_Cum"] / df_results["Benchmark_Cum"].cummax() - 1).min()

print("=== 策略绩效 ===")
print(f"策略年化收益: {annual_return:.2%}")
print(f"策略年化波动率: {annual_vol:.2%}")
print(f"策略夏普比率: {sharpe_ratio:.2f}")
print(f"策略最大回撤: {max_drawdown:.2%}")

print("\n=== 基准绩效 ===")
print(f"基准年化收益: {annual_return_bmk:.2%}")
print(f"基准年化波动率: {annual_vol_bmk:.2%}")
print(f"基准夏普比率: {sharpe_ratio_bmk:.2f}")
print(f"基准最大回撤: {max_drawdown_bmk:.2%}")
