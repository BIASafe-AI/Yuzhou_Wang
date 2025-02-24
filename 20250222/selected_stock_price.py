import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import time

def clean_ticker(ticker):
    """ 清除 ticker 中的无效字符，例如 '*' """
    return ticker.replace("*", "").strip()

def get_stock_data_for_day(ticker, target_date):
    """
    获取指定 ticker 在 target_date 当天的行情数据。
    使用 target_date 到 target_date+1 的区间作为查询时间。
    返回 DataFrame 中的单行数据（Series），如果无数据返回 None。
    """
    # 清洗 ticker，确保请求的是有效的代码
    ticker_clean = clean_ticker(ticker)
    start_str = target_date.strftime("%Y-%m-%d")
    end_str = (target_date + timedelta(days=1)).strftime("%Y-%m-%d")
    
    try:
        stock = yf.Ticker(ticker_clean)
        data = stock.history(start=start_str, end=end_str)
    except Exception as e:
        print(f"Error retrieving {ticker_clean} on {target_date.strftime('%Y-%m-%d')}: {e}")
        return None

    if data.empty:
        return None
    data = data.reset_index()
    return data.iloc[0]

if __name__ == "__main__":
    # 读取 consolidated.csv 文件，要求文件中包含 "ticker" 和 "logic_date" 列
    consolidated_file = r"C:\Users\Wyuzh\Desktop\Capstone\biasafe\archive\consolidated.csv"  # 根据实际情况修改路径
    df = pd.read_csv(consolidated_file, parse_dates=["logic_date"])

    results = []
    for idx, row in df.iterrows():
        raw_ticker = row["ticker"]
        target_date = row["logic_date"]
        print(f"正在处理 {raw_ticker} 在 {target_date.date()} 的数据...")
        stock_data = get_stock_data_for_day(raw_ticker, target_date)
        if stock_data is not None:
            # 将清洗后的 ticker 和逻辑日期加入到结果中
            stock_data["ticker"] = clean_ticker(raw_ticker)
            stock_data["logic_date"] = target_date.strftime("%Y-%m-%d")
            results.append(stock_data)
        else:
            print(f"没有找到 {raw_ticker} 在 {target_date.date()} 的数据。")
        # 每次请求后延时 1 秒，降低请求频率
        time.sleep(1)

    if results:
        result_df = pd.DataFrame(results)
        output_file = "stock_data_by_ticker_date.csv"
        result_df.to_csv(output_file, index=False)
        print(f"✅ 数据已保存到 {output_file}")
    else:
        print("⚠️ 没有获取到任何股票数据。")
