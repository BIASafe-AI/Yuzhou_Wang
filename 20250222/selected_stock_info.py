import os
import re
import pandas as pd
from datetime import datetime, timedelta

def extract_file_date(filename):
    """
    从文件名 sp500_YYYYMMDD.csv 提取日期 (datetime)。
    如果不匹配，返回 None。
    """
    match = re.match(r"sp500_(\d{8})\.csv", filename)
    if match:
        date_str = match.group(1)
        return datetime.strptime(date_str, "%Y%m%d")
    return None

def load_sp500_file(file_path):
    """
    读取某个 sp500_YYYYMMDD.csv 文件，返回一个 DataFrame。
    根据你的文件格式，可自行决定要读取哪些列、如何命名等。
    """
    df = pd.read_csv(file_path)
    # 假设你的文件有 "ticker"、"sector"、"weight" 等列
    # 这里可以做一些列的清洗或重命名
    return df

def find_next_available_file(file_list, target_date):
    """
    在 file_list (已按日期升序排序) 中，找出日期 >= target_date 的第一项。
    如果找不到，返回 None。
    
    参数:
    - file_list: [(date, file_path), ...] 已按 date 排序
    - target_date: datetime, 目标日期
    
    返回:
    - (found_date, file_path) 或 None
    """
    for d, path in file_list:
        if d >= target_date:
            return d, path
    return None

def generate_trading_days(start_date, end_date):
    """
    生成 start_date ~ end_date 间的所有“交易日”列表，
    这里仅简单排除周末。如果要精准处理美股节假日，需要引入交易所日历。
    """
    current = start_date
    trading_days = []
    while current <= end_date:
        # weekday(): Monday=0, Sunday=6
        if current.weekday() < 5:  # 只要不是周六日，就当作交易日
            trading_days.append(current)
        current += timedelta(days=1)
    return trading_days

def get_sp500_data_in_range(folder, start_date_str, end_date_str):
    """
    主函数：
    1. 扫描 folder 下所有 sp500_YYYYMMDD.csv 文件，解析日期并排序
    2. 根据用户输入的开始日期和结束日期，生成交易日列表
    3. 对每个交易日，如果没有对应文件，就用后面最近的文件
    4. 合并所有读取的DataFrame，并加一列 "logic_date" 标明逻辑日期
    
    返回: 合并后的DataFrame (或 None)
    """
    start_date = pd.to_datetime(start_date_str)
    end_date = pd.to_datetime(end_date_str)
    
    # 1. 收集并排序文件列表
    file_list = []
    for filename in os.listdir(folder):
        if filename.endswith(".csv"):
            file_date = extract_file_date(filename)
            if file_date is not None:
                full_path = os.path.join(folder, filename)
                file_list.append((file_date, full_path))
    file_list.sort(key=lambda x: x[0])  # 按日期升序
    
    # 如果没有文件，直接返回 None
    if not file_list:
        print("No files found in folder.")
        return None
    
    # 2. 生成交易日列表
    trading_days = generate_trading_days(start_date, end_date)
    
    # 3. 逐日查找文件并读取
    all_data = []
    for day in trading_days:
        found = find_next_available_file(file_list, day)
        if found is None:
            # 如果连 day 之后的文件都找不到，就跳过或自行处理
            # print(f"No file available on or after {day.strftime('%Y-%m-%d')}")
            continue
        
        actual_date, path = found
        df = load_sp500_file(path)
        
        # 增加一列 logic_date，表示“对用户来说是这一天的数据”
        df["logic_date"] = day.date()  # 或者保留 datetime
        # 如果需要知道实际文件日期，也可以额外存一列
        df["file_date"] = actual_date.date()
        
        all_data.append(df)
    
    # 4. 合并所有DataFrame
    if not all_data:
        print("No valid data found in the specified date range.")
        return None
    
    result_df = pd.concat(all_data, ignore_index=True)
    return result_df

# ====================
# 示例调用
if __name__ == "__main__":
    daily_folder = r"C:\Users\Wyuzh\Desktop\Capstone\biasafe\archive\daily\daily"
    start_date_str = "2012-04-30"
    end_date_str = "2012-5-3"
    
    final_df = get_sp500_data_in_range(daily_folder, start_date_str, end_date_str)
    

    if final_df is not None:
        print(final_df.head())
        print(f"Rows in final DataFrame: {len(final_df)}")
        # 你也可以保存到CSV
        final_df.to_csv(r"C:\Users\Wyuzh\Desktop\Capstone\biasafe\archive\consolidated.csv", index=False)
