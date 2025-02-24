import pandas as pd

# 读取 CSV 文件
df = pd.read_csv(r"C:\Users\Wyuzh\Desktop\Capstone\biasafe\git\Yuzhou_Wang\Data Extract\changed_history.csv")

# 直接将 "Date" 列转换为日期格式，并覆盖原始数据
df["Date"] = pd.to_datetime(df["Date"], errors='coerce')

# 保存结果到一个新的 CSV 文件，不包含索引
output_file = r"C:\Users\Wyuzh\Desktop\Capstone\biasafe\git\Yuzhou_Wang\Data Extract\changed_history_converted.csv"
df.to_csv(output_file, index=False)

print(f"转换后的数据已保存到 {output_file}")
