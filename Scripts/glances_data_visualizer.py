import pandas as pd
import matplotlib.pyplot as plt
from datetime import timedelta
import os

csv_path = input('请输入csv文件路径: ')

# 检查并创建保存图表的目录
output_dir = input('请输入结果保存目录: ')
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# 读取 CSV 文件
df = pd.read_csv(csv_path)

# 转换时间戳为 datetime 格式
df['timestamp'] = pd.to_datetime(df['timestamp'])

# 计算相对时间（以秒为单位）
start_time = df['timestamp'].iloc[0]  # 获取第一个时间戳作为起始时间
df['relative_time'] = (df['timestamp'] - start_time).dt.total_seconds()  # 转换为秒

# 绘制 CPU 使用率并保存图表
plt.figure(figsize=(12, 6))
plt.plot(df['relative_time'], df['cpu_total'], label='CPU Total', color='blue')
plt.plot(df['relative_time'], df['cpu_user'], label='CPU User', color='green')
plt.plot(df['relative_time'], df['cpu_system'], label='CPU System', color='red')
plt.title('CPU Usage Over Time')
plt.xlabel('Time Elapsed (seconds)')
plt.ylabel('CPU Usage (%)')
plt.legend()
plt.grid()
plt.savefig(os.path.join(output_dir, 'cpu_usage.png'))
plt.close()

# 绘制内存使用情况并保存图表
plt.figure(figsize=(12, 6))
plt.plot(df['relative_time'], df['mem_used'], label='Memory Used', color='purple')
plt.title('Memory Usage Over Time')
plt.xlabel('Time Elapsed (seconds)')
plt.ylabel('Memory (Bytes)')
plt.legend()
plt.grid()
plt.savefig(os.path.join(output_dir, 'memory_usage.png'))
plt.close()

# 绘制 IO 使用情况并保存图表
plt.figure(figsize=(12, 6))
plt.plot(df['relative_time'], df['diskio_vda_read_bytes'], label='Disk Read', color='blue')
plt.plot(df['relative_time'], df['diskio_vda_write_bytes'], label='Disk Write', color='red')
plt.title('Disk I/O Over Time')
plt.xlabel('Time Elapsed (seconds)')
plt.ylabel('Bytes')
plt.legend()
plt.grid()
plt.savefig(os.path.join(output_dir, 'disk_io.png'))
plt.close()

print(f"所有图表已成功保存到 {output_dir} 目录下。")