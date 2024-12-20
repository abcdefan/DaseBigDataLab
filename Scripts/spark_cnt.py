import sys
import time  # 导入 time 模块
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, col, lower

# 检查参数
if len(sys.argv) != 3:
    print("Usage: spark-submit wordcount.py <input_file> <output_file>")
    sys.exit(1)

# 获取命令行参数
input_file = sys.argv[1]  # 输入文件路径
output_file = sys.argv[2]  # 输出文件路径

# 记录开始时间
start_time = time.time()

# 创建 SparkSession
spark = SparkSession.builder \
    .appName("WordCount") \
    .getOrCreate()

# 读取 HDFS 上的文本文件
df = spark.read.text(input_file)

# 将每行文本按空格分割成单词
words_df = df.select(explode(split(lower(col("value")), r"\s+")).alias("word"))

# 过滤掉空字符串
filtered_words_df = words_df.filter(col("word") != "")

# 统计词频
word_count_df = filtered_words_df.groupBy("word").count()

# 按词频降序排序
sorted_word_count_df = word_count_df.orderBy(col("count").desc())

# 输出结果到 HDFS
sorted_word_count_df.write.csv(output_file, mode="overwrite")

# 打印前 10 个词频结果
sorted_word_count_df.show(10)

# 停止 SparkSession
spark.stop()

# 记录结束时间
end_time = time.time()

# 计算并打印总执行时间
total_time = end_time - start_time
print(f"Total execution time: {total_time:.2f} seconds")