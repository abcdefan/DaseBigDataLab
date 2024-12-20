import os
import time
import subprocess

# 定义输入文件和输出目录
INPUT_FILES = ["ch_input_1", "ch_input_2", "ch_input_3"]
OUTPUT_DIR = "/user/dase-dis/ch_output"

# 定义 Spark 提交命令及配置
SPARK_SUBMIT = "/usr/local/spark/bin/spark-submit"
MASTER = "spark://10.24.1.114:7077"
DRIVER_MEMORY = "1g"
EXECUTOR_MEMORY = "1g"
TOTAL_CORES = "2"
PY_SCRIPT = "/home/dase-dis/wst_test/spark_cnt.py"
EVENT_LOG_DIR = "file:/tmp/spark-events"

# 删除并创建输出目录
def setup_output_dir():
    # 删除目录
    subprocess.run(["hadoop", "fs", "-rm", "-r", "-f", OUTPUT_DIR])
    # 创建目录
    subprocess.run(["hadoop", "fs", "-mkdir", "-p", OUTPUT_DIR])

# 运行 Spark 任务并返回执行时间
def run_spark_task(input_file):
    start_time = time.time()
    subprocess.run([
        SPARK_SUBMIT,
        "--master", MASTER,
        "--driver-memory", DRIVER_MEMORY,
        "--executor-memory", EXECUTOR_MEMORY,
        "--total-executor-cores", TOTAL_CORES,
        "--conf", "spark.eventLog.enabled=true",
        "--conf", f"spark.eventLog.dir={EVENT_LOG_DIR}",
        PY_SCRIPT,
        f"/user/dase-dis/{input_file}.txt",
        OUTPUT_DIR
    ])
    end_time = time.time()
    return end_time - start_time

# 确保事件日志目录存在
def setup_event_log_dir():
    event_dir = EVENT_LOG_DIR.replace("file:", "")
    if not os.path.exists(event_dir):
        os.makedirs(event_dir)
        # 设置权限确保 Spark 可以写入
        os.chmod(event_dir, 0o777)

# 主函数
def main():
    # 确保事件日志目录存在
    setup_event_log_dir()

    for input_file in INPUT_FILES:
        print(f"Processing input file: {input_file}")
        # 设置输出目录
        setup_output_dir()
        # 初始化总时间
        total_time = 0
        # 运行任务 3 次
        for i in range(1, 4):
            print(f"Running iteration {i} for {input_file}...")
            execution_time = run_spark_task(input_file)
            print(f"Iteration {i} execution time: {execution_time:.2f} seconds")
            total_time += execution_time
        # 计算平均执行时间
        average_time = total_time / 3
        print(f"Average execution time for {input_file}: {average_time:.2f} seconds")
        print("--------------------------------------------------")

if __name__ == "__main__":
    main()