import subprocess
import time
import statistics
import os

def ensure_dir_exists(dir_path):
    """确保目录存在，如果不存在则创建"""
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

def run_hadoop_job(input_file, output_dir, round_num):
    # 删除输出目录
    subprocess.run(f"hadoop fs -rm -r {output_dir}", shell=True, stderr=subprocess.DEVNULL)

    # 构建Hadoop命令 - 修改了 -D 参数的位置和格式
    hadoop_command = f"""hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar \
        -D mapreduce.map.memory.mb=1024 \
        -D mapreduce.reduce.memory.mb=1024 \
        -D mapreduce.map.cpu.vcores=1 \
        -D mapreduce.reduce.cpu.vcores=1 \
        -D mapreduce.job.reduces=2 \
        -files /home/dase-dis/wst_test/mapreduce_cnt.py \
        -mapper "python3 mapreduce_cnt.py mapper" \
        -reducer "python3 mapreduce_cnt.py reducer" \
        -input {input_file} \
        -output {output_dir} \
        -numReduceTasks 2"""

    print(f"\nStarting Hadoop job for {os.path.basename(input_file)} - Round {round_num}")
    print("=" * 50)

    # 记录开始时间
    start_time = time.time()
    print(f"Start time: {time.strftime('%Y-%m-%d %H:%M:%S')}")

    # 执行命令
    process = subprocess.run(hadoop_command, shell=True, capture_output=True, text=True)

    # 记录结束时间
    end_time = time.time()
    print(f"End time: {time.strftime('%Y-%m-%d %H:%M:%S')}")

    # 计算执行时间
    execution_time = end_time - start_time
    print(f"Execution time: {execution_time:.2f} seconds")
    print("=" * 50)

    # 获取输入文件名(不含路径)
    input_filename = os.path.basename(input_file)

    # 保存标准输出和错误输出
    output_dir = "mapreduce_output"
    ensure_dir_exists(output_dir)

    # 构建输出文件名
    output_file = os.path.join(output_dir, f"{input_filename}_round{round_num}_output.txt")

    # 写入输出文件
    with open(output_file, 'w') as f:
        f.write("=== JOB INFORMATION ===\n")
        f.write(f"Input file: {input_file}\n")
        f.write(f"Round: {round_num}\n")
        f.write(f"Start time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))}\n")
        f.write(f"End time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time))}\n")
        f.write(f"Total execution time: {execution_time:.2f} seconds\n\n")
        f.write("=== STANDARD OUTPUT ===\n")
        f.write(process.stdout)
        f.write("\n=== STANDARD ERROR ===\n")
        f.write(process.stderr)

    # 检查任务是否成功
    if process.returncode != 0:
        print(f"Error in round {round_num} for {input_file}")
        print(process.stderr)
        return None

    return execution_time

def main():
    # 确保输出目录存在
    ensure_dir_exists("mapreduce_output")

    # 输入文件列表
    input_files = [
        "/user/dase-dis/ch_input_1.txt",
        "/user/dase-dis/ch_input_2.txt",
        "/user/dase-dis/ch_input_3.txt"
    ]

    # 结果存储
    results = []

    # 打开结果文件
    with open('mr_result.txt', 'w') as f:
        # 对每个输入文件执行3次
        for input_file in input_files:
            times = []
            f.write(f"\nProcessing {input_file}:\n")
            print(f"\nProcessing {input_file}:")

            # 执行3次
            for i in range(3):
                output_dir = f"/user/dase-dis/output_mr_{i}"
                print(f"Round {i+1}...")

                # 执行任务并获取执行时间
                execution_time = run_hadoop_job(input_file, output_dir, i+1)

                if execution_time is not None:
                    times.append(execution_time)
                    f.write(f"Round {i+1}: {execution_time:.2f} seconds\n")
                    print(f"Round {i+1} completed in {execution_time:.2f} seconds")

            # 计算平均执行时间
            if times:
                avg_time = statistics.mean(times)
                std_dev = statistics.stdev(times) if len(times) > 1 else 0
                f.write(f"Average execution time: {avg_time:.2f} seconds\n")
                f.write(f"Standard deviation: {std_dev:.2f} seconds\n")
                f.write("-" * 50 + "\n")

                results.append({
                    'file': input_file,
                    'times': times,
                    'avg': avg_time,
                    'std': std_dev
                })

        # 写入总结
        f.write("\nSummary:\n")
        for result in results:
            f.write(f"\nFile: {result['file']}\n")
            f.write(f"Individual times: {', '.join([f'{t:.2f}' for t in result['times']])} seconds\n")
            f.write(f"Average time: {result['avg']:.2f} seconds\n")
            f.write(f"Standard deviation: {result['std']:.2f} seconds\n")

if __name__ == "__main__":
    main()