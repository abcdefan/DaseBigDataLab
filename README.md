# DaseBigDataLab
## 实验目的

利用经典的词频统计任务对比Spark和MapReduce的迭代性能

## 实验设计

任务: 词频统计

数据集

1. 500MB 约270万条数据
2. 2.1G 约1100万行数据
3. 8.6G 约4500万行数据

执行时间收集:

每个规模的数据集执行三次词频统计任务，取平均值作为最终执行时间

资源使用情况:

利用glances工具监控任务执行时间段内的资源使用情况

## 实验环境



## 执行时间

### spark

#### Input1

```JavaScript
Iteration 1 execution time: 43.55 seconds
Iteration 2 execution time: 43.54 seconds
Iteration 3 execution time: 43.11 seconds
Average execution time for ch_input_1: 43.40 seconds
```

#### Input2

```JavaScript
Iteration 1 execution time: 128.06 seconds
Iteration 2 execution time: 127.08 seconds
Iteration 3 execution time: 127.56 seconds
Average execution time for ch_input_2: 127.57 seconds
```

#### Input3

```JavaScript
Iteration 1 execution time: 459.98 seconds
Iteration 2 execution time: 464.54 seconds
Iteration 3 execution time: 458.04 seconds
Average execution time for ch_input_3: 460.86 seconds
```

### MapReduce

#### Input1

```
Round 1: 89.43 seconds
Round 2: 90.36 seconds
Round 3: 88.43 seconds
Average execution time: 89.41 seconds
```

#### Input2

```
Round 1: 212.39 seconds
Round 2: 212.85 seconds
Round 3: 216.09 seconds
Average execution time: 213.78 seconds
```

#### Input3

```
Round 1: 814.37 seconds
Round 2: 790.73 seconds
Round 3: 781.78 seconds
Average execution time: 795.63 seconds
```

## 资源使用情况

### Spark

#### Input1

CPU

![](./spark_input1_res/cpu_usage.png)

内存

![](./spark_input1_res/memory_usage.png)

IO

![](./spark_input1_res/disk_io.png)

#### Input2

CPU

![](./spark_input2_res/cpu_usage.png)

内存

![](./spark_input2_res/memory_usage.png)

IO

![](./spark_input2_res/disk_io.png)

#### Input3

CPU

![](./spark_input3_res/cpu_usage.png)

内存

![](./spark_input3_res/memory_usage.png)

IO

![](./spark_input3_res/disk_io.png)

