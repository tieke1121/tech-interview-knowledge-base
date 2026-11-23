# Spark 3.5+ 湖仓一体自适应执行 (AQE) 与倾斜治理工程 (spark-lakehouse-demo)

> 本工程面向 **数据平台研发负责人 / 大数据架构师** 岗位面试中的“Spark 内核调优”、“千亿级数据倾斜治理实战”与“湖仓一体 Shuffle 优化”考察。  
> 通过一套完整的 **电商大促热点倾斜关联场景**，系统拆解 Spark 3.5+ AQE 动态优化机制、统一内存模型与大厂高频真题。

---

## 一、业务场景与架构设计

在千亿级点击流与交易湖仓事实表关联中，**长尾 Task 导致整个 Stage 卡死在 99%** 是最经典的生产痛点：
- **场景**：90% 的用户点击集中在少数头部大促超级爆款商品上（如 iPhone 16 发售），导致特定 Hash 分区数据量暴涨至数十 GB，而其余分区仅有几十 MB；
- **治理维度**：
  1. **方案 A（现代主推）**：开启 Spark 3.5+ AQE（Adaptive Query Execution），引擎在运行时自动切分倾斜 Task；
  2. **方案 B（传统兜底）**：代码级“加盐两阶段打散”（Salting），通过将倾斜 Key 拼接随机数、维表对应膨胀 N 倍实现算力均衡；
  3. **方案 C（广播消除）**：当维表在广播安全水位内时，强制使用 `BroadcastHashJoin` 彻底消除 Shuffle。

---

## 二、源码工程目录结构

```text
spark-lakehouse-demo/
├── pom.xml                                   # 模块 Maven 依赖 (Spark 3.5+ / Java 17)
├── src/
│   ├── main/
│   │   └── java/com/tech/interview/spark/
│   │       └── SparkAqeSkewJoinApp.java      # AQE 配置、加盐两阶段与广播关联主逻辑
│   └── test/
│       └── java/com/tech/interview/spark/
│           └── SparkAqeSkewJoinTest.java     # AQE 运行时配置与加盐关联单元测试
└── README.md                                 # 架构机制拆解与大厂真题手册
```

---

## 三、代码核心机制与底层原理深度剖析

### 1. Spark 3.5+ AQE 动态倾斜 Join 执行原理

```
[上游 Shuffle Write 阶段]
Partition 0: 50MB
Partition 1: 50MB
Partition 2 (倾斜): 500MB (达到 skewedPartitionFactor 阈值)
Partition 3: 50MB

                  ▼ [AQE 收集运行时 Stage 真实统计信息]

[下游 Shuffle Read 阶段 - AQE 动态重写物理计划]
- Task 0: 读取 Partition 0 (正常 Join)
- Task 1: 读取 Partition 1 (正常 Join)
- Task 2-1: 仅读取 Partition 2 的前 100MB  <---\ 自动切分
- Task 2-2: 仅读取 Partition 2 的中间 100MB <---- 倾斜大分区，
- Task 2-3: 仅读取 Partition 2 的后续 100MB <---/ 对应维表数据被复用
- Task 3: 读取 Partition 3 (正常 Join)
```

#### AQE 核心配置基线：
- `spark.sql.adaptive.enabled = true`：全局开启 AQE；
- `spark.sql.adaptive.skewJoin.enabled = true`：开启倾斜关联优化；
- `spark.sql.adaptive.skewJoin.skewedPartitionFactor = 5`：当某分区体积超过各分区中位数的 5 倍时，判定为倾斜；
- `spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes = 64MB`：只有达到该绝对体积才切分，避免误伤小分区。

---

### 2. Spark 统一内存管理模型（Unified Memory Manager）

面试中常考 *“Spark 内存是如何划分的？Execution 和 Storage 怎么动态借用？”*

```
+------------------------------------------------------------------------------------+
|                                JVM 堆内总内存 (On-Heap)                             |
|                                                                                    |
|  +--------------------+  +------------------------------------------------------+  |
|  | Reserved Memory    |  |               User Memory (用户自定义内存)             |  |
|  | (硬编码预留 300MB)  |  | (1 - spark.memory.fraction = 40%，存用户对象与元数据) |  |
|  +--------------------+  +------------------------------------------------------+  |
|                                                                                    |
|  +------------------------------------------------------------------------------+  |
|  |                      Spark Memory (框架统一内存，占比 60%)                    |  |
|  |  +------------------------------------+  +--------------------------------+  |  |
|  |  |    Storage Memory (缓存内存 50%)    |  |  Execution Memory (计算内存 50%)|  |  |
|  |  | (Dataset.cache(), RDD 缓存, 广播变量)|  | (Shuffle 聚合, 排序缓冲区)      |  |  |
|  |  +------------------------------------+  +--------------------------------+  |  |
|  +------------------------------------------------------------------------------+  |
+------------------------------------------------------------------------------------+
```

#### 动态借用与驱逐原则（Dynamic Borrowing Rule）：
1. **Execution 借 Storage**：如果 Execution 内存不足而 Storage 闲置，Execution 可以借用 Storage 空间；
2. **Storage 借 Execution**：反之，Storage 也可以借用 Execution 空间；
3. **关键差异（谁能赶走谁）**：
   - 当 Storage 借了 Execution 内存后，一旦 Execution 需要用，**Execution 可以强制将 Storage 缓存的数据写出到磁盘（Evict to Disk）抢回内存**！
   - **Storage 永远无法驱逐 Execution 占用的内存**，因为计算正在进行中，杀掉计算会导致 Task 失败。

---

## 四、高频互联网大厂真题与三层应答示范

### Q1：线上 Spark 任务频繁报 Exit Code 137 (OOM-Killed)，但堆内存监控一切正常，如何系统定位与排查？
- **简洁回答**：Exit 137 是操作系统内核根据 cgroups 内存配额对容器进行的物理强制杀死。堆内正常说明堆内存未溢出，元凶通常是**堆外内存（Overhead/Off-Heap）超限**（如 Shuffle Netty 缓冲泄露、广播变量解压缩或 JNI 本地动态库占用）。
- **深入回答**：K8s 或 YARN 为 Executor 分配的内存为 `spark.executor.memory + spark.executor.memoryOverhead`。若任务拉取了超大广播变量并在堆外反序列化，或 Python UDF（PySpark）在子进程中消耗了过大内存，亦或开启了堆外 Tungsten 内存（`spark.memory.offHeap.enabled=true`）但配额计算不足，整机物理内存突破了 Container 上限，直接触发内核 OOM-Killer 发送 `kill -9`。
- **架构师/管理视角**：调优 SOP：
  1. 调大堆外缓冲配额：将 `spark.executor.memoryOverheadFactor` 从默认的 0.1 调高至 0.2；
  2. 严控大维表广播：将 `spark.sql.autoBroadcastJoinThreshold` 从随意设置收紧至 20MB 以内，禁止开发盲目使用 `/*+ BROADCAST */` 强 Hint；
  3. 推进存算分离与流转批处理，降低单个 Executor 的物理压力。

### Q2：面对大促亿级数据倾斜，除了等待 AQE 自动切分，在架构设计层面有哪些体系化的解决方案？
- **简洁回答**：治理数据倾斜有四大经典架构策略：**过滤异常倾斜 Key**、**小表广播消除 Shuffle（Broadcast Join）**、**拆分倾斜 Key 单独处理**，以及**加盐两阶段打散（Salting Two-Stage Join）**。
- **深入回答**：
  1. *空值过滤*：日志中大量 `null` 或 `-1` 默认值导致单个 Reduce 崩溃，在 Join/GroupBy 前提前 `filter(key.isNotNull)` 或将空值赋予随机负数；
  2. *加盐两阶段打散*：对主表 Key 附加 `0~N` 随机数，对维表跨表笛卡尔积膨胀 N 份（如本工程 `executeManualSaltingJoin`），第一阶段局部关联，第二阶段去盐全局聚合；
  3. *倾斜 Key 拆流*：通过采样（Sample）识别出 TOP 5 的超级 Key，将原流拆分为两路：倾斜流用广播 Join，其余 99% 的普通数据走正常的 SortMergeJoin，最后 `union` 汇总。
- **架构师/管理视角**：AQE 能够解决 80% 的简单场景，但当数据倾斜达到数万倍且伴随多字段复合倾斜时，AQE 仍可能造成单个 Node 网络打满。技术负责人应在平台治理层建立**血缘元数据与倾斜预警门禁**，在离线调度与实时入仓阶段自动侦测倾斜分布并强制推行规范化 SOP。
