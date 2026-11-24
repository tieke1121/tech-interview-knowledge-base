# 技术面试实战源代码管理库 (Code Demos)

> 使用 Java 17 和 Maven 在本地演练面试中的机制问题。各模块说明列出了输入、可观察的结果和集群运行条件。

---

## 一、模块目录矩阵与考察索引

| 模块目录 | 技术栈与版本 | 核心业务场景 | 核心底层机制与高频面试考点 |
| :--- | :--- | :--- | :--- |
| **[flink-yarn-demo](./flink-yarn-demo/README.md)** | Apache Flink 1.20.1 / Java 17 | 1. 订单与支付双流对账与超时关单预警<br/>2. 电商高并发实时交易风控指标聚合 | 多数据源协同 (Connect)、多输入 Watermark 取最小值 (`min(W1, W2)`)、空闲分区探测 (`withIdleness`)、事件时间定时器 (EventTime Timer)、两级侧道输出 (Side Output)、增量聚合与 RocksDB 增量快照、YARN Application 模式一键提交运维 |
| **[kafka-transaction-demo](./kafka-transaction-demo/README.md)** | Apache Kafka 3.7+ / Java 17 | 金融账务流水流转与跨 Topic 精准一次转换 | 幂等性与分布式事务（2PC）、Transaction Coordinator、`read_committed` 隔离级别、Rebalance 重平衡监听器与零拷贝参数调优 |
| **[spark-lakehouse-demo](./spark-lakehouse-demo/README.md)** | Apache Spark 3.5+ / Scala 2.13 | 湖仓一体实时链路与严重倾斜日志清洗 | 自适应查询执行（AQE 动态合并与倾斜 Join 优化）、广播 Hash Join 阈值控制、Shuffle Spill 机制与内存统一分配模型 |
| **[paimon-flink-demo](./paimon-flink-demo/README.md)** | Apache Paimon 0.9.0 / Flink 1.20.1 / Java 17 | 1. Partial-Update 按订单拼宽表<br/>2. 单表模拟 CDC 与 input Changelog 透传<br/>3. 从快照启动增量订阅<br/>4. Append-Only 纯追加归档与小文件自动合并 | LSM-Tree on Lakehouse、四大 Merge Engine（deduplicate, partial-update, aggregation, first-row）、四大 Changelog Producer（none, input, lookup, full-compaction）、Flink Checkpoint 与 Paimon Snapshot 原子协同、写合分离（Write-only + 异步 Compactor）治理 |
| **[java-distributed-demo](./java-distributed-demo/README.md)** | Java 17 / Redisson 3.28.0 / Spring 6.2.19（测试） | 异步数据补全、库存扣减及订单回滚后的审计记录 | Spring 事务代理与跨线程提交、线程池监控、`CompletableFuture` 异步编排与超时熔断降级、Redisson 看门狗分布式锁与 Lua 脚本原子操作 |

---

## 二、工程构建与规范指南

### 1. 环境依赖基线
- **JDK**: Java 17+ (LTS)
- **构建工具**: Apache Maven 3.8+ / 3.9+
- **目标计算底座**: Apache Hadoop YARN 3.3+ / Apache Kafka 3.6+ / Apache Spark 3.5+ / Redis 7.0+

### 2. 根工程统一构建
```bash
# 进入代码工程根目录
cd code-demos

# 编译并打包所有实战模块 (跳过测试快速构建)
mvn clean package -DskipTests
```

### 3. 从示例验证面试答案

先执行 `mvn test`。Flink 的两阶段加盐测试把乱序订单放在相邻的两个窗口，检查合计金额是否仍归到正确店铺与时间段。Paimon 的 CDC 测试运行示例入口，随后从快照读取最终余额。部署到集群时，依赖打包方式、存储路径与提交命令以对应模块说明为准。
