# 技术面试实战源代码管理库 (Code Demos)

> 本目录用于系统管理技术面试、上机实操（Live Coding）与白板演练所需的生产级真实业务代码工程与部署运维脚本。  
> 杜绝纯理论“纸上谈兵”，全面对齐现代化工程标准（Java 17、Maven 多模块管理、容器化与分布式集群部署）。

---

## 一、模块目录矩阵与考察索引

| 模块目录 | 技术栈与版本 | 核心业务场景 | 核心底层机制与高频面试考点 |
| :--- | :--- | :--- | :--- |
| **[flink-yarn-demo](./flink-yarn-demo/README.md)** | Apache Flink 1.19+ / Java 17 | 1. 订单与支付双流对账与超时关单预警<br/>2. 电商高并发实时交易风控指标聚合 | 多数据源协同 (Connect)、Watermark 动态对齐 (`min(W1, W2)`)、空闲分区探测 (`withIdleness`)、事件时间定时器 (EventTime Timer)、两级侧道输出 (Side Output)、增量聚合与 RocksDB 增量快照、YARN Application 模式一键提交运维 |
| **[kafka-transaction-demo](./kafka-transaction-demo/README.md)** | Apache Kafka 3.7+ / Java 17 | 金融账务流水流转与跨 Topic 精准一次转换 | 幂等性与分布式事务（2PC）、Transaction Coordinator、`read_committed` 隔离级别、Rebalance 重平衡监听器与零拷贝参数调优 |
| **[spark-lakehouse-demo](./spark-lakehouse-demo/README.md)** | Apache Spark 3.5+ / Scala 2.13 | 湖仓一体实时链路与严重倾斜日志清洗 | 自适应查询执行（AQE 动态合并与倾斜 Join 优化）、广播 Hash Join 阈值控制、Shuffle Spill 机制与内存统一分配模型 |
| **[java-distributed-demo](./java-distributed-demo/README.md)** | Java 17 / Redisson 3.28+ | 高吞吐数据补全网关与防超卖库存扣减 | 生产级动态监控线程池、`CompletableFuture` 异步编排与超时熔断降级、Redisson 看门狗分布式锁与 Lua 脚本原子原子操作 |

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

### 3. 设计哲学与面试落地要求
1. **真实生产级语义**：代码必须包含真实容错机制（如事务提交、异常重试、迟到数据分流、背压防护、快照状态落盘），严禁仅写 Demo 级别的简单打印。
2. **依赖隔离规范**：遵循流计算与分布式系统打包标准，宿主集群已包含的依赖统一标记为 `provided`，通过 `maven-shade-plugin` 打出极轻量 Fat JAR。
3. **运维脚本自闭环**：每个分布式计算模块均配备开箱即用的 Shell 部署运维脚本（涵盖集群提交、状态查询、Savepoint 触发与优雅停机）。
