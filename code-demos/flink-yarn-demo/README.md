# Flink 生产级流计算实战工程：Source、计算、Sink 全生命周期架构 (flink-yarn-demo)

> 本工程面向 **数据平台研发负责人 / 大数据架构师 / 技术总监** 岗位面试中的“实时流计算全特性实操”、“Source/计算/Sink 全链路概念覆盖”、“零废弃现代 Flink 1.20+ / 2.0 规范”与“YARN 集群生产部署运维”考察。  
> 涵盖 10 个完整的生产级实战作业、覆盖 Flink 所有的核心代码概念、单元测试 100% 绿灯且经 `-Xlint:deprecation` 零警告校验。

---

## 一、全景架构：Source 端 -> 计算端 -> Sink 端 全生命周期版图

```mermaid
flowchart LR
    subgraph S["【Source 端】接入与切片发现"]
        S1["FLIP-27 架构<br>(DataGeneratorSource / SplitEnumerator)"]
        S2["Watermark 生成策略<br>(乱序容忍 / 单调递增 / 标点式)"]
        S3["防流卡死机制<br>(withIdleness 空闲检测)"]
        S4["物理传输分区器<br>(keyBy / rebalance / rescale / partitionCustom)"]
    end

    subgraph C["【计算端】流计算与状态时序核心"]
        C1["多流协同计算<br>(connect / union / 广播规则流)"]
        C2["状态体系与治理<br>(Value/List/Map/ReducingState + TTL)"]
        C3["时间与定时器<br>(TimerService 最小堆去重 / 超时下发)"]
        C4["高阶窗口引擎<br>(增量聚合 + 自定义 Trigger + Evictor)"]
        C5["异步 I/O 维表关联<br>(unorderedWait + Caffeine 本地缓存防击穿)"]
        C6["复杂事件处理 CEP<br>(NFA 状态机 + 模式超时捕获)"]
        C7["极端倾斜治理<br>(流式两阶段加盐打散 Salting)"]
        C8["多级侧输出流<br>(OutputTag 异常兜底 / 分流清洗)"]
    end

    subgraph K["【Sink 端】输出与端到端一致性"]
        K1["现代 SinkV2 架构<br>(SinkWriter + Committer 解耦)"]
        K2["端到端 Exactly-Once<br>(两阶段提交 2PC 状态机)"]
        K3["幂等写入与 At-Least-Once<br>(主键去重 / 状态覆盖)"]
        K4["微批缓冲与优雅排空<br>(Batching + Flush on Close)"]
        K5["死信队列与容错重试<br>(DLQ + 指数退避)"]
    end

    S --> C --> K
```

---

## 二、源码工程目录结构与全量覆盖矩阵

```text
flink-yarn-demo/
├── pom.xml                                      # 模块构建配置 (Java 17, Flink 1.20.1, flink-cep)
├── bin/                                         # YARN 集群运维与提交脚本
│   ├── submit-yarn-application.sh               # [推荐] YARN Application 模式一键提交脚本
│   ├── submit-yarn-session.sh                   # YARN Session 模式交互调试提交脚本
│   └── stop-yarn-job.sh                         # 触发 Savepoint 优雅停机脚本
├── src/
│   ├── main/java/com/tech/interview/flink/
│   │   ├── DualStreamReconciliationJob.java     # [旗舰] 双流对账、Watermark 协同与 EventTime 定时器
│   │   ├── OrderRiskStreamingJob.java           # 滚动窗口增量聚合 (Aggregate+ProcessWindowFunction)
│   │   │
│   │   ├── source/                              # 【Source 端核心实现】
│   │   │   ├── CustomPunctuatedWatermarkJob.java# 标点符号式 (VIP立即触发) vs 周期性 Watermark 对比
│   │   │   └── StreamPartitioningDemoJob.java   # 7 种物理分区策略对照 (rebalance/rescale/partitionCustom)
│   │   │
│   │   ├── compute/                             # 【计算端核心实现】
│   │   │   ├── broadcast/                       # 广播状态模式 (Broadcast State Pattern)
│   │   │   │   ├── DynamicBroadcastRuleJob.java # 动态风控规则热加载 (KeyedBroadcastProcessFunction)
│   │   │   │   └── RuleModel.java               # 规则定义模型 (增删改热生效)
│   │   │   ├── async/                           # 异步 I/O 维表关联
│   │   │   │   ├── AsyncEnrichmentStreamJob.java# unorderedWait + CompletableFuture 异步 RPC 补全
│   │   │   │   ├── LocalTtlCache.java           # 堆内高并发 LRU + TTL 本地缓存防击穿
│   │   │   │   └── EnrichedOrderEvent.java      # 补全后宽表模型
│   │   │   ├── cep/                             # 复杂事件处理 (Flink CEP)
│   │   │   │   ├── FraudDetectionCepJob.java    # NFA 状态机 (3次登录失败+敏感操作) + 超时匹配捕获
│   │   │   │   └── UserLoginEvent.java          # 安全登录事件模型
│   │   │   ├── window/                          # 高级窗口机制与触发器
│   │   │   │   └── AdvancedWindowTriggerJob.java# 自定义提前触发器 (FIRE/FIRE_AND_PURGE) 与会话窗口
│   │   │   ├── state/                           # 状态原语与算子状态深度实践
│   │   │   │   └── StatePrimitivesDeepDiveJob.java# MapState点查点写 vs ListState vs Operator UnionListState
│   │   │   └── skew/                            # 极端数据倾斜治理
│   │   │       └── TwoStageSaltingJob.java      # 大促热点两阶段加盐打散与全局汇总
│   │   │
│   │   ├── sink/                                # 【Sink 端核心实现】
│   │   │   ├── ExactlyOnceSinkV2Job.java        # 现代 SinkV2 两阶段提交 (2PC) 事务状态机闭环
│   │   │   └── IdempotentBatchingSinkJob.java   # 现代 SinkV2 微批缓冲攒批写入与主键幂等去重
│   │   │
│   │   ├── metrics/                             # 【横向切面监控】
│   │   │   └── CustomMetricsJob.java            # Counter/Gauge/MeterView 指标度量与反压公式剖析
│   │   │
│   │   ├── model/                               # 核心实体模型 (OrderEvent, PaymentEvent, ReconcileResult 等)
│   │   └── function/                            # 算子实现类 (OrderPaymentCoProcessFunction 等)
│   │
│   └── test/java/com/tech/interview/flink/      # 全量单元测试 (10 个测试用例，100% 绿灯)
│       ├── DualStreamReconciliationJobTest.java # 双流对账测试
│       ├── OrderRiskStreamingJobTest.java       # 增量风控窗口测试
│       ├── PunctuatedWatermarkTest.java         # 标点式 Watermark 测试
│       ├── LocalTtlCacheTest.java               # 本地 LRU+TTL 缓存测试
│       ├── TwoStageSaltingTest.java             # 两阶段加盐算法测试
│       └── SinkPayloadSerializerTest.java       # SinkV2 2PC 事务凭据序列化测试
└── README.md
```

---

## 三、Source 端、计算端、Sink 端 核心机制与底层哲学

### 1. 【Source 端】Watermark 推进与分区拓扑
- **标点式（Punctuated）vs 周期性（Periodic）**：
  - 周期性依赖 `pipeline.auto-watermark-interval`（默认 200ms）在 `onPeriodicEmit` 触发；
  - 标点式在特定业务关键事件到达（如高优 VIP 订单、批次结束标记）时在 `onEvent` 中立即调用 `output.emitWatermark()`，实现毫秒级触发下游关键窗口计算；
- **空闲分区防挂起（`withIdleness`）**：
  - 下游多通道对齐遵循 $\min(W_1, W_2, ...)$。若某分区夜间无数据，下游 Watermark 彻底被锁死。配置 `withIdleness(Duration)` 让 Flink 自动忽略空闲分区；
- **7 种物理分区语义与网络开销**：
  - `rebalance()`：跨机器全局轮询（Round-Robin），消除倾斜但网络开销高；
  - `rescale()`：本地 Slot 内部轮询，避免跨机器 Shuffle，网络开销极低；
  - `forward()`：严格 1:1 本地直连，零序列化与零跨网络开销；
  - `partitionCustom()`：按特定租户或业务分流定制隔离。

---

### 2. 【计算端】状态治理、时序调度与复杂计算
- **广播状态模式（Broadcast State Pattern）**：
  - 低频控制规则流调用 `.broadcast(MapStateDescriptor)` 转化为广播流；
  - 业务流在 `processElement` 中只读读取 `ReadOnlyBroadcastState`，控制流在 `processBroadcastElement` 中独占写入，实现**毫秒级规则热加载且零停机**；
- **原生状态类型避坑指南**：
  - 严禁滥用 `ValueState<Map>`，否则单 Key 修改会引发全量反序列化与网络/磁盘风暴；
  - 必须采用 `MapState<UK, UV>`，底层直接映射到 RocksDB 的单个 Column Family Key，点查点写性能高出百倍；
  - 算子状态恢复：`ListState` 均分恢复，`UnionListState` 广播恢复（Kafka 消费位点恢复核心机制）；
- **异步 I/O（Async I/O）与本地缓存防击穿**：
  - 杜绝在 `map()` 中使用同步阻塞 RPC；
  - 使用 `AsyncDataStream.unorderedWait()` 消除队头阻塞（Head-of-Line Blocking）；
  - 算子内嵌 `LocalTtlCache`（堆内高并发 LRU + TTL），防击穿外部 Redis/HBase，配合 `timeout()` 优雅兜底降级；
- **复杂事件处理（Flink CEP）**：
  - 采用非确定有限状态自动机（NFA），利用 `SharedBuffer` 共享状态树避免笛卡尔积状态膨胀；
  - 必须设置 `within(Duration)` 进行状态剪枝，通过 `TimedOutPartialMatchHandler` 捕获超时未达成的部分匹配模式（如连续登录失败但超时未转账）；
- **极端数据倾斜治理（Two-Stage Key Salting）**：
  - 阶段一：原始 Key 拼接随机盐值 `keyBy(shopId + "_" + random(N))` 进行局部微批预聚合；
  - 阶段二：剥离盐值还原原始 Key，由全局窗口进行二次归并汇总，彻底化解单 TaskManager 100% CPU 瓶颈。

---

### 3. 【Sink 端】现代 SinkV2 架构与端到端 Exactly-Once
- **现代 SinkV2 解耦架构（FLIP-191）**：
  - 彻底摒弃已被官方废弃的 `TwoPhaseCommitSinkFunction` 与 `RichSinkFunction`；
  - 实现 `Sink<InputT>` 与 `SupportsCommitter<CommT>`；
  - 写入逻辑收敛在 `SinkWriter`，最终提交逻辑收敛在 `Committer`，生命周期清晰解耦；
- **两阶段提交（2PC）状态机闭环**：
  - *Phase 1 (Pre-commit)*：Checkpoint Barrier 到达 SinkWriter，触发 `prepareCommit()` 生成事务凭据持久化进快照；
  - *Phase 2 (Final Commit)*：JobManager 确认所有算子 Checkpoint 成功后通知 Committer 执行 `commit()`；若故障则触发回滚；
  - *超时约束*：Kafka `transaction.max.timeout.ms` 必须严格大于 Flink Checkpoint 超时加上故障恢复耗时；下游配置 `read_committed`；
- **微批缓冲与幂等写入（Idempotent Sink）**：
  - 内存队列按批次大小（如 500 条）和时间间隔（如 1000ms）双触发微批写入；
  - 基于业务主键 `orderId` 幂等覆盖，实现高吞吐下的等效 Exactly-Once。

---

### 4. 【横向切面】可观测性与反压公式
- **四大度量类型**：`Counter`（累加计数）、`Gauge`（瞬时队列/内存）、`MeterView`（实时 TPS）、`Histogram`（P99 延迟）；
- **FLIP-33 / FLIP-123 现代反压公式**：
  $$\text{BackPressureRatio} = \frac{\text{backPressuredTimeMsPerSecond}}{1000}$$
  $$\text{BusyRatio} = \frac{\text{busyTimeMsPerSecond}}{1000}$$
  $$\text{IdleRatio} = \frac{\text{idleTimeMsPerSecond}}{1000}$$

---

## 四、YARN 资源管理下的提交与运维

### 1. 生产首选：YARN Application 模式一键提交
```bash
# 提交旗舰双流对账作业
./bin/submit-yarn-application.sh reconcile

# 提交实时风控增量窗口作业
./bin/submit-yarn-application.sh risk
```

### 2. 生产优雅停机与 Savepoint 保存
```bash
./bin/stop-yarn-job.sh application_1688888888888_0001 <flink-job-id>
```

---

## 五、编译与质量验证保障

本工程全量代码均通过以下三级质检：
1. **Javac 废弃检查（`-Xlint:deprecation`）**：
   ```bash
   mvn clean compile -Dmaven.compiler.showDeprecation=true -Dmaven.compiler.compilerArgument=-Xlint:deprecation
   ```
   **结果：24 个源文件全部通过，0 警告，0 错误**。
2. **全量单元测试（`mvn clean test`）**：
   覆盖 Source、Compute、Sink 核心逻辑，10 个测试用例全部绿灯。
3. **知识库自动化三维质检**：
   ```bash
   python3 scripts/verify_knowledge_base.py
   ```
   确保 0 失效引用、0 本机绝对路径硬编码。
