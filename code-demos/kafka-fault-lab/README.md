# 真实 Kafka 故障、网络分区与数据正确性实验

> [!NOTE] 案例性质说明
> 本模块使用构造数据进行教学实验，不作为个人生产项目经历。Kafka 固定为 4.1.1，Python 客户端固定为 confluent-kafka 2.12.2。版本选择用于复现本实验，不表示最新版本或生产升级建议。

## 运行与资源范围

需要 macOS/Linux、Python 3.10+、Docker CLI 与可用 daemon。Docker 需能容纳额外三个各限 768 MiB 的容器；每个 Kafka JVM 堆为 256 MiB。Windows 可在适配 Docker 的 Linux 环境运行。

从仓库根目录执行：

```bash
docker pull apache/kafka:4.1.1
python3 -m venv /tmp/interview-kafka-venv
/tmp/interview-kafka-venv/bin/python -m pip install -r code-demos/kafka-fault-lab/requirements.txt
/tmp/interview-kafka-venv/bin/python code-demos/kafka-fault-lab/run.py --report /tmp/interview-kafka-results.json
```

[脚本](run.py)为每次运行创建唯一名称的 Docker 网络与三个容器，只发布回环地址的随机端口。使用三个同时承担 Broker 和 Controller 的 KRaft 节点，无 ZooKeeper；这便于本机教学，生产部署需评估角色隔离。所有 Topic 为单分区、三副本，`min.insync.replicas=2`，关闭非同步副本选主；生产者使用 `acks=all` 和幂等发送。内部位点和事务 Topic 也使用三副本、各三个分区。普通发送的交付超时为 30 秒、请求超时 3 秒、socket 超时及连接建立超时 5 秒，元数据定期刷新间隔 1 秒；多数派丢失用例把交付超时设为 12 秒，便于观察有限时间内的失败回调。这些是教学实验参数，不能直接当作生产基线。

成功退出输出 `5 experiments passed`，再写 JSON 报告。异常会失败退出；旧报告不代表新一轮成功。可用 `--image` 指定镜像 ID 或摘要，报告记录实际镜像 ID。脚本正常或异常退出会删除本次容器、匿名卷、网络和临时 SQLite 文件。若主程序被强杀或 Docker 失联，可先查看残留，核对名称后单独清理：

```bash
docker ps -a --filter label=purpose=interview-kafka-lab
docker network ls --filter label=purpose=interview-kafka-lab
```

## 拓扑和故障注入方法

主机 Python 客户端通过 EXTERNAL listener 访问 Broker；Broker 复制和 KRaft 选举通过独立 Docker bridge 网络上的 INTERNAL/CONTROLLER listener 通信。

- **进程故障**：查询实际分区 Leader，发送 Docker SIGKILL，检查退出码 137 和 OOM 标志。保留容器数据目录，随后启动同一容器。
- **单节点网络分区**：断开实际 Leader 的实验网络，核对容器仍运行且网络已脱离，观察其余两个节点选主和继续接收写入，然后重新连接网络。
- **多数派丢失**：断开两个 follower 的实验网络，各自与剩余节点隔离，尝试发送并等待交付回调；恢复网络后以相同业务 ID 重发。
- **消费提交间隙**：子进程将事件 ID 去重记录和余额更新放入同一 SQLite 事务，提交后通过管道屏障暂停；父进程 SIGKILL，确认 Kafka 尚无提交位点，然后恢复同一消费组。
- **事务可见性对照**：写入 5 条后 abort，再写入另 5 条后 commit，对照 read_committed 和 read_uncommitted。

Docker 断网会切断该节点与其他节点的双向通信，也影响经该网络发布的客户端访问。它模拟节点级网络隔离，不等于单向丢包或“只丢响应”的精细故障。断开两个混合角色节点时，数据复制和控制器多数派同时受影响，不能据此单独证明 `min.insync.replicas` 的拒写机制。[Docker 断开网络](https://docs.docker.com/reference/cli/docker/network/disconnect/) · [Kafka Broker 配置](https://kafka.apache.org/41/configuration/broker-configs/)

## 数据验收方法

前三项每个 Topic 使用业务 ID 0～39，每条金额 10。先等待前 20 条发送成功，再注入故障，恢复协议期间或之后写余下 20 条。最终从头消费至分区 EOF，逐条核对 ID 集合和金额，拒绝漏 ID、额外 ID 或错误负载。单 Leader 故障要求恰好 40 条；多数派故障允许超时记录最终存在，也允许业务重发产生重复，另记录重复数与按 ID 汇总金额 400。

超时不等于未写入；幂等 Producer 不会把新实例提交的相同业务 ID 自动识别为同一请求。多数派用例中的 `unique_amount` 是审计计算，不是外部数据库恰好一次承诺。[生产者配置](https://kafka.apache.org/41/configuration/producer-configs/)

消费崩溃用例单独检查真实外部数据库：10 个 ID 的金额应为 100。恢复前第一条已在 SQLite 提交，Kafka 位点未提交；恢复后再次收到这条，唯一键阻止重复加钱，最终 Kafka 提交位点为 10（下一条要读的位置）。脚本手动分配分区，验证持久化位点恢复，不包含消费组 Rebalance 协议。[Python 客户端 API](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html)

## 2026-11-24 实测结果

五项全部通过，完整证据见[JSON 报告](results/2026-11-24-kafka411.json)。环境为 Kafka 4.1.1、confluent-kafka 2.12.2、librdkafka 2.12.1、SQLite 3.49.2，镜像 ID 记录在报告中。

| 用例 | 实际状态与结果 |
| --- | --- |
| Leader 强杀 | Leader 2 被 SIGKILL，退出码 137、非 OOM；Leader 切至 3，ISR 为 `[1,3]`，重启后恢复三副本；40 条、40 个 ID、按 ID 金额 400 |
| Leader 网络隔离 | 节点 3 仍运行但已断网；Leader 切至 1，ISR 为 `[1,2]`；重连后恢复三副本；40 条、40 个 ID、按 ID 金额 400 |
| 两节点网络隔离 | 隔离节点 1、2；发送 ID=20 得到 `_MSG_TIMED_OUT`；重连并重发后 41 条、40 个 ID、重复 1 条、按 ID 金额 400 |
| 消费提交间隙 | 第一次数据库已提交 ID=0、金额 10，进程被 SIGKILL；Kafka 位点为 -1001（尚无已提交位点）；恢复读取 10 条，命中 1 次去重，最终金额 100、提交位点 10 |
| 事务可见性 | read_committed 仅读 ID 5～9，共 5 条；read_uncommitted 读 ID 0～9，共 10 条 |

三个故障 Topic 均无缺失 ID 或额外 ID，所有负载金额均核对为 10。强杀与单节点隔离的观测选主耗时分别约 8.410 秒和 8.835 秒，包含注入命令和轮询开销，不能用作生产性能指标。其他机器的 Leader 编号、耗时以及超时重发是否产生重复可能不同；验收依据是不变量，而不是强制复现这些偶然数值。

本次特别验证了“超时但消息实际留存”：多数派隔离场景的 41 条日志中有两个相同业务 ID。这里金额 400 是按 ID 审计的合计；外部数据库一次生效的证据来自单独的消费崩溃用例，最终余额为 100。

## 双角色面试：六道延伸练习

以下是围绕实验设计的延伸题，不声称为某家公司的逐字真题。

### 1. Leader 被强杀后，怎样证明已经确认的消息没有丢？

**求职者简答：** 先记录成功回调对应的业务 ID，再查实际 Leader 并强杀。待新 Leader 和 ISR 稳定后继续写，恢复旧节点，最后从头读取并逐个比较 ID 和金额。

**面试官追问：** 只检查总条数够吗？

**求职者深入回答：** 不够。少一条又多一条仍可能条数相同；所以本实验同时检查完整 ID 集合、每条负载与重复数量。三副本成功恢复也不能证明磁盘同时损坏时不丢数据。

**架构师视角：** 把已确认消息的 RPO、恢复写入时间和积压追赶时间分开验收。单次本机测量不能当作生产 P99 或 SLA。

### 2. 网络分区和进程退出有什么不同？

**求职者简答：** 网络隔离时进程可以仍在运行，但无法可靠联系其他节点。客户端不能只靠进程存活判断系统可用。

**面试官追问：** 旧 Leader 还活着，为什么不能让两边随意写？

**求职者深入回答：** 控制面依赖 KRaft 多数派管理元数据与领导权，分区数据写入还受 Leader、ISR 和确认条件约束。实验核对旧节点仍运行、网络已断开、新 Leader 不等于旧 Leader，并验证剩余两个副本接收写入。恢复后等待 ISR 回到三副本，再读数据对账。

**架构师视角：** 监控进程、控制器 quorum、离线分区、ISR 和请求错误，不能只部署端口探活。跨可用区放置要同时考虑延迟与共同故障域。

### 3. 三副本配 acks=all，就能一直写吗？

**求职者简答：** 不能。它需要当前协议要求的副本确认，也依赖可用 Leader 和控制面。本实验隔离两个节点后不再得到成功交付回调。

**面试官追问：** 这是 min ISR=2 导致的吗？

**求职者深入回答：** 本实验的 Broker 和 Controller 合并部署，隔离两个节点也破坏控制器多数派，因此不能只归因于 min ISR。若要单独验证它，需要独立 Controller 多数派保持健康，再只减少数据副本。acks=all 与 min ISR 的组合也不表示“任意两个副本答复即可”。

**架构师视角：** 接受短暂拒写通常比降低一致性条件后默默丢数据更容易对账。入口需背压、有限缓冲与有界重试，按租户隔离积压，避免恢复时重试风暴。[Topic 配置](https://kafka.apache.org/41/configuration/topic-configs/)

### 4. 发送超时后换一个 Producer 重发，为什么仍需要业务 ID？

**求职者简答：** 超时只是客户端没有得到成功结果，服务端可能已经写入。新 Producer 的重发可能成为另一条记录。

**面试官追问：** 同样的 key，Kafka 会自动去重吗？

**求职者深入回答：** 不会。key 用于分区等语义，普通日志仍允许相同 key。幂等发送处理协议重试；应用层重新提交、重启后换实例的业务重复需要稳定业务 ID 与下游唯一约束。本实验的多数派恢复允许超时请求重发，并报告真实重复数量。

**架构师视角：** 区分成功、明确拒绝和结果未知，保留业务身份与对账线索。重试必须有时间预算，不能因超时就生成新业务单号。

### 5. 数据库已提交、Kafka 位点没提交就退出，会重复扣钱吗？

**求职者简答：** 消息会重读，但可以让业务只生效一次：事件 ID 去重记录和金额修改同事务，数据库成功后再提交 Kafka 位点。

**面试官追问：** 你真的杀过进程，还是只抛异常模拟？

**求职者深入回答：** 父进程等到子进程报告数据库提交完成才 SIGKILL，并核对退出信号、余额和 Kafka 位点。恢复从持久化位点读取，重复事件经过唯一键检查跳过金额更新。示例用 SQLite 验证单消费者事务边界，不能替代远端 PostgreSQL 故障或多人消费的验证。

**架构师视角：** 相同 ID 不同参数应拒绝；去重保留期覆盖最长重放窗口。短信、支付等外部副作用还需下游幂等与查询契约。

### 6. read_committed 验证通过，能称端到端 exactly-once 吗？

**求职者简答：** 只能说明此 Kafka 事务的可见性符合预期，不能自动覆盖外部数据库。

**面试官追问：** read_uncommitted 能读到 abort 的消息，说明回滚失效了吗？

**求职者深入回答：** 事务回滚不等于立即擦除日志。read_committed 根据事务状态过滤已中止记录；read_uncommitted 可以看到它们。Consume-Transform-Produce 还要把输出和输入位点放进同一 Kafka 事务；本用例没有执行 sendOffsetsToTransaction，不能冒称验证了这个完整链路。

**架构师视角：** 验收文档必须写清保证范围：日志可见性、输入位点、数据库业务状态、外部接口分别怎样恢复。事务开销需用自己的消息大小和批次测量。

## 未覆盖的边界

当前为一台 Docker 主机上的三节点、小数据量教学实验。没有验证跨机房、宿主机断电、磁盘损坏、单向丢包、仅响应丢失、事务协调器中途故障、Consumer Rebalance、生产性能和恶意故障。已确认消息恢复结论只适用于本次注入路径；本实验不使用 Kubernetes。

## 调试记录：集群恢复与客户端恢复分开看

首轮中，Leader 强杀通过；网络隔离后，通过 Broker 内部管理命令已观察到新 Leader 和两个 ISR，但原生产者的后续 20 条消息全部报告 `_MSG_TIMED_OUT`，首轮失败并自动清理，没有生成成功报告。当时使用 12 秒交付超时，socket 与元数据刷新采用客户端默认值。

后续明确设置连接探测、socket 和元数据刷新参数，并把普通交付预算改为 30 秒。这个问题说明验收需要同时记录集群状态与客户端回调；仅凭新 Leader 出现就宣布恢复并不充分。由于多个参数一起调整，本实验不声称已经隔离出某一个参数的唯一因果作用。
