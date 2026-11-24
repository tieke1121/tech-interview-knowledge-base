# 数据库并发与分布式协议故障实验

> [!NOTE] 案例性质说明
> 使用构造数据进行本地教学实验，结果不代表个人生产经历或生产容量。2026-11-24 实测 13 项全部通过，数据库为 PostgreSQL 17.11（ARM64），驱动为 psycopg 3.2.10。[原始结果](results/2026-11-24-postgres17.json)记录镜像 ID、版本、断点状态与恢复结果。

## 运行方法

需要 macOS/Linux、Python 3.10+、Docker CLI 和可用的 Docker daemon。脚本使用 POSIX SIGKILL；Windows 请在适配 Docker 的 Linux 环境运行。从仓库根目录执行：

```bash
python3 -m venv /tmp/interview-correctness-venv
/tmp/interview-correctness-venv/bin/python -m pip install -r code-demos/correctness-lab/requirements.txt
/tmp/interview-correctness-venv/bin/python code-demos/correctness-lab/run.py --report /tmp/interview-correctness-results.json
```

正常结束输出 `13 experiments passed`。断言不符、连接失败、未观察到锁等待或进程未到断点都会使程序失败。JSON 仅在全部通过且清理完成后写入；重跑失败时，旧 JSON 不代表本次成功，应检查退出码与时间。

默认使用 `postgres:17-alpine`，首次运行需要拉取镜像；可通过 `--image` 指定本机镜像 ID 或固定摘要。标签会更新，比较结果前先检查 JSON 中的实际版本。脚本创建随机名称的独立容器，限制内存 512 MiB，只绑定本机回环地址的随机端口，使用临时随机密码，不访问现有数据库。正常结束或可处理异常时清理该容器和匿名卷；强杀主程序或 Docker 失联时可能残留，可用以下命令查看并按名称确认后清理：

```bash
docker ps -a --filter label=purpose=interview-correctness-lab
```

本次已确认实验容器无残留。该 Python 实验独立运行，不属于 Maven 构建。

## 如何保证实验真的发生

[run.py](run.py)使用真实 PostgreSQL 连接和事务。库存实验先让 A 持锁，再让 B 发起更新，观察 `pg_stat_activity.wait_event_type=Lock` 后才释放 A。死锁实验让两个事务各持有一行锁，在屏障汇合后反向申请另一行。

故障实验由子进程执行订单、转发和消费步骤，到指定提交前后通过管道通知父进程并等待。父进程实际发送 SIGKILL，核对退出码，再读取已提交数据，恢复同一业务请求两次，并强制重新消费所有投递。这样可以精确打到提交间隙，不依赖“随机睡几秒后杀进程”。

订单与 Outbox 同事务；消费去重记录与余额变更同事务。消息传输由独立连接提交的 `deliveries` 表模拟，允许同一事件 ID 出现多条投递记录。

## 数据库并发：5 项实测

| 用例 | 操作 | 实际观察 |
| --- | --- | --- |
| stock_commit | 库存 1，A 条件扣减并提交，B 等待 | 观察到锁等待；B 影响 0 行，库存 0 |
| stock_rollback | 同上，A 改为回滚 | 观察到锁等待；B 影响 1 行，库存 0 |
| read_committed | A 读 1，B 改为 2 并提交，A 再读 | 两次读为 `[1,2]` |
| repeatable_read | 上例改为 RR | 两次读为 `[1,1]` |
| deadlock | 两事务反向更新两行 | 一方 SQLSTATE `40P01`，另一方提交；两行均由 1 变 2 |

库存操作为 `UPDATE stock SET quantity=quantity-1 WHERE id=1 AND quantity>=1`，显式使用 RC。死锁失败事务的第一步也被回滚；本用例没有实现自动重试。RC 与 RR 实验使用普通 SELECT，不能推断锁定读或其他引擎具有相同表现。[PostgreSQL 隔离级别](https://www.postgresql.org/docs/17/transaction-iso.html) · [锁与死锁](https://www.postgresql.org/docs/17/explicit-locking.html)

## 应用崩溃恢复：5 项实测

下表每个断点均真实 SIGKILL。状态数组依次为：**订单数、Outbox 数、投递数、去重记录数、余额、未确认投递数**。初始余额 0，每个业务请求增加 10。

| 断点 | 杀进程后、恢复前 | 完成恢复及重复消费后 |
| --- | --- | --- |
| before_producer_commit：订单事务提交前 | `[0,0,0,0,0,0]` | `[1,1,1,1,10,0]` |
| after_producer_commit：订单事务提交后、转发前 | `[1,1,0,0,0,0]` | `[1,1,1,1,10,0]` |
| after_publish：投递提交后、发送标记前 | `[1,1,1,0,0,1]` | `[1,1,2,1,10,0]` |
| before_consumer_commit：去重和余额写入后、事务提交前 | `[1,1,1,0,0,1]` | `[1,1,1,1,10,0]` |
| after_consumer_commit：消费提交后、确认前 | `[1,1,1,1,10,1]` | `[1,1,1,1,10,0]` |

关键证据是第三行：确实产生了两条投递，但余额保持 10。Outbox 允许重发，消费端事务去重才约束这里的业务副作用。[AWS Outbox 模式及重复投递说明](https://docs.aws.amazon.com/prescriptive-guidance/latest/cloud-design-patterns/transactional-outbox.html)

## 重复、乱序和旧写：3 项实测

| 用例 | 输入 | 实际观察 |
| --- | --- | --- |
| concurrent_idempotency | 6 个并发请求使用同一请求 ID、同一金额 | 1 个订单、1 个事件、1 条去重记录，余额 10 |
| version_and_tombstone | 依次写 v1、v2、旧 v1、v3 删除、旧 v2 | 受影响行数 `[1,1,0,1,0]`；保留 v3 删除标记 |
| fencing | 新持有者 token=2 先写，旧持有者 token=1 后写 | 条件更新拒绝旧写；移除校验的负例确实被旧写覆盖 |

版本实验要求同一实体的版本可比较，删除保留墓碑。fencing 使用 PostgreSQL sequence 生成本次运行内递增 token，资源端原子比较并保存 token。示例每个 token 只写一次，使用严格小于比较；生产中同一持有者多次写入及同 token 重试需要另定义操作身份，不能直接照搬。

## 求职者回答与面试官追问

**面试官：** 两个请求都查到还有一件库存，为什么仍然不会超卖？

**求职者：** 不能依赖先查后算。这个实验在 UPDATE 中同时判断库存和扣减。第二个连接真实进入锁等待；第一个提交后，第二个条件不成立，影响行数是 0。订单和扣库存仍需同一事务，还要防同一请求重复扣减。

**面试官：** 为什么死锁后不能只重试最后一条语句？

**求职者：** 本次一个事务收到 40P01，最终两行都只增加一次，说明失败事务的第一步也没有保留。需要重新开启并重做整个业务事务，限定次数与退避，并避免重复外部副作用。

**面试官：** Outbox 做了之后，为什么还看到两条消息？

**求职者：** 我在投递持久化后、标记发送前杀掉进程。恢复时同一个事件再次发送，投递数由 1 变 2；唯一事件 ID 和消费业务放在同一事务，余额保持 10。投递次数和业务生效次数必须分开数。

**面试官：** 消费者写完余额还没确认就退出呢？

**求职者：** 实测退出后余额已经是 10，确认仍未完成。恢复后查到去重记录，跳过金额更新再确认；如果去重和金额分开提交，就不能用这个结论保证安全。

**面试官：** 已经用锁了，为什么还要 fencing？

**求职者：** 资源必须能拒绝旧持有者的写入。本次新 token 先写后，旧 token 更新影响 0 行；移除资源校验后，旧写成功覆盖。实验验证的是资源端约束，租约过期和 Redis 故障还需要另测。

**架构师视角：** 把这些不变量作为发布验收项，同时监控锁等待、死锁、Outbox 事件年龄和去重冲突。实验没有测吞吐或租户隔离；上线还需设置连接预算、热点限流、有界重试与补偿告警，用压力数据决定资源投入。

## 验证边界与后续练习

本轮实际验证 PostgreSQL 并发、应用进程强杀及协议恢复。订单、传输模拟表、消费表共享一台 PostgreSQL，使用独立事务；没有覆盖网络分区、数据库断电、主从切换、磁盘损坏、Kafka Broker 故障、Redis 租约、MySQL 锁语义和 CDC 连接器。不能据此声称端到端跨系统 exactly-once 已验证。

请求负载固定且相同，没有测试“相同 ID、不同参数”冲突处理；没有测试去重记录过期、并发转发器抢占、事件永久失败和短信等外部副作用。生产化需补参数摘要校验、保留期、任务领取与对账协议。

下一次先遮住结果，预测每个提交点崩溃后的六个数字，再运行对照；随后练习解释“投递 2 次、业务生效 1 次”。对应[数据库题集](../../08-interview-questions/11-database.md)和[分布式正确性题集](../../08-interview-questions/12-distributed-correctness.md)。

后续已补充[真实 Kafka 故障实验](../kafka-fault-lab/README.md)，独立验证三节点强杀、网络隔离与消费提交间隙。该实验的外部消费库为 SQLite；本模块的 PostgreSQL 投递表仍保留为事务边界教学对照。
