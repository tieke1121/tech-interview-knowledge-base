---
id: "interview-database"
title: "数据库索引、事务与 CDC 双角色练习"
module: "08-interview-questions"
category: "database"
version_target: "MySQL 8.4 InnoDB；PostgreSQL 17"
doc_status: "已完成"
learning_status: "未学习"
importance: "P0"
last_reviewed: "2026-11-24"
tags: ["双角色面试", "数据正确性", "故障推演"]
---

# 数据库索引、事务与 CDC 双角色练习

> [!NOTE] 案例性质说明
> 数字、输入数据和事故为教学推演；个人经历以履历事实清单为准。公开面经用于确认考点，追问与参考回答由本库整理。

## 1. 知识概述与业务价值

重点回答查询为什么慢、并发修改怎样保证约束、故障后如何恢复，以及业务库变更如何进入数仓。先说明数据库、存储引擎、版本与隔离级别。

## 2. 核心概念与关系

索引负责定位记录，缓冲池减少磁盘访问，锁协调并发写，MVCC 提供版本可见性，日志参与恢复与复制。事务的一致性还依赖约束与正确的业务操作。

## 3. 底层工作机制与设计哲学

读取成本取决于扫描量、随机访问、排序、关联和结果传输。普通快照读、锁定读、UPDATE 的行为不同。隔离级别同名也不能直接假定不同数据库实现相同。

## 4. 高频复杂业务场景实操与落地

给定订单表 orders(id, tenant_id, status, created_at, amount)，推演租户最近订单查询。先估算筛选比例与返回行数，再用 EXPLAIN 验证候选索引。EXPLAIN ANALYZE 会实际执行受支持的语句，应在适合的环境和范围使用。

下面是可在独立测试库中按顺序验证的两会话时序，初始商品数量为 1，关闭自动提交后明确开始事务。这里给出预期，尚未执行 MySQL 实机并发实验。

| 顺序 | 会话 A | 会话 B | 应观察的结果 |
| --- | --- | --- | --- |
| 1 | BEGIN 后执行带 quantity>=1 条件的减一更新 | 暂不执行 | A 影响 1 行并持有相关锁 |
| 2 | 暂不提交 | BEGIN 后对同一商品执行同样更新 | B 等待 A 结束，前提是未超时 |
| 3 | COMMIT | 等待结束 | B 重新检查条件，影响 0 行；提交后库存为 0 |
| 变体 | 第 3 步改为 ROLLBACK | 同上 | B 可以减一，影响 1 行；B 提交后库存为 0 |

普通快照读的另一个实验是 A 在 RR 事务中首次 SELECT 得到 1，B 更新为 2 并提交，A 再做普通一致性 SELECT 仍为 1。A 换成 RC 时第二次读得到 2。此例 A 没有自身写入，表使用 InnoDB，开始前明确设置隔离级别；锁定读另行推演。

### PostgreSQL 已运行的对应实验

2026-11-24 在独立 PostgreSQL 17.11 容器中完成以下验证，运行方法与完整证据见[实验手册](../code-demos/correctness-lab/README.md)及[原始结果](../code-demos/correctness-lab/results/2026-11-24-postgres17.json)。上面的 MySQL/InnoDB 时序仍需在目标引擎验证。

| 实验 | 实测结果 | 能说明什么 |
| --- | --- | --- |
| RC 条件扣减，A 提交 | 观察到 B 等锁；B 影响 0 行，最终库存 0 | UPDATE 中的条件在等待结束后参与判定 |
| RC 条件扣减，A 回滚 | 观察到 B 等锁；B 影响 1 行，最终库存 0 | 未提交扣减不被当作最终业务结果 |
| RC 两次普通查询 | 先读 1，再读到 B 已提交的 2 | 此场景的语句快照变化 |
| RR 两次普通查询 | 两次都读 1 | 此场景沿用事务快照 |
| 两事务反向更新两行 | 一方收到 40P01，一方提交；两行都只增加一次 | 死锁受害事务的前序写入也未保留 |

**面试官追问：** 你怎样证明第二个事务真的等过锁？

**求职者回答：** 脚本先让 A 持锁，再在另一个连接执行 B 的 UPDATE，查询数据库活动视图确认 B 的等待类型为 Lock 后才释放 A。不是根据程序耗时猜测。死锁用两方屏障形成等待环，并检查数据库错误码和最终数据。

**面试官追问：** 这能证明库存系统已经正确了吗？

**求职者回答：** 证明了这里单行条件扣减的并发行为。订单与库存原子提交、重复请求、跨库补偿和高负载性能还需各自验证，不能把一个通过的用例扩大成整个业务系统的保证。

## 5. 生产异常故障深度排查与根因分析

慢查询先分锁等待、执行耗时与客户端传输；事务失败区分死锁、锁等待超时、唯一约束冲突和序列化失败。CDC 延迟查源端日志位置、连接器进度、下游提交和日志保留期。

## 6. 技术选型对比、架构演进与成本ROI

索引加快部分读操作，也增加写入、存储和维护成本。提高隔离强度可能增加等待或事务重试。把实际业务不变量写出来，再选择约束、条件更新、锁或串行化。

## 7. 技术经理与架构师实战管控

限制长事务与无界查询，变更前检查锁影响、复制积压及恢复路径。按租户限制连接与查询并发；批量回填分批提交，避免挤占在线事务。恢复演练需验证备份加日志能否重建目标时点。

## 8. 互联网大厂高频面试真题库

数据库索引、隔离级别与死锁主题见[阿里多场实习面经转载](https://www.nowcoder.com/feed/main/detail/bef78a96b48f4db6a09af4d8a449872e)，索引与日志追问见[字节商业化原帖](https://www.nowcoder.com/feed/main/detail/e56ca5688e3d4f64b7c91ba5f5f0cb71)。题干为考点改写，库存和 CDC 是本库扩展场景，技术答案以官方文档为依据。

1. **Q1** 联合索引怎样服务筛选和排序？
2. **Q2** MVCC 如何影响两次查询结果？
3. **Q3** 库存只剩一件，两个请求怎样避免超卖？
4. **Q4** 遇到数据库死锁应该如何处理？
5. **Q5** redo、undo、binlog 各负责什么？
6. **Q6** 全量快照和 CDC 增量怎样衔接？

## 9. 结构化深度参考答案

### 题目1　联合索引怎样服务筛选和排序？

#### 简洁回答

**求职者**　从 WHERE、ORDER BY、LIMIT 和返回列一起设计，确认等值前缀、范围与排序的关系，再看执行计划。

#### 深入回答

**面试官**　tenant_id 等值、status 等值、created_at 范围，还能按 id 排序吗？

**求职者**　索引 (tenant_id,status,created_at,id) 先按时间组织，同一时间内才按 id，不能直接假定满足全局 id 排序。还要看扫描量、回表和优化器实际选择；使用索引仍可能很慢。

#### 架构师/技术管理视角

用接近生产的数据分布比较扫描行数、延迟和写入成本，避免为每条 SQL 都加索引。

### 题目2　MVCC 如何影响两次查询结果？

#### 简洁回答

**求职者**　InnoDB RC 普通一致性读每次建立新快照，RR 通常沿用事务首次一致性读建立的快照，并能看到本事务修改。

#### 深入回答

**面试官**　RR 下 SELECT 与 UPDATE 都看到同一版本吗？

**求职者**　普通一致性读和锁定读或写操作的语义不同，混用需逐条推演。PostgreSQL RR 采用快照隔离并可能出现序列化异常，Serializable 需要处理重试。

#### 架构师/技术管理视角

在两会话中列出每步提交与查询结果，按实际引擎验证关键不变量，避免用“RR 就解决一切并发问题”作为设计依据。

### 题目3　库存只剩一件，两个请求怎样避免超卖？

#### 简洁回答

**求职者**　用带条件的原子更新，例如 UPDATE stock SET quantity=quantity-1 WHERE sku=? AND quantity>=1，再检查受影响行数。

#### 深入回答

**面试官**　已经扣库存，订单插入失败怎么办？

**求职者**　同库业务在同一事务里完成扣减与订单写入，失败整体回滚；相同业务请求还需唯一约束避免重复扣减。跨服务需定义预留、确认、取消及幂等。

#### 架构师/技术管理视角

热点库存仍会争锁。控制入口并发，核对业务失败与技术失败的重试策略，记录重复请求返回的结果。

### 题目4　遇到数据库死锁应该如何处理？

#### 简洁回答

**求职者**　保存死锁信息、相关 SQL 和事务顺序，识别等待环。缩短事务并统一访问顺序，增加合适索引以减少锁定范围。

#### 深入回答

**面试官**　数据库回滚了一个事务，应用只重试最后一条 SQL 可以吗？

**求职者**　应确认失败类型和事务状态。InnoDB 死锁受害事务被回滚，重试通常需重做完整业务事务；锁等待超时的回滚范围受设置影响。重试有界、带退避，并保证外部副作用可去重。

#### 架构师/技术管理视角

给死锁与重试建立指标，复盘热点和访问顺序。把发消息、远端调用放在长持锁事务中会增加不确定性。

### 题目5　redo、undo、binlog 各负责什么？

#### 简洁回答

**求职者**　InnoDB redo 用于崩溃恢复，undo 支持回滚和历史版本读取，MySQL binlog 记录复制及日志恢复所需的变更。

#### 深入回答

**面试官**　提交成功就必然抗住所有断电和主库丢失吗？

**求职者**　需核对日志刷盘、binlog 同步、硬件持久性和复制确认策略。单节点持久化与灾备 RPO 是不同约束。恢复需可用备份与连续日志，不能只有当前数据目录。

#### 架构师/技术管理视角

把可接受的数据丢失和恢复时间写入目标，演练恢复后按业务约束对账，评估更强持久性带来的延迟。

### 题目6　全量快照和 CDC 增量怎样衔接？

#### 简洁回答

**求职者**　需要连接器把快照读取与源端日志位置关联起来，跟踪快照期间的并发变更，并持久化消费进度。

#### 深入回答

**面试官**　先 SELECT 全表再从当前日志开始读行吗？

**求职者**　两者之间可能有遗漏；重叠读取又可能重复。按具体连接器的快照协议处理，并用主键、版本与删除语义保证重放。日志过期时需重新建立可信基线。

#### 架构师/技术管理视角

验证快照期间更新、删除、DDL、重启和长时间停机。下游按同一截止位点比对数量、金额及抽样记录。[Debezium MySQL 连接器](https://debezium.io/documentation/reference/stable/connectors/mysql.html)

技术机制核对入口。[InnoDB 锁](https://dev.mysql.com/doc/refman/8.4/en/innodb-locking.html) · [一致性读](https://dev.mysql.com/doc/refman/8.4/en/innodb-consistent-read.html) · [死锁处理](https://dev.mysql.com/doc/refman/8.4/en/innodb-deadlocks-handling.html) · [PostgreSQL 17 隔离级别](https://www.postgresql.org/docs/17/transaction-iso.html)

## 10. 知识自检与口述推演清单

- [ ] 一分钟说明输入、业务约束与结果。
- [ ] 面试官增加重复、乱序或崩溃后，能够重新推演。
- [ ] 指出需要的证据、恢复步骤与仍然存在的边界。

原理、证据、边界、实施各评 0～2 分，记录回答原句和需要复测的条件。实际作答后再更新学习状态。

## 11. 知识脉络与技术图谱关联

结合 [Flink](01-flink.md)、[Paimon](02-paimon.md)、[Doris](03-doris.md)、[Spring](06-spring.md)理解跨层边界。再做[双角色项目答辩](../07-project-review/03-interview-defense-playbook.md)。

## 12. 权威参考与官方进阶资料

[联合索引](https://dev.mysql.com/doc/refman/8.4/en/multiple-column-indexes.html) · [Redo](https://dev.mysql.com/doc/refman/8.4/en/innodb-redo-log.html) · [Undo](https://dev.mysql.com/doc/refman/8.4/en/innodb-undo-logs.html) · [Binlog 配置](https://dev.mysql.com/doc/refman/8.4/en/replication-options-binary-log.html)

[InnoDB 锁](https://dev.mysql.com/doc/refman/8.4/en/innodb-locking.html) · [一致性读](https://dev.mysql.com/doc/refman/8.4/en/innodb-consistent-read.html) · [死锁处理](https://dev.mysql.com/doc/refman/8.4/en/innodb-deadlocks-handling.html) · [PostgreSQL 17 隔离级别](https://www.postgresql.org/docs/17/transaction-iso.html)

PostgreSQL 并发实测见第 4 节；MySQL/InnoDB、CDC 和数据库节点故障尚未实测。机制依据见 [PostgreSQL 显式锁与死锁](https://www.postgresql.org/docs/17/explicit-locking.html)。
