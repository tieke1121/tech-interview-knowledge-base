---
id: "interview-doris"
title: "Doris 面试六问：看板选型、事务与慢查询"
module: "08-interview-questions"
category: "实时分析"
version_target: "Apache Doris 4.x（涉及旧版时单独说明）"
doc_status: "已完成"
learning_status: "未学习"
importance: "P0"
last_reviewed: "2026-11-24"
tags: ["Doris", "OLAP", "面经", "Query Profile"]
---

# Doris 面试六问：看板选型、事务与慢查询

## 1. 知识概述与业务价值

同一份订单明细既要支撑分钟级更新的看板，又要供分析师按条件筛选。面试时先说明业务到底要看汇总数字、找几笔订单，还是导出全部命中行。三种需求的瓶颈分别可能落在聚合、查找和结果传输。

## 2. 核心概念与关系

| 概念 | 在一条查询里的职责 |
| --- | --- |
| FE | 解析 SQL，制定分布式执行计划并协调执行。 |
| BE | 执行扫描、聚合、Join 和交换数据。经典部署还在本地保存数据副本。 |
| 分区与索引 | 分区按条件排除整片数据。倒排索引在剩余数据里定位符合条件的行。 |
| EXPLAIN 与 Query Profile | 前者显示计划，后者记录一次真实执行的行数、耗时和资源消耗。 |

存算分离部署另外使用 Meta Service 和共享存储，BE 可以用本地缓存服务热读。[系统架构](https://doris.apache.org/docs/4.x/features-architecture/system-architecture/) · [Query Profile](https://doris.apache.org/docs/4.x/query-acceleration/query-profile/)

## 3. 底层工作机制与设计哲学

一次筛选聚合通常先在 FE 做分区裁剪，再由 BE 扫描候选列，应用谓词或索引，做局部聚合后按需交换数据。前面少读一列、少送一批行，都会改变后面算子的成本。通过 Profile 判断耗时到底花在哪一段。[架构概览](https://doris.apache.org/docs/4.x/getting-started/what-is-apache-doris/) · [Query Profile](https://doris.apache.org/docs/4.x/query-acceleration/query-profile/)

## 4. 高频复杂业务场景实操与落地

> [!NOTE] 案例性质说明：以下大促订单看板为教学推演。

客户按门店和小时看实付订单金额，还能点进某个订单看最新状态。先定义退款、重复通知、订单跨日更新的口径，再决定原始事件如何保留、订单状态以什么键覆盖、小时汇总能容忍多久刷新延迟。验收同时记录查询 P95、源端变更到看板可见的时间、抽样核账结果。

## 5. 生产异常故障深度排查与根因分析

1. 同一条 SQL 突然变慢，先区分排队、执行和结果传输。只在高峰排队时，查并发和相邻租户任务。
2. 执行慢，先从 Profile 找长尾 Fragment。扫描字节变大，看分区裁剪和索引过滤行数。扫描正常但 Exchange 时间上升，看 Join 或聚合前后的数据量和节点分布。
3. 数据查得快但看板迟迟不更新，另查导入批次、事务可见状态和应用缓存，逐段比较时间戳。[Query Profile](https://doris.apache.org/docs/4.x/query-acceleration/query-profile/) · [导入事务](https://doris.apache.org/docs/4.x/key-features/load-transaction/)

## 6. 技术选型对比

保留开放文件、离线扫描和重算占主导时，可继续使用已有湖仓。经常按多维条件聚合并要求快速返回时，用代表性 SQL 压测 Doris。以文本相关性和搜索体验为主时，单独验证搜索功能。计算每千次查询成本时，连同索引构建、刷新、冗余存储与值班支出一起算。[Doris 能力概览](https://doris.apache.org/docs/4.x/getting-started/what-is-apache-doris/)

## 7. 技术经理与架构师实战管控

建议把前台看板、离线导出放到不同 Workload Group，给导出设置并发与队列限制。对于必须隔离计算资源的场景，可在存算分离部署中使用独立计算组，同时压测共享对象存储带宽。高峰降级时允许看板展示上次刷新时间，把全量导出异步排队。[Workload Group](https://doris.apache.org/docs/4.x/key-features/workload-group/) · [Compute Group](https://doris.apache.org/docs/4.x/key-features/compute-group/)

## 8. 互联网大厂高频面试真题库

六题均转述公开候选人记录中的议题。公司、轮次依发帖人描述，单篇面经无法给出出现频率。技术细节和压测建议是本库准备的答法。

1. **Q1** 美团大数据开发面经问，Doris 在项目里负责哪部分分析？[面经原帖](https://www.nowcoder.com/discuss/529339006056747008)
2. **Q2** 为营销分析选择存储时，怎么在 Doris、Hive、ES 之间做决定？[字节交易与广告一面，第 12 题](https://www.nowcoder.com/discuss/932587389027962880?sourceSSR=post&weFlow=true)
3. **Q3** 既然看板可以用 Doris，它支持事务到什么程度，能否承接典型 MySQL 事务负载？[字节业务中台一面，第 2 组追问](https://www.nowcoder.com/feed/main/detail/38b1ea994d73482ea2b893754c67190a)
4. **Q4** 营销数据量大，一条出数 SQL 很慢，怎样找到值得改的地方？[字节交易与广告一面，第 11、16 题](https://www.nowcoder.com/discuss/932587389027962880?sourceSSR=post&weFlow=true)
5. **Q5** 给 Doris 加倒排索引前，怎样判断瓶颈和索引收益？[字节交易与广告一面，第 17 题](https://www.nowcoder.com/discuss/932587389027962880?sourceSSR=post&weFlow=true)
6. **Q6** 索引已经建好，过滤后仍命中数亿行，怎样继续缩短整条链路的耗时？[字节交易与广告一面，第 18、19 题](https://www.nowcoder.com/discuss/932587389027962880?sourceSSR=post&weFlow=true)

## 9. 结构化深度参考答案

### 题目1　把 Doris 放在哪一层？

#### 简洁回答

适合放在分析服务层。以上面的看板为例，Doris 接收清洗后的订单明细和变更，用 SQL 回答哪些门店订单增加、退款金额如何变化。回答个人经历时，只把自己实际负责的链路讲成第一人称。

#### 深入回答

明细流水要保留同一订单的多次事件，可用 Duplicate Key。按订单号展示最新状态，可用 Unique Key，并处理乱序更新。已固定维度的汇总可考虑 Aggregate Key，先确认度量能否合并，例如金额可相加，日 UV 不能直接跨天相加。三者约定了遇到同一键时要保留什么数据。[数据模型](https://doris.apache.org/docs/4.x/key-features/data-model/)

#### 架构师/技术管理视角

先和业务确定退款归属日、历史纠错范围和查询并发，再选择表模型。验收时挑选发生支付、退款和重放的订单逐笔核对，能发现只看总耗时发现不了的错误。

### 题目2　如何做存储选型？

#### 简洁回答

同一组需求分别考量聚合查询、明细检索和离线重算。多维聚合和关联占主导时，我会先测 Doris。已有 Hive 链路能满足时效和预算时，保留它可以节约一次同步。相关性排序决定搜索体验时，把 ES 放入同场试验。

#### 深入回答

Doris 按列读数据、并行执行算子，可以选择分桶和 Join 策略。它也支持倒排索引，但文本分词、命中语义和响应时延都要用实际查询验证。大范围扫描只在深夜运行一次，与白天每秒几百次的小范围聚合，需要不同的测试负载。[Doris 架构](https://doris.apache.org/docs/4.x/features-architecture/system-architecture/) · [倒排索引](https://doris.apache.org/docs/4.x/key-features/inverted-index/)

#### 架构师/技术管理视角

抽取按真实比例混合的查询，带着同步写入同时回放。比较尾延迟、数据可见延迟和峰值机器数，另测冷缓存与单节点退出。只有收益覆盖迁移、运维和维护第二份数据的成本，再决定上线。

### 题目3　事务能保证什么？

#### 简洁回答

一次导入可以原子地发布一批记录。Doris 4.x 支持在同一数据库内把受支持的多条写操作组成显式事务，隔离级别为 READ COMMITTED。面向高频读改写的交易服务仍要核对并发控制和读回自己修改的需求。[事务文档](https://doris.apache.org/docs/4.x/data-operate/transaction/)

#### 深入回答

单个查询读取语句开始时已可见的数据。显式写事务有语句组合限制，不能随意把查询和 DDL 混在其中，也不能靠下一条语句读取本事务上一条尚未提交的修改。导入返回超时时要用原批次标识追查状态，区分已提交和已可见；换标签盲重发会让重试变成新批次。[事务语义](https://doris.apache.org/docs/4.x/data-operate/transaction/) · [导入生命周期](https://doris.apache.org/docs/4.x/key-features/load-transaction/)

#### 架构师/技术管理视角

资金扣减、库存预占由满足其事务要求的业务库处理。分析侧接收变更，按批次记录来源位置，给运营人员提供可见水位与核账差异。用重试、乱序、重复到达三组数据验证状态表和汇总表能否对齐。

### 题目4　慢 SQL 从哪里下手？

#### 简洁回答

先保留一次慢执行的查询 ID。用 EXPLAIN 看设计上会读哪些分区、怎么 Join，再在 Profile 中从最慢的扫描、交换、聚合或结果发送环节倒推。[EXPLAIN](https://doris.apache.org/docs/4.x/sql-manual/sql-statements/data-query/EXPLAIN/) · [Query Profile](https://doris.apache.org/docs/4.x/query-acceleration/query-profile/)

#### 深入回答

如果扫描所有日期，核对过滤谓词能否裁剪分区。若扫描量小但 Join 后行数暴涨，查关联键是否唯一、有没有漏写条件。多个实例中只有一个特别慢，就比较各实例输入行数识别热键。修改 Join、预聚合或分区谓词后，对照同一业务窗口的返回行数和金额，防止优化时意外丢行。[表结构优化](https://doris.apache.org/docs/4.x/query-acceleration/tuning/tuning-plan/schema-and-index-optimization/)

#### 架构师/技术管理视角

先修消耗最多集群时间的一组 SQL，兼顾核心看板的 P95。发布时同时观测其他租户，避免某条查询提速仅仅因为抢到了更多内存或并发。

### 题目5　什么证据支持建倒排索引？

#### 简洁回答

高频过滤条件能排除大多数候选行，但现有分区或排序只能排除很少数据时，值得试。比如经常按一个追踪编号在当天日志里找几十行。[倒排索引](https://doris.apache.org/docs/4.x/key-features/inverted-index/)

#### 深入回答

倒排表把值或词项映射到所在行。等值筛选和按词搜索要用与字段及分词方式相配的谓词。比较同一查询启用、禁用索引的 Profile，检查过滤行数、索引过滤耗时和剩余扫描量；给存量数据加索引还要确认旧数据的索引已经建成。[索引效果验证](https://doris.apache.org/docs/table-design/index/inverted-index) · [索引选择](https://doris.apache.org/docs/4.x/query-acceleration/tuning/tuning-plan/optimizing-table-index/)

#### 架构师/技术管理视角

先在有代表性的分区做试验，兼测常见值与稀有值。收益按查询频率折算，成本计入磁盘、构建时间和新增写入负载，再决定是否给所有分区建索引。

### 题目6　过滤后还剩几亿行怎么办？

#### 简洁回答

和使用方确认最终需要一个计数、一页样本，还是完整明细。定位到行之后仍要读取、聚合或传输它们，返回多少数据是下一步设计的约束。

#### 深入回答

看 Profile 里过滤后行数及结果发送耗时。固定维度的统计可以做预聚合或物化视图，提前规定刷新延迟；查列表时选择必要列，带稳定排序和分页；确实要全量明细时，按分区异步导出。若用户必须获得全部匹配行，缩小时间范围或采样需要得到业务同意。[Query Profile](https://doris.apache.org/docs/4.x/query-acceleration/query-profile/) · [物化视图能力](https://doris.apache.org/docs/4.x/getting-started/what-is-apache-doris/)

#### 架构师/技术管理视角

给交互查询与导出分别约定完成时间和队列预算。高峰期暂缓全量导出，保留任务 ID、快照或数据水位及重试进度。业务可以检查结果是否完整，也不会因浏览器超时重复提交。

## 10. 知识自检与口述推演清单

- [ ] 画出一次多维聚合经过 FE 和 BE 的顺序，并在图上标出分区裁剪和 Exchange。
- [ ] 对同一条慢查询，分别举出加索引会获益、不会明显获益的一组数据分布。
- [ ] 用一个反例解释批量导入事务为什么无法直接覆盖库存扣减的所有需求。

## 11. 知识脉络与技术图谱关联

索引和模型的进一步推导见 [Doris OLAP 讲义](../01-bigdata/08-doris-lakehouse/01-doris-realtime-olap.md)。开放表格式和历史重算见 [Iceberg 讲义](../01-bigdata/08-doris-lakehouse/02-iceberg-lakehouse-architecture.md)。

## 12. 权威参考与官方进阶资料

优先查与运行版本一致的三份文档。FE/BE 的部署边界看[系统架构](https://doris.apache.org/docs/4.x/features-architecture/system-architecture/)，导入和 SQL 事务的隔离边界看[事务](https://doris.apache.org/docs/4.x/data-operate/transaction/)，性能结论用一次真实执行的[查询剖析](https://doris.apache.org/docs/4.x/query-acceleration/query-profile/)复核。
