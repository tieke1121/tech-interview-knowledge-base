---
id: "interview-sql-modeling"
title: "SQL 与数据建模双角色练习"
module: "08-interview-questions"
category: "sql-modeling"
version_target: "SQLite 3.25+ 可运行示例；迁移至 MySQL 8.4 / PostgreSQL 17 时核对方言"
doc_status: "已完成"
learning_status: "未学习"
importance: "P0"
last_reviewed: "2026-11-24"
tags: ["双角色面试", "数据正确性", "故障推演"]
---

# SQL 与数据建模双角色练习

> [!NOTE] 案例性质说明
> 数字、输入数据和事故为教学推演；个人经历以履历事实清单为准。公开面经用于确认考点，追问与参考回答由本库整理。

## 1. 知识概述与业务价值

从“每一行代表什么”开始，再确定时间口径、去重规则和关联关系。六个练习涵盖最新版本、连续行为、留存、排名、资金汇总和历史维度。

## 2. 核心概念与关系

业务实体、事件和快照具有不同粒度。订单号标识订单，支付流水号标识一次支付；订单表一行与多笔支付关联会增加行数。主键、时间、状态、金额单位共同决定指标含义。

## 3. 底层工作机制与设计哲学

先确定有效数据集，再分组或执行窗口计算。窗口函数保留行，聚合改变粒度。稳定排序需要业务允许的并列规则。删除记录应参与版本选择后再过滤，避免旧版本重新出现。

## 4. 高频复杂业务场景实操与落地

第 9 节六段 SQL 均含独立教学输入，可复制到 SQLite 执行。金额用整数分，日期使用统一业务时区的 ISO 日历日期。运行 `python3 scripts/verify_sql_exercises.py` 可复验文档 SQL 与预期结果。日期函数移植时核对目标数据库语法。

## 5. 生产异常故障深度排查与根因分析

金额翻倍先查关联前后行数与每键数量；留存偏高查重复活跃记录和分母；旧记录复活查删除过滤顺序；历史口径变化查维表版本和重算截止点。抽取一条业务实体逐层重建，再扩大验证。

## 6. 技术选型对比、架构演进与成本ROI

先按键聚合能避免多对多放大并减少传输，但会丢失明细。明细事实表保留可追溯事件，汇总表服务固定口径。宽表需要明确维度按当前值还是事件发生时取值，维护成本随更新方式变化。

## 7. 技术经理与架构师实战管控

评审门禁检查粒度、并列顺序、空值、时区、删除、迟到及分母成熟期。历史回填限制并发，先写隔离结果并对账，再发布给下游。所有示例为教学数据。

## 8. 互联网大厂高频面试真题库

连续登录主题见[高德数据开发一面](https://www.nowcoder.com/feed/main/detail/04691218474c4f1d9b8873d708f61892)，月度 Top3 主题见[瓜子数仓校招面经](https://www.nowcoder.com/discuss/353159108276723712)。本库将阈值改成三天和 Top2 以便验证。其余为业务建模补充题，留存、去重、退款和历史维度的输入均由本库设计。

1. **Q1** 同一业务键有多个版本，如何取最新有效行？
2. **Q2** 怎样找连续至少三天登录的用户？
3. **Q3** 次日留存率怎样定义并计算？
4. **Q4** 每店销售额前两名，如何处理并列？
5. **Q5** 一单多支付、多退款，如何计算净收款？
6. **Q6** 用户搬家后，历史订单应该属于哪个城市？

## 9. 结构化深度参考答案

### 题目1　同一业务键有多个版本，如何取最新有效行？

#### 简洁回答

**求职者**　先按业务键分组，按可靠版本倒序排序，保留第一条，再判断是否删除。

#### 深入回答

**面试官**　为什么不能先过滤删除记录？

**求职者**　先过滤会让已删除实体的旧版本再次成为第一条。示例还加入晚到旧版本，排序优先业务版本。seq 只是本例约定的并列顺序，生产中同版本内容冲突应告警或定义源端顺序。

```sql
WITH events(id, version, seq, amount, deleted) AS (
  VALUES ('A',1,1,100,0), ('A',2,2,80,0), ('A',1,3,100,0),
         ('B',1,4,50,0), ('B',2,5,50,1)
), ranked AS (
  SELECT *, ROW_NUMBER() OVER (
    PARTITION BY id ORDER BY version DESC, seq DESC
  ) AS rn FROM events
)
SELECT id, amount FROM ranked
WHERE rn = 1 AND deleted = 0 ORDER BY id;
```

预期结果按列顺序为 JSON 数组，`null` 表示 SQL NULL。

```json
[["A", 80]]
```

#### 架构师/技术管理视角

保留原始变更便于重建，规定版本可比较的范围；跨源独立序号不能直接混排。

### 题目2　怎样找连续至少三天登录的用户？

#### 简洁回答

**求职者**　先按用户与日期去重，再按日期编号。连续日期减去递增编号会得到同一分组键。

#### 深入回答

**面试官**　同一天登录十次、跨月、断一天怎么办？

**求职者**　去重消除同日重复；日期运算处理跨月，断日使分组键改变。本题输出每段连续区间；若只要用户名单，还需对结果用户去重。

```sql
WITH logins(uid, day) AS (
  VALUES ('A','2026-11-01'), ('A','2026-11-01'),
         ('A','2026-11-02'), ('A','2026-11-03'),
         ('B','2026-11-01'), ('B','2026-11-03')
), days AS (
  SELECT DISTINCT uid, day FROM logins
), numbered AS (
  SELECT uid, day, ROW_NUMBER() OVER (
    PARTITION BY uid ORDER BY day
  ) AS rn FROM days
), streaks AS (
  SELECT uid, MIN(day) AS first_day, MAX(day) AS last_day, COUNT(*) AS n
  FROM numbered GROUP BY uid, julianday(day) - rn
)
SELECT uid, first_day, last_day, n FROM streaks
WHERE n >= 3 ORDER BY uid, first_day;
```

预期结果按列顺序为 JSON 数组，`null` 表示 SQL NULL。

```json
[["A", "2026-11-01", "2026-11-03", 3]]
```

#### 架构师/技术管理视角

确认业务时区和数据完整性，迟到登录会改变连续区间，重算要覆盖受影响日期。

### 题目3　次日留存率怎样定义并计算？

#### 简洁回答

**求职者**　分母是当天首次出现的用户，分子是这批人在次日再次出现的人数。先把每天的用户去重。

#### 深入回答

**面试官**　最新一天没有次日数据，该算 0 吗？

**求职者**　应排除尚未完成次日观察的 cohort。示例数据已完整到 11 月 3 日，故只算 11 月 1、2 日 cohort。首次出现需基于完整历史或可信注册表，短时间截取会把老用户当新用户。

```sql
WITH logins(uid, day) AS (
  VALUES ('A','2026-11-01'), ('A','2026-11-02'), ('A','2026-11-02'),
         ('B','2026-11-01'), ('C','2026-11-02'), ('D','2026-11-03')
), days AS (
  SELECT DISTINCT uid, day FROM logins
), cohorts AS (
  SELECT uid, MIN(day) AS first_day FROM days GROUP BY uid
)
SELECT c.first_day, COUNT(*) AS new_users,
       COUNT(d.uid) AS returned_users,
       ROUND(1.0 * COUNT(d.uid) / COUNT(*), 4) AS retention
FROM cohorts c LEFT JOIN days d
  ON c.uid = d.uid AND d.day = date(c.first_day, '+1 day')
WHERE c.first_day < '2026-11-03'
GROUP BY c.first_day ORDER BY c.first_day;
```

预期结果按列顺序为 JSON 数组，`null` 表示 SQL NULL。

```json
[["2026-11-01", 2, 1, 0.5], ["2026-11-02", 1, 0, 0.0]]
```

#### 架构师/技术管理视角

记录注册口径、活跃口径及数据截止点，零用户的日期可通过日历表补齐并明确返回 NULL 或其他约定。

### 题目4　每店销售额前两名，如何处理并列？

#### 简洁回答

**求职者**　先汇总商品销售额，再按门店做 DENSE_RANK，取前两个金额名次。

#### 深入回答

**面试官**　如果产品要求严格两件呢？

**求职者**　使用 ROW_NUMBER，并增加稳定且业务认可的并列键。DENSE_RANK 前两名可能多于两行，RANK 会因并列跳号，必须先确认“前两名”的含义。

```sql
WITH sales(shop, product, amount) AS (
  VALUES ('S','A',100), ('S','B',80), ('S','C',80), ('S','D',60)
), totals AS (
  SELECT shop, product, SUM(amount) AS revenue
  FROM sales GROUP BY shop, product
), ranked AS (
  SELECT *, DENSE_RANK() OVER (
    PARTITION BY shop ORDER BY revenue DESC
  ) AS ranking FROM totals
)
SELECT shop, product, revenue, ranking FROM ranked
WHERE ranking <= 2 ORDER BY shop, ranking, product;
```

预期结果按列顺序为 JSON 数组，`null` 表示 SQL NULL。

```json
[["S", "A", 100, 1], ["S", "B", 80, 2], ["S", "C", 80, 2]]
```

#### 架构师/技术管理视角

把并列示例写进验收数据，排序规则和导出分页保持一致。

### 题目5　一单多支付、多退款，如何计算净收款？

#### 简洁回答

**求职者**　支付和退款分别汇总到订单粒度，再连接订单。没有对应记录时按约定转为零。

#### 深入回答

**面试官**　直接连接三个表有什么问题？

**求职者**　同一订单两笔支付乘两笔退款会形成四行，使汇总放大。本例输入假设已经按流水号去重且仅含成功交易；真实链路先处理状态、撤销和截止时间。

```sql
WITH orders(id) AS (VALUES ('A'), ('B'), ('C')),
payments(id, amount) AS (VALUES ('A',6000), ('A',4000), ('B',5000)),
refunds(id, amount) AS (VALUES ('A',1000), ('A',2000)),
p AS (SELECT id, SUM(amount) AS amount FROM payments GROUP BY id),
r AS (SELECT id, SUM(amount) AS amount FROM refunds GROUP BY id)
SELECT o.id, COALESCE(p.amount,0) - COALESCE(r.amount,0) AS net
FROM orders o LEFT JOIN p ON o.id = p.id LEFT JOIN r ON o.id = r.id
ORDER BY o.id;
```

预期结果按列顺序为 JSON 数组，`null` 表示 SQL NULL。

```json
[["A", 7000], ["B", 5000], ["C", 0]]
```

#### 架构师/技术管理视角

使用整数分或合适的 DECIMAL，分别对账支付、退款及净额，排查没有主订单的孤立流水。

### 题目6　用户搬家后，历史订单应该属于哪个城市？

#### 简洁回答

**求职者**　先确认报表采用当前城市还是下单时城市。若按历史归属，订单时间关联维度有效区间。

#### 深入回答

**面试官**　刚好等于城市变更时间，会匹配几行？

**求职者**　使用左闭右开区间，边界落在新版本。示例保留没有维度的订单，城市为 NULL。需检测维度区间重叠，否则一笔订单会匹配多行；还要处理有效区间缺口。

```sql
WITH orders(id, uid, occurred_at) AS (
  VALUES ('O1','A','2026-11-01'), ('O2','A','2026-11-02'),
         ('O3','B','2026-11-02')
), history(uid, city, valid_from, valid_to) AS (
  VALUES ('A','合肥','2026-10-01','2026-11-02'),
         ('A','上海','2026-11-02',NULL)
)
SELECT o.id, h.city FROM orders o LEFT JOIN history h
  ON o.uid = h.uid AND o.occurred_at >= h.valid_from
  AND (h.valid_to IS NULL OR o.occurred_at < h.valid_to)
ORDER BY o.id;
```

预期结果按列顺序为 JSON 数组，`null` 表示 SQL NULL。

```json
[["O1", "合肥"], ["O2", "上海"], ["O3", null]]
```

#### 架构师/技术管理视角

区分业务生效时间和系统接收时间，迟到修订需标记受影响的报表与重算范围。

技术机制核对入口。[SQLite 窗口函数](https://www.sqlite.org/windowfunctions.html)

## 10. 知识自检与口述推演清单

- [ ] 一分钟说明输入、业务约束与结果。
- [ ] 面试官增加重复、乱序或崩溃后，能够重新推演。
- [ ] 指出需要的证据、恢复步骤与仍然存在的边界。

原理、证据、边界、实施各评 0～2 分，记录回答原句和需要复测的条件。实际作答后再更新学习状态。

## 11. 知识脉络与技术图谱关联

结合 [Flink](01-flink.md)、[Paimon](02-paimon.md)、[Doris](03-doris.md)、[Spring](06-spring.md)理解跨层边界。再做[双角色项目答辩](../07-project-review/03-interview-defense-playbook.md)。

## 12. 权威参考与官方进阶资料

[SQLite 日期函数](https://www.sqlite.org/lang_datefunc.html) · [SQL 结果核验脚本](../scripts/verify_sql_exercises.py)

[SQLite 窗口函数](https://www.sqlite.org/windowfunctions.html)

按照标注版本核对参数和语义。数据库并发与分布式故障部分为协议推演，需在目标引擎及真实连接器上另做验证。
