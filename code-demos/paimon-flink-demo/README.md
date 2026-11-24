# 用 Flink 写入、读取 Paimon 表

本模块固定使用 Java 17、Flink 1.20.1 和 Paimon 0.9.0。四个入口共用一个文件系统 Catalog。输入来自本地生成的数据，适合在面试前观察一条变更何时对读者可见。

| 入口 | 输入和表 | 可观察的结果 |
| --- | --- | --- |
| [PaimonPartialUpdateWideTableJob](src/main/java/com/tech/interview/paimon/PaimonPartialUpdateWideTableJob.java) | 三组生成数据，各自填充订单、支付或物流字段，按订单 ID 写入同一张主键表 | 同一订单得到多个来源的字段。省去了流计算侧的长期 Join 状态，存储侧仍要缓冲写入、查旧值和合并文件。 |
| [PaimonCdcIngestionJob](src/main/java/com/tech/interview/paimon/PaimonCdcIngestionJob.java) | 单表五条模拟 CDC 记录，包括插入、更新前后镜像和删除 | 最新快照只剩用户 1001，余额为 800。input 模式依赖输入已经提供所需变更。 |
| [PaimonStreamingChangelogReadJob](src/main/java/com/tech/interview/paimon/PaimonStreamingChangelogReadJob.java) | 订阅上述订单宽表，从 latest-full 启动 | 读出当前表内容后持续等候后续提交。已有数据回放的起点取决于快照保留范围。 |
| [PaimonAppendOnlyAuditJob](src/main/java/com/tech/interview/paimon/PaimonAppendOnlyAuditJob.java) | 持续向无主键的 Parquet 表写入模拟审计事件 | 观察周期性快照和文件数量。合并频率与文件大小取决于流量、桶和合并策略。 |

## 本地演练

从模块目录执行。以下命令使用一个新的临时仓库，重复运行宽表作业相当于向同一批主键再写一次字段更新。

```bash
mvn test dependency:build-classpath -Dmdep.outputFile=target/runtime-classpath
demo_cp="target/classes:$(cat target/runtime-classpath)"
demo_warehouse="$(mktemp -d)"
demo_pkg=com.tech.interview.paimon

java -cp "$demo_cp" "$demo_pkg.PaimonCdcIngestionJob" "file://$demo_warehouse"
java -cp "$demo_cp" "$demo_pkg.PaimonPartialUpdateWideTableJob" "file://$demo_warehouse"
java -cp "$demo_cp" "$demo_pkg.PaimonStreamingChangelogReadJob" "file://$demo_warehouse"
```

在读作业中观察订单 ID、金额、支付渠道与配送状态，按 Ctrl+C 结束。将最后一行的类名换成 `PaimonAppendOnlyAuditJob` 可独立观察持续写入。省略仓库参数时，示例使用 Java 临时目录下的 `paimon_warehouse`。集群运行需要各节点都可访问的仓库。

## 面试时怎样解释结果

- **按主键更新。** 同一订单的三个来源分别拥有不同列，使用 partial-update 可以合并非空字段。若两个来源争写同一列，必须提前约定顺序或字段所有权。收到 NULL 通常意味着保留已有值，显式清空字段要单独设计。[Paimon 0.9 Partial Update](https://paimon.apache.org/docs/0.9/primary-key-table/merge-engine/partial-update/)
- **流读与可见性。** 写入作业设置 5 秒 Checkpoint 便于观察。数据文件写完后还须发布快照，读者才能发现新数据。端到端时延还包括快照提交和读端发现的时间。一次 Checkpoint 可能产生数据和合并两种快照。[Paimon 0.9 文件与提交](https://paimon.apache.org/docs/0.9/learn-paimon/understand-files/)
- **选择变更日志。** input 适合已有完整变更的上游。lookup 需要获取旧值，full-compaction 在全量合并时生成变更。none 适用于能直接处理 upsert 或自己补齐旧值的下游。选项会同时改变写入开销、读取状态和数据新鲜度。[Paimon 0.9 Changelog Producer](https://paimon.apache.org/docs/0.9/primary-key-table/changelog-producer/)

继续练习湖表选型和排障，可阅读 [Paimon 专项题](../../08-interview-questions/02-paimon.md)和[数据湖专项题](../../08-interview-questions/04-data-lake.md)。
