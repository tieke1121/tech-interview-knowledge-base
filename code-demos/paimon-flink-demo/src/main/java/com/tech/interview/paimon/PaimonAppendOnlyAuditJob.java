package com.tech.interview.paimon;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * 【Paimon + Flink 核心实战四：Append-Only 纯追加流式归档与小文件治理】
 * 
 * 核心架构考点：
 * 1. Append-Only 表设计适用场景：
 *    - 用户点击行为流、系统安全审计日志、IoT 传感器高频流水等“只增不改”的超大吞吐日志场景；
 *    - 无主键排序与合并开销，写入性能达到物理存储峰值（比 Primary Key 表快数倍）；
 * 2. 小文件治理与合并参数调优：
 *    - 实时流每 10 秒写入一次 Checkpoint 会产生大量碎片 Parquet 小文件；
 *    - 配置 'compaction.min.file-num' = '5' 与 'compaction.max.file-num' = '10'：
 *      后台自动探测并合并碎片文件为 128MB 以上标准 Parquet 文件，避免查询端元数据打爆与 HDFS NameNode 崩溃；
 * 3. 快照生命周期治理：
 *    - 配置 'snapshot.time-retained' = '2 h' 与 'snapshot.num-retained.min' = '10'：
 *      自动过期并物理删除历史无效快照与废弃数据文件，防止存储空间无限泄漏。
 */
public class PaimonAppendOnlyAuditJob {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonAppendOnlyAuditJob.class);

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        String warehouseDir = System.getProperty("java.io.tmpdir") + File.separator + "paimon_warehouse";

        // 1. 注册 Catalog
        tEnv.executeSql(String.format(
                "CREATE CATALOG paimon_catalog WITH ('type' = 'paimon', 'warehouse' = '%s')", warehouseDir));
        tEnv.executeSql("USE CATALOG paimon_catalog");

        // 2. 创建 Append-Only 审计日志表 (配置小文件自动合并与快照自动淘汰)
        tEnv.executeSql(
                "CREATE TABLE IF NOT EXISTS security_audit_log_table (" +
                "  trace_id STRING," +
                "  client_ip STRING," +
                "  api_path STRING," +
                "  http_status INT," +
                "  cost_ms BIGINT," +
                "  event_time TIMESTAMP(3)" +
                ") WITH (" +
                "  'write-mode' = 'append-only'," +            // 核心：纯追加写入模式
                "  'file.format' = 'parquet'," +               // 底层存储格式采用高压缩比 Parquet
                "  'compaction.min.file-num' = '5'," +         // 最少累积 5 个小文件触发合并
                "  'compaction.max.file-num' = '10'," +        // 最多一次合并 10 个小文件
                "  'snapshot.time-retained' = '2 h'," +        // 快照保留 2 小时，自动物理清理过期文件
                "  'snapshot.num-retained.min' = '10'" +       // 最少保留 10 个有效版本快照
                ")"
        );

        // 3. 模拟高吞吐审计流水
        tEnv.executeSql(
                "CREATE TEMPORARY TABLE src_audit_generator (" +
                "  trace_id STRING," +
                "  client_ip STRING," +
                "  api_path STRING," +
                "  http_status INT," +
                "  cost_ms BIGINT," +
                "  event_time TIMESTAMP(3)" +
                ") WITH (" +
                "  'connector' = 'datagen'," +
                "  'rows-per-second' = '50'" +
                ")"
        );

        // 4. 流式写入
        LOG.info("Streaming high-throughput logs into Paimon Append-Only table...");
        tEnv.executeSql(
                "INSERT INTO security_audit_log_table " +
                "SELECT trace_id, client_ip, api_path, http_status, cost_ms, event_time " +
                "FROM src_audit_generator"
        );
    }
}
