package com.tech.interview.paimon;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * 【Paimon + Flink 核心实战三：流式 Changelog 增量消费与去 Kafka 化】
 * 
 * 核心架构考点：
 * 1. 架构革新：将 Paimon 作为流式消息队列替代 Kafka：
 *    - 既有 Kafka 的流式低延迟增量订阅能力，又有对象存储的廉价持久化能力与批处理 SQL 分析能力；
 * 2. 多样化流读模式 (Scan Mode)：
 *    - latest-full (默认): 首次启动先流读最新全量数据（包含当前快照），随后持续无缝接收增量 Changelog；
 *    - latest: 仅消费作业启动之后的最新增量变更；
 *    - from-snapshot / from-timestamp: 支持流式回溯到几天前、几个月前的历史快照点重新消费；
 * 3. 完整的增量流语义：
 *    - 流式读取还原出完整的 RowKind (+I, -U, +U, -D)，下游可以直接驱动实时聚合、同步 ElasticSearch/Doris 或触发微服务事件。
 */
public class PaimonStreamingChangelogReadJob {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonStreamingChangelogReadJob.class);

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

        // 2. 流式订阅 Paimon 宽表 (使用 SQL Hint 动态指定流读参数)
        // scan.mode = 'latest-full' 表示启动时先读全量快照，再持续监听后续流式变更
        Table streamingTable = tEnv.sqlQuery(
                "SELECT * FROM order_wide_table /*+ OPTIONS(" +
                "  'scan.mode' = 'latest-full'" +
                ") */"
        );

        // 3. 将 Paimon 流式表转化为 DataStream<Row>，观察 RowKind 变动
        DataStream<Row> changelogStream = tEnv.toChangelogStream(streamingTable);

        changelogStream.map(row -> {
            // row.getKind() 真实呈现 +I (Insert), -U (Update_Before), +U (Update_After), -D (Delete)
            return String.format("📡 [Streaming Read Event] Kind=%s | OrderId=%s | UserId=%s | Amount=%s | PayChannel=%s | Status=%s",
                    row.getKind(), row.getField(0), row.getField(1), row.getField(2), row.getField(5), row.getField(7));
        }).print();

        LOG.info("Submitting Paimon Streaming Read Job...");
        env.execute("Paimon Streaming Changelog Read Job");
    }
}
