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

/** 从订单宽表的当前快照启动，再订阅后续变更。可回放的范围取决于快照与变更日志的保留策略。 */
public class PaimonStreamingChangelogReadJob {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonStreamingChangelogReadJob.class);

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        String warehouseDir = args.length == 0
                ? new File(System.getProperty("java.io.tmpdir"), "paimon_warehouse").toURI().toString()
                : args[0];

        // 1. 注册 Catalog
        tEnv.executeSql(String.format(
                "CREATE CATALOG paimon_catalog WITH ('type' = 'paimon', 'warehouse' = '%s')", warehouseDir.replace("'", "''")));
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
