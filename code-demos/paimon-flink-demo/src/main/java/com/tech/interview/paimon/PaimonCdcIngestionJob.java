package com.tech.interview.paimon;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;

/** 把内存中的五条 RowKind 变更写入一张主键表，input 模式复用上游变更日志。 */
public class PaimonCdcIngestionJob {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonCdcIngestionJob.class);

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        env.enableCheckpointing(5000);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        String warehouseDir = args.length == 0
                ? new File(System.getProperty("java.io.tmpdir"), "paimon_warehouse").toURI().toString()
                : args[0];

        // 1. 注册 Catalog
        tEnv.executeSql(String.format(
                "CREATE CATALOG paimon_catalog WITH ('type' = 'paimon', 'warehouse' = '%s')", warehouseDir.replace("'", "''")));
        tEnv.executeSql("USE CATALOG paimon_catalog");

        // 2. 创建用户资产 CDC 镜像表 (采用 input 模式透传 CDC 变更)
        tEnv.executeSql(
                "CREATE TABLE IF NOT EXISTS user_account_cdc_table (" +
                "  user_id STRING," +
                "  user_name STRING," +
                "  balance DOUBLE," +
                "  updated_time STRING," +
                "  PRIMARY KEY (user_id) NOT ENFORCED" +
                ") WITH (" +
                "  'merge-engine' = 'deduplicate'," +       // 最新值去重覆盖
                "  'changelog-producer' = 'input'" +        // 核心：复用上游 CDC 自带变更日志，保留上游的前后镜像
                ")"
        );

        // 3. 按变更发生顺序模拟单个上游分区的 RowKind 变更流 (包含 +I, -U, +U, -D)
        DataStream<Row> cdcDataStream = env.fromData(
                Arrays.asList(
                        // 1. 初始插入：用户 1001 开户 1000 元
                        Row.ofKind(RowKind.INSERT, "1001", "Alice", 1000.0, "2026-11-23 10:00:00"),
                        // 2. 初始插入：用户 1002 开户 2000 元
                        Row.ofKind(RowKind.INSERT, "1002", "Bob", 2000.0, "2026-11-23 10:00:01"),
                        // 3. 更新操作：用户 1001 扣款 200 元 (包含更新前 -U 与更新后 +U)
                        Row.ofKind(RowKind.UPDATE_BEFORE, "1001", "Alice", 1000.0, "2026-11-23 10:00:00"),
                        Row.ofKind(RowKind.UPDATE_AFTER, "1001", "Alice", 800.0, "2026-11-23 10:05:00"),
                        // 4. 删除操作：用户 1002 销户注销 (-D)
                        Row.ofKind(RowKind.DELETE, "1002", "Bob", 2000.0, "2026-11-23 10:10:00")
                ),
                Types.ROW_NAMED(new String[]{"user_id", "user_name", "balance", "updated_time"},
                        Types.STRING, Types.STRING, Types.DOUBLE, Types.STRING)
        ).setParallelism(1);

        // 4. 将带有 RowKind 的 DataStream 转化为 Flink Changelog Table
        Schema schema = Schema.newBuilder()
                .column("user_id", DataTypes.STRING().notNull())
                .column("user_name", DataTypes.STRING())
                .column("balance", DataTypes.DOUBLE())
                .column("updated_time", DataTypes.STRING())
                .primaryKey("user_id")
                .build();

        Table cdcTable = tEnv.fromChangelogStream(cdcDataStream, schema);

        // 5. 写入 Paimon 湖表
        LOG.info("Writing CDC changelog stream into Paimon table...");
        cdcTable.executeInsert("user_account_cdc_table").await();
    }
}
