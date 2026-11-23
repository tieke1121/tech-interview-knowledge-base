package com.tech.interview.paimon;

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

/**
 * 【Paimon + Flink 核心实战二：全库 CDC 毫秒级极速入湖与变更日志透传】
 * 
 * 核心架构考点：
 * 1. 传统架构痛点：
 *    - 传统链路：MySQL -> Flink CDC -> Kafka (数十个中间 Topic) -> Flink -> Iceberg/Hive；
 *    - 中间依赖多，Kafka 存储成本高昂，且 Iceberg/Hive 处理行级高频 UPDATE/DELETE 会产生小文件灾难与重写写放大；
 * 2. Paimon 破局方案：
 *    - Flink CDC 直接一跳入 Paimon，完全去掉中间层 Kafka；
 *    - 当上游是 CDC 数据源时，Paimon 配置 'changelog-producer' = 'input'：直接复用透传上游的变更日志 (-U, +U, -D)，
 *      不需要在写入时发起昂贵的 lookup 点查，写入吞吐最大化；
 *    - Paimon 底层 LSM-Tree 存储结构天然友好支持高频 UPDATE 与 DELETE，在后台 Compaction 时自动物理清除被删除数据。
 */
public class PaimonCdcIngestionJob {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonCdcIngestionJob.class);

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
                "  'changelog-producer' = 'input'" +        // 核心：复用上游 CDC 自带变更日志，零额外开销
                ")"
        );

        // 3. 模拟上游 Flink CDC 发出的原始 RowKind 变更流 (包含 +I, -U, +U, -D)
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
                )
        );

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
        cdcTable.executeInsert("user_account_cdc_table").print();
    }
}
