package com.tech.interview.paimon;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * 【Paimon + Flink 核心实战一：Partial-Update 零状态多流拼宽表】
 * 
 * 核心架构考点：
 * 1. 业务痛点（Before）：
 *    - 在传统实时流处理中，要将订单基本流、支付流、物流流拼接成一张电商全生命周期宽表，
 *      必须在 Flink SQL 中写双流/三流 FULL OUTER JOIN；
 *    - Flink 算子底层必须在 RocksDB 中为每个流维持全量历史状态数天或数月，导致状态膨胀至数个 TB，
 *      极易引发 Checkpoint 超时失败、TaskManager OOM 崩溃与作业扩缩容困难；
 * 2. Paimon 破局方案（After）：
 *    - 将宽表定义为 Paimon 主键表，配置 'merge-engine' = 'partial-update' 与 'changelog-producer' = 'lookup'；
 *    - 订单流、支付流、物流流作为三个独立的数据流并行写入同一张 Paimon 宽表，各流只负责更新自己的列；
 *    - 数据在 Paimon 存储层内部的 LSM 合并时自动按主键将非空字段合并为完整大宽表；
 *    - Flink 计算算子完全无状态（Stateless），内存占用从几十 GB 锐减至数百 MB，彻底根治状态爆炸！
 */
public class PaimonPartialUpdateWideTableJob {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonPartialUpdateWideTableJob.class);

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        // 1. 设置本地或 HDFS/S3 仓库路径
        String warehouseDir = System.getProperty("java.io.tmpdir") + File.separator + "paimon_warehouse";
        LOG.info("Paimon Warehouse Directory: {}", warehouseDir);

        // 2. 注册并使用 Paimon 文件系统 Catalog
        tEnv.executeSql(String.format(
                "CREATE CATALOG paimon_catalog WITH (" +
                "  'type' = 'paimon'," +
                "  'warehouse' = '%s'" +
                ")", warehouseDir));
        tEnv.executeSql("USE CATALOG paimon_catalog");

        // 3. 创建核心 Partial-Update 宽表
        tEnv.executeSql(
                "CREATE TABLE IF NOT EXISTS order_wide_table (" +
                "  order_id STRING," +
                "  user_id STRING," +
                "  amount DOUBLE," +
                "  order_status STRING," +
                "  pay_time STRING," +
                "  pay_channel STRING," +
                "  tracking_no STRING," +
                "  delivery_status STRING," +
                "  PRIMARY KEY (order_id) NOT ENFORCED" +
                ") WITH (" +
                "  'merge-engine' = 'partial-update'," +     // 核心：部分列更新合并引擎
                "  'changelog-producer' = 'lookup'," +       // 核心：点查生成变更日志，保证流读下游数据完整性
                "  'lookup.cache' = 'auto'" +
                ")"
        );

        // 4. 创建模拟输入源 A：订单基础流 (只写 order_id, user_id, amount, order_status)
        tEnv.executeSql(
                "CREATE TEMPORARY TABLE src_order_base (" +
                "  order_id STRING," +
                "  user_id STRING," +
                "  amount DOUBLE," +
                "  order_status STRING" +
                ") WITH (" +
                "  'connector' = 'datagen'," +
                "  'rows-per-second' = '10'," +
                "  'fields.order_id.kind' = 'sequence'," +
                "  'fields.order_id.start' = '1001'," +
                "  'fields.order_id.end' = '1050'" +
                ")"
        );

        // 5. 创建模拟输入源 B：支付流水流 (只写 order_id, pay_time, pay_channel)
        tEnv.executeSql(
                "CREATE TEMPORARY TABLE src_payment (" +
                "  order_id STRING," +
                "  pay_time STRING," +
                "  pay_channel STRING" +
                ") WITH (" +
                "  'connector' = 'datagen'," +
                "  'rows-per-second' = '10'," +
                "  'fields.order_id.kind' = 'sequence'," +
                "  'fields.order_id.start' = '1001'," +
                "  'fields.order_id.end' = '1050'" +
                ")"
        );

        // 6. 创建模拟输入源 C：物流配送流 (只写 order_id, tracking_no, delivery_status)
        tEnv.executeSql(
                "CREATE TEMPORARY TABLE src_logistics (" +
                "  order_id STRING," +
                "  tracking_no STRING," +
                "  delivery_status STRING" +
                ") WITH (" +
                "  'connector' = 'datagen'," +
                "  'rows-per-second' = '10'," +
                "  'fields.order_id.kind' = 'sequence'," +
                "  'fields.order_id.start' = '1001'," +
                "  'fields.order_id.end' = '1050'" +
                ")"
        );

        // 7. 使用 StatementSet 将三个独立数据流并发写入同一张 Paimon 宽表 (各写入负责列，其余留 NULL)
        StatementSet statementSet = tEnv.createStatementSet();

        // 写入分支 1：订单基础信息
        statementSet.addInsertSql(
                "INSERT INTO order_wide_table (" +
                "  order_id, user_id, amount, order_status, " +
                "  pay_time, pay_channel, tracking_no, delivery_status" +
                ") SELECT " +
                "  order_id, user_id, amount, order_status, " +
                "  CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING) " +
                "FROM src_order_base"
        );

        // 写入分支 2：支付信息 (异步拼接)
        statementSet.addInsertSql(
                "INSERT INTO order_wide_table (" +
                "  order_id, user_id, amount, order_status, " +
                "  pay_time, pay_channel, tracking_no, delivery_status" +
                ") SELECT " +
                "  order_id, CAST(NULL AS STRING), CAST(NULL AS DOUBLE), CAST(NULL AS STRING), " +
                "  pay_time, pay_channel, CAST(NULL AS STRING), CAST(NULL AS STRING) " +
                "FROM src_payment"
        );

        // 写入分支 3：物流信息 (异步拼接)
        statementSet.addInsertSql(
                "INSERT INTO order_wide_table (" +
                "  order_id, user_id, amount, order_status, " +
                "  pay_time, pay_channel, tracking_no, delivery_status" +
                ") SELECT " +
                "  order_id, CAST(NULL AS STRING), CAST(NULL AS DOUBLE), CAST(NULL AS STRING), " +
                "  CAST(NULL AS STRING), CAST(NULL AS STRING), tracking_no, delivery_status " +
                "FROM src_logistics"
        );

        LOG.info("Submitting Paimon Partial-Update Wide Table StatementSet...");
        statementSet.execute();
    }
}
