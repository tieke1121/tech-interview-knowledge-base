package com.tech.interview.paimon;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/** 把订单、支付、物流的字段按主键写入同一宽表。partial-update 在存储层合并非空字段。 */
public class PaimonPartialUpdateWideTableJob {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonPartialUpdateWideTableJob.class);

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        env.enableCheckpointing(5000);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        // 1. 设置本地或 HDFS/S3 仓库路径
        String warehouseDir = args.length == 0
                ? new File(System.getProperty("java.io.tmpdir"), "paimon_warehouse").toURI().toString()
                : args[0];
        LOG.info("Paimon Warehouse Directory: {}", warehouseDir);

        // 2. 注册并使用 Paimon 文件系统 Catalog
        tEnv.executeSql(String.format(
                "CREATE CATALOG paimon_catalog WITH (" +
                "  'type' = 'paimon'," +
                "  'warehouse' = '%s'" +
                ")", warehouseDir.replace("'", "''")));
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
                "  'changelog-producer' = 'lookup'" +
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
                "  'fields.amount.min' = '1'," +
                "  'fields.amount.max' = '999'," +
                "  'fields.user_id.length' = '8'," +
                "  'fields.order_status.length' = '4'," +
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

        // 三个分支映射为同一列序，只提交一个写入作业。
        LOG.info("Writing order, payment and logistics fields into the Paimon wide table");
        tEnv.executeSql("""
                INSERT INTO order_wide_table
                SELECT order_id, user_id, amount, order_status,
                       CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING)
                  FROM src_order_base
                UNION ALL
                SELECT order_id, CAST(NULL AS STRING), CAST(NULL AS DOUBLE), CAST(NULL AS STRING),
                       pay_time, pay_channel, CAST(NULL AS STRING), CAST(NULL AS STRING)
                  FROM src_payment
                UNION ALL
                SELECT order_id, CAST(NULL AS STRING), CAST(NULL AS DOUBLE), CAST(NULL AS STRING),
                       CAST(NULL AS STRING), CAST(NULL AS STRING), tracking_no, delivery_status
                  FROM src_logistics
                """).await();
    }
}
