package com.tech.interview.paimon;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/** 把持续生成的审计记录写入无主键的追加表，定期提交快照。 */
public class PaimonAppendOnlyAuditJob {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonAppendOnlyAuditJob.class);

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
                "  'file.format' = 'parquet'," +               // 采用 Parquet 列式文件
                "  'compaction.min.file-num' = '5'," +         // 达到目标大小时，至少 5 个文件才触发合并
                "  'compaction.max.file-num' = '10'," +        // 累积 10 个时即使未达目标大小也可触发合并
                "  'snapshot.time-retained' = '2 h'," +        // 按时间淘汰快照，同时受保留数量等条件约束
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
        ).await();
    }
}
