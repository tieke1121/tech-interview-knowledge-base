package com.tech.interview.paimon;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Apache Paimon 与 Flink 整合核心功能单元测试")
public class PaimonCatalogAndTableTest {

    @Test
    @DisplayName("CDC 示例写入五条变更后只保留更新过的账户")
    void cdcJobPreservesUpdatesAndDeletes(@TempDir Path warehouse) throws Exception {
        String warehouseUri = warehouse.toUri().toString();
        PaimonCdcIngestionJob.main(new String[]{warehouseUri});
        var batch = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        batch.executeSql("CREATE CATALOG lake WITH ('type'='paimon', 'warehouse'='" + warehouseUri + "')");
        try (var rows = batch.executeSql("SELECT user_id, balance FROM lake.`default`.user_account_cdc_table").collect()) {
            Row remainingAccount = rows.next();
            assertEquals("1001", remainingAccount.getField(0));
            assertEquals(800.0, remainingAccount.getField(1));
            assertFalse(rows.hasNext());
        }
    }

    @Test
    @DisplayName("测试 Paimon Catalog 注册与 Partial-Update 主键表创建及写入")
    void testPartialUpdateTableCreationAndInsert(@TempDir File tempWarehouse) throws Exception {
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        // 1. 创建并使用 Paimon Catalog
        tEnv.executeSql(String.format(
                "CREATE CATALOG paimon_catalog WITH ('type' = 'paimon', 'warehouse' = '%s')",
                tempWarehouse.getAbsolutePath()));
        tEnv.executeSql("USE CATALOG paimon_catalog");

        // 2. 创建 Partial-Update 主键表
        tEnv.executeSql(
                "CREATE TABLE test_partial_table (" +
                "  id STRING," +
                "  name STRING," +
                "  age INT," +
                "  PRIMARY KEY (id) NOT ENFORCED" +
                ") WITH (" +
                "  'merge-engine' = 'partial-update'" +
                ")"
        );

        // 3. 模拟批式写入
        TableResult insertResult = tEnv.executeSql(
                "INSERT INTO test_partial_table VALUES ('101', 'Alice', 25), ('102', 'Bob', 30)"
        );
        insertResult.await();

        // 4. 读取验证
        TableResult queryResult = tEnv.executeSql("SELECT id, name, age FROM test_partial_table WHERE id = '101'");
        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> it = queryResult.collect()) {
            while (it.hasNext()) {
                rows.add(it.next());
            }
        }

        assertFalse(rows.isEmpty(), "应能成功读取 Paimon 表数据");
        assertEquals("101", rows.get(0).getField(0));
        assertEquals("Alice", rows.get(0).getField(1));
        assertEquals(25, rows.get(0).getField(2));
    }

    @Test
    @DisplayName("测试 Paimon Append-Only 纯追加表创建与写入")
    void testAppendOnlyTableCreationAndInsert(@TempDir File tempWarehouse) throws Exception {
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        tEnv.executeSql(String.format(
                "CREATE CATALOG paimon_catalog WITH ('type' = 'paimon', 'warehouse' = '%s')",
                tempWarehouse.getAbsolutePath()));
        tEnv.executeSql("USE CATALOG paimon_catalog");

        // 创建 Append-Only 纯追加表
        tEnv.executeSql(
                "CREATE TABLE test_append_table (" +
                "  log_id STRING," +
                "  message STRING" +
                ") WITH (" +
                "  'write-mode' = 'append-only'" +
                ")"
        );

        TableResult insertResult = tEnv.executeSql(
                "INSERT INTO test_append_table VALUES ('L1', 'System started'), ('L2', 'Task finished')"
        );
        insertResult.await();

        TableResult queryResult = tEnv.executeSql("SELECT count(*) FROM test_append_table");
        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> it = queryResult.collect()) {
            while (it.hasNext()) {
                rows.add(it.next());
            }
        }

        assertEquals(1, rows.size());
        assertEquals(2L, rows.get(0).getField(0));
    }
}
