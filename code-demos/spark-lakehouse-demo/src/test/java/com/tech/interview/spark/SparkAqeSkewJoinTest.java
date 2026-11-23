package com.tech.interview.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Spark AQE 与倾斜 Join 单元测试
 */
class SparkAqeSkewJoinTest {

    private static SparkSession spark;

    @BeforeAll
    static void setUp() {
        spark = SparkAqeSkewJoinApp.createSparkSession(true);
    }

    @AfterAll
    static void tearDown() {
        if (spark != null) {
            spark.stop();
        }
    }

    @Test
    @DisplayName("验证 AQE 相关核心运行时配置是否成功注入")
    void testAqeConfigs() {
        assertNotNull(spark);
        assertEquals("true", spark.conf().get("spark.sql.adaptive.enabled"));
        assertEquals("true", spark.conf().get("spark.sql.adaptive.coalescePartitions.enabled"));
        assertEquals("true", spark.conf().get("spark.sql.adaptive.skewJoin.enabled"));
    }

    @Test
    @DisplayName("验证手动加盐关联逻辑在小规模倾斜数据下的正确性")
    void testManualSaltingJoin() {
        List<Row> clickRows = new ArrayList<>();
        clickRows.add(RowFactory.create("c1", "hot_item", 1000L));
        clickRows.add(RowFactory.create("c2", "hot_item", 1001L));
        clickRows.add(RowFactory.create("c3", "normal_item", 1002L));

        StructType clickSchema = new StructType(new StructField[]{
                new StructField("clickId", DataTypes.StringType, false, Metadata.empty()),
                new StructField("itemId", DataTypes.StringType, false, Metadata.empty()),
                new StructField("eventTime", DataTypes.LongType, false, Metadata.empty())
        });

        Dataset<Row> clickDf = spark.createDataFrame(clickRows, clickSchema);

        List<Row> itemRows = new ArrayList<>();
        itemRows.add(RowFactory.create("hot_item", "爆款商品", 99.9));
        itemRows.add(RowFactory.create("normal_item", "普通商品", 9.9));

        StructType itemSchema = new StructType(new StructField[]{
                new StructField("itemId", DataTypes.StringType, false, Metadata.empty()),
                new StructField("itemName", DataTypes.StringType, false, Metadata.empty()),
                new StructField("price", DataTypes.DoubleType, false, Metadata.empty())
        });

        Dataset<Row> itemDf = spark.createDataFrame(itemRows, itemSchema);

        Dataset<Row> resultDf = SparkAqeSkewJoinApp.executeManualSaltingJoin(spark, clickDf, itemDf, 3);
        assertEquals(3, resultDf.count());
    }
}
