package com.tech.interview.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Spark 3.5+ 自适应查询执行 (AQE) 与数据倾斜 (Data Skew) 治理实战工程
 *
 * 核心演示内容：
 * 1. AQE 动态合并小分区 (Coalesce Shuffle Partitions)；
 * 2. AQE 自动识别倾斜分区并进行运行时切片拆解 (Adaptive Skew Join)；
 * 3. 架构级方案对比：AQE 引擎自动拆解 vs 手动加盐两阶段关联 (Salting Two-Stage Join)；
 * 4. 广播散列关联 (Broadcast Hash Join) 阈值控制与内存安全防 OOM 边界。
 */
public class SparkAqeSkewJoinApp implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(SparkAqeSkewJoinApp.class);

    public static SparkSession createSparkSession(boolean isLocal) {
        SparkSession.Builder builder = SparkSession.builder()
                .appName("Spark-Lakehouse-AQE-SkewJoin-Demo");

        if (isLocal) {
            builder.master("local[2]");
        }

        // ====================================================================
        // Spark 3.5+ 生产级自适应查询执行 (AQE) 参数基线
        // ====================================================================
        builder
                // 1. 全局开启 AQE
                .config("spark.sql.adaptive.enabled", "true")

                // 2. AQE 动态合并 Shuffle 分区 (取代固定 spark.sql.shuffle.partitions=200 的历史顽疾)
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.initialPartitionNum", "200")
                .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "67108864") // 建议单分区 64MB

                // 3. AQE 动态倾斜 Join 自动治理 (核心亮点)
                .config("spark.sql.adaptive.skewJoin.enabled", "true")
                .config("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5") // 中位数 5 倍即判定为倾斜
                .config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "67108864") // 64MB 阈值

                // 4. 动态转换 Join 策略 (SortMergeJoin 运行时自动降级为 BroadcastHashJoin)
                .config("spark.sql.autoBroadcastJoinThreshold", "10485760") // 10MB 广播上限

                // 5. 统一内存模型配额 (堆内 Execution 与 Storage 动态借用)
                .config("spark.memory.fraction", "0.6")
                .config("spark.memory.storageFraction", "0.5");

        return builder.getOrCreate();
    }

    /**
     * 方案 A：依赖 Spark 3.5+ AQE 引擎自动治理倾斜关联
     */
    public static Dataset<Row> executeAqeSkewJoin(Dataset<Row> clickStreamDf, Dataset<Row> itemMetaDf) {
        LOG.info("执行方案 A: 依赖 Spark AQE 自动识别并拆解倾斜 Task...");
        // 正常业务 Join，无代码侵入，AQE 将在 Shuffle Read 阶段根据统计信息自动将倾斜的大分区拆解
        return clickStreamDf.join(itemMetaDf, "itemId");
    }

    /**
     * 方案 B：代码级手动加盐两阶段打散 (用于非 AQE 场景、极度严重的跨维度复合倾斜或大促特殊兜底)
     */
    public static Dataset<Row> executeManualSaltingJoin(
            SparkSession spark, 
            Dataset<Row> clickStreamDf, 
            Dataset<Row> itemMetaDf, 
            int saltFactor) {

        LOG.info("执行方案 B: 代码级手动加盐两阶段关联 (SaltFactor={})...", saltFactor);

        // 1. 倾斜流数据加盐：在 itemId 上拼接 [0, saltFactor) 的随机数
        Dataset<Row> saltedClickDf = clickStreamDf.withColumn(
                "salted_key",
                functions.concat(functions.col("itemId"), functions.lit("_"), functions.floor(functions.rand().multiply(saltFactor)))
        );

        // 2. 维表扩容 (Explode)：将维表按盐值膨胀 N 份
        List<Row> saltList = new ArrayList<>();
        for (int i = 0; i < saltFactor; i++) {
            saltList.add(RowFactory.create(i));
        }
        StructType saltSchema = new StructType(new StructField[]{
                new StructField("salt_val", DataTypes.IntegerType, false, Metadata.empty())
        });
        Dataset<Row> saltDf = spark.createDataFrame(saltList, saltSchema);

        Dataset<Row> expandedItemDf = itemMetaDf.crossJoin(saltDf)
                .withColumn("salted_key", functions.concat(functions.col("itemId"), functions.lit("_"), functions.col("salt_val")))
                .drop("salt_val");

        // 3. 基于打散后的 salted_key 均匀分布关联
        return saltedClickDf.join(expandedItemDf, "salted_key")
                .drop("salted_key");
    }

    /**
     * 方案 C：广播散列关联 (适用于维表在内存可容忍范围内的最高效关联方式)
     */
    public static Dataset<Row> executeBroadcastHashJoin(Dataset<Row> clickStreamDf, Dataset<Row> itemMetaDf) {
        LOG.info("执行方案 C: 强制触发 BroadcastHashJoin (消除全量 Shuffle 损耗)...");
        return clickStreamDf.join(functions.broadcast(itemMetaDf), "itemId");
    }

    public static void main(String[] args) {
        SparkSession spark = createSparkSession(true);

        try {
            // 构造模拟测试数据：90% 点击集中在热点商品 "item_hot_001"
            List<Row> clickData = new ArrayList<>();
            Random random = new Random();
            for (int i = 0; i < 1000; i++) {
                String itemId = (i % 10 == 0) ? "item_normal_" + (i % 50) : "item_hot_001";
                clickData.add(RowFactory.create("click_" + i, itemId, System.currentTimeMillis()));
            }

            StructType clickSchema = new StructType(new StructField[]{
                    new StructField("clickId", DataTypes.StringType, false, Metadata.empty()),
                    new StructField("itemId", DataTypes.StringType, false, Metadata.empty()),
                    new StructField("eventTime", DataTypes.LongType, false, Metadata.empty())
            });

            Dataset<Row> clickStreamDf = spark.createDataFrame(clickData, clickSchema);

            // 构造商品维表
            List<Row> itemData = new ArrayList<>();
            itemData.add(RowFactory.create("item_hot_001", "大促超级爆品iPhone 16", 7999.0));
            for (int i = 0; i < 50; i++) {
                itemData.add(RowFactory.create("item_normal_" + i, "普通商品_" + i, 99.0));
            }

            StructType itemSchema = new StructType(new StructField[]{
                    new StructField("itemId", DataTypes.StringType, false, Metadata.empty()),
                    new StructField("itemName", DataTypes.StringType, false, Metadata.empty()),
                    new StructField("price", DataTypes.DoubleType, false, Metadata.empty())
            });

            Dataset<Row> itemMetaDf = spark.createDataFrame(itemData, itemSchema);

            // 执行测试
            Dataset<Row> resultA = executeAqeSkewJoin(clickStreamDf, itemMetaDf);
            resultA.show(5, false);

            Dataset<Row> resultB = executeManualSaltingJoin(spark, clickStreamDf, itemMetaDf, 4);
            resultB.show(5, false);

        } finally {
            spark.stop();
        }
    }
}
