package com.tech.interview.flink.source;

import com.tech.interview.flink.model.OrderEvent;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serial;
import java.util.Random;

/**
 * 【Source/算子间数据流转核心考点：7 种物理分区与重分布策略深度对比】
 * 
 * 核心架构考点：
 * 1. keyBy(): 基于 KeyGroup 哈希映射，相同 key 必然路由到相同 Subtask，进入 KeyedState；
 * 2. rebalance(): 跨 TaskManager 全局轮询 (Round-Robin)，打散数据倾斜但产生高额跨机器网络 Shuffle；
 * 3. rescale(): 仅在本地 TaskManager 内部的上下游 Slot 间轮询，避免跨机器跨网络通信，网络效率远超 rebalance；
 * 4. forward(): 严格一对一本地直连传递，要求上下游并发度完全一致，零网络与零反序列化开销；
 * 5. broadcast(): 将每条记录广播给下游所有并发实例 (用于动态规则流下发)；
 * 6. global(): 将所有数据全量汇聚到下游第 0 号并发实例 (慎用，极易造成单点瓶颈)；
 * 7. partitionCustom(): 自定义 Partitioner 算法，按租户或业务分级实现定制路由隔离。
 */
public class StreamPartitioningDemoJob {

    private static final Logger LOG = LoggerFactory.getLogger(StreamPartitioningDemoJob.class);

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(4);

        // 基础数据流 (并发度 2)
        DataGeneratorSource<OrderEvent> orderSource = new DataGeneratorSource<>(
                new GeneratorFunction<>() {
                    @Serial
                    private static final long serialVersionUID = 1L;
                    private final Random random = new Random();

                    @Override
                    public OrderEvent map(Long value) {
                        String orderId = "ORD-" + value;
                        String tenant = (value % 3 == 0) ? "TENANT_VIP" : "TENANT_NORMAL_" + (value % 5);
                        return new OrderEvent(orderId, tenant, 100.0, System.currentTimeMillis());
                    }
                },
                1000L,
                RateLimiterStrategy.perSecond(50),
                TypeInformation.of(OrderEvent.class)
        );

        DataStream<OrderEvent> baseStream = env
                .fromSource(orderSource, org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(), "Partition-Source")
                .setParallelism(2);

        // 1. Rebalance (轮询打散倾斜)
        DataStream<String> rebalancedStream = baseStream
                .rebalance()
                .map(new SubtaskTrackerMapFunction("Rebalance"))
                .setParallelism(4);

        // 2. Rescale (轻量本地轮询，无跨机 Shuffle)
        DataStream<String> rescaledStream = baseStream
                .rescale()
                .map(new SubtaskTrackerMapFunction("Rescale"))
                .setParallelism(4);

        // 3. PartitionCustom (基于租户自定义分区器，VIP 租户硬隔离到专用分区)
        DataStream<String> customPartitionedStream = baseStream
                .partitionCustom(new TenantPartitioner(), OrderEvent::getUserId)
                .map(new SubtaskTrackerMapFunction("PartitionCustom"))
                .setParallelism(4);

        rebalancedStream.print();
        rescaledStream.print();
        customPartitionedStream.print();

        env.execute("Stream Partitioning Comparison Job");
    }

    /**
     * 自定义租户隔离分区器
     */
    public static class TenantPartitioner implements Partitioner<String> {
        @Serial
        private static final long serialVersionUID = 1L;

        @Override
        public int partition(String key, int numPartitions) {
            // VIP 租户硬隔离到 0 号分区，其余租户在剩余分区均匀轮询
            if ("TENANT_VIP".equals(key)) {
                return 0;
            }
            if (numPartitions <= 1) {
                return 0;
            }
            return 1 + (Math.abs(key.hashCode()) % (numPartitions - 1));
        }
    }

    /**
     * 辅助监控 Map 算子：打印数据被分发到的当前 Subtask Index
     */
    public static class SubtaskTrackerMapFunction extends RichMapFunction<OrderEvent, String> {
        @Serial
        private static final long serialVersionUID = 1L;

        private final String strategyName;
        private int subtaskIndex;

        public SubtaskTrackerMapFunction(String strategyName) {
            this.strategyName = strategyName;
        }

        @Override
        public void open(Configuration parameters) {
            this.subtaskIndex = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
        }

        @Override
        public String map(OrderEvent value) {
            return String.format("[%s] Key=%s OrderId=%s -> HandledBySubtask=%d",
                    strategyName, value.getUserId(), value.getOrderId(), subtaskIndex);
        }
    }
}
