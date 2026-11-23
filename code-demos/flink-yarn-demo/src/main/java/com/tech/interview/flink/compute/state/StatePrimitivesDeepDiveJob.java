package com.tech.interview.flink.compute.state;

import com.tech.interview.flink.model.OrderEvent;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serial;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

/**
 * 【计算端核心考点：原生状态五大类型与 Operator UnionListState 深度实践】
 * 
 * 核心架构考点：
 * 1. 为什么大厂严禁滥用 ValueState<Map<K, V>>：
 *    - ValueState 底层必须将整个 Map 全量序列化为 byte[] 写入 RocksDB。即使只修改一个 key，也要全量读出反序列化再全量写回，CPU与I/O瞬间爆炸！
 *    - MapState<UK, UV> 底层将每个 userKey 直接映射为 RocksDB 的单个 Column Family Key，支持纳秒级点查 (get)、点写 (put)、点删 (remove)，性能相差百倍；
 * 2. 算子状态 (Operator State) 的恢复语义：
 *    - ListState: 恢复时将状态均分给各个并行度实例 (Round-Robin 分配)；
 *    - UnionListState: 恢复时将全量状态原样广播给每个并行度实例 (每个并发拿到所有快照，自行认领属于自己的分片，Kafka 消费位点恢复的核心机制)。
 */
public class StatePrimitivesDeepDiveJob {

    private static final Logger LOG = LoggerFactory.getLogger(StatePrimitivesDeepDiveJob.class);

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);

        // 模拟生成多品类订单流
        DataGeneratorSource<OrderEvent> orderSource = new DataGeneratorSource<>(
                new GeneratorFunction<>() {
                    @Serial
                    private static final long serialVersionUID = 1L;
                    private final Random random = new Random();

                    @Override
                    public OrderEvent map(Long value) {
                        String orderId = "ORD-" + value;
                        String userId = "USER-" + (100 + (value % 4));
                        // 借 amount 传类别标记：如 Category_1, Category_2
                        double amount = 10.0 + random.nextInt(5) * 50.0;
                        return new OrderEvent(orderId, userId, amount, System.currentTimeMillis());
                    }
                },
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(10),
                TypeInformation.of(OrderEvent.class)
        );

        DataStream<OrderEvent> orderStream = env
                .fromSource(orderSource, org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(), "State-Source");

        orderStream
                .keyBy(OrderEvent::getUserId)
                .flatMap(new ComprehensiveStateRichFlatMapFunction())
                .print();

        env.execute("State Primitives Deep Dive Job");
    }

    /**
     * 集合 Keyed State (MapState, ListState) 与 Operator State (UnionListState) 的综合实践算子
     */
    public static class ComprehensiveStateRichFlatMapFunction 
            extends RichFlatMapFunction<OrderEvent, String> 
            implements CheckpointedFunction {

        @Serial
        private static final long serialVersionUID = 1L;

        // 1. Keyed State: MapState (用户在不同品类下的累计消费金额)
        private transient MapState<String, Double> categorySpendingMapState;

        // 2. Keyed State: ListState (用户最近 5 笔交易订单 ID)
        private transient ListState<String> recentOrdersListState;

        // 3. Operator State: UnionListState (全局各 Subtask 处理的订单总计数器，用于故障广播式恢复)
        private transient ListState<Long> operatorUnionTotalCountState;
        private long localSubtaskCount = 0L;

        @Override
        public void open(OpenContext openContext) {
            // 初始化 Keyed State
            MapStateDescriptor<String, Double> mapStateDesc = new MapStateDescriptor<>(
                    "category-spending-map",
                    BasicTypeInfo.STRING_TYPE_INFO,
                    BasicTypeInfo.DOUBLE_TYPE_INFO
            );
            this.categorySpendingMapState = getRuntimeContext().getMapState(mapStateDesc);

            ListStateDescriptor<String> listStateDesc = new ListStateDescriptor<>(
                    "recent-orders-list",
                    BasicTypeInfo.STRING_TYPE_INFO
            );
            this.recentOrdersListState = getRuntimeContext().getListState(listStateDesc);
        }

        @Override
        public void flatMap(OrderEvent order, Collector<String> out) throws Exception {
            localSubtaskCount++;

            // 演示 MapState 高效点查与点写 (无需全量反序列化)
            String category = "CAT-" + ((int) order.getAmount() / 50);
            Double currentCatAmount = categorySpendingMapState.get(category);
            double updatedCatAmount = (currentCatAmount == null) ? order.getAmount() : currentCatAmount + order.getAmount();
            categorySpendingMapState.put(category, updatedCatAmount);

            // 演示 ListState 追加
            recentOrdersListState.add(order.getOrderId());

            // 迭代 MapState 汇聚用户总消费
            double totalUserSpending = 0.0;
            Iterator<Map.Entry<String, Double>> iterator = categorySpendingMapState.iterator();
            while (iterator.hasNext()) {
                totalUserSpending += iterator.next().getValue();
            }

            out.collect(String.format("User=%s UpdatedCat=%s CatTotal=%.2f GlobalUserTotal=%.2f SubtaskHandledCount=%d",
                    order.getUserId(), category, updatedCatAmount, totalUserSpending, localSubtaskCount));
        }

        /**
         * Checkpoint 快照持久化回调 (持久化 Operator State)
         */
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            operatorUnionTotalCountState.clear();
            operatorUnionTotalCountState.add(localSubtaskCount);
        }

        /**
         * 算子初始化 / 故障重启恢复回调
         */
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            // 声明 Operator UnionListState (故障时全量广播给所有并发)
            ListStateDescriptor<Long> unionListDesc = new ListStateDescriptor<>(
                    "operator-union-count-state",
                    BasicTypeInfo.LONG_TYPE_INFO
            );
            this.operatorUnionTotalCountState = context.getOperatorStateStore().getUnionListState(unionListDesc);

            if (context.isRestored()) {
                long restoredTotal = 0L;
                for (Long count : operatorUnionTotalCountState.get()) {
                    restoredTotal += count;
                }
                LOG.info(">>> [UnionListState Restored] Total count recovered across all subtasks: {}", restoredTotal);
            }
        }
    }
}
