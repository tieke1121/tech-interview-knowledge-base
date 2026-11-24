package com.tech.interview.flink.compute.skew;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.Serial;
import java.io.Serializable;
import java.time.Duration;
import java.util.Locale;
import java.util.Random;

/**
 * 将热点店铺分散到五个盐桶，各桶先按事件时间汇总，再按店铺合并。
 * 局部窗口结果的时间戳为窗口结束时间减一毫秒，两个阶段使用相同的窗口边界。
 */
public class TwoStageSaltingJob {

    private static final Duration WINDOW_SIZE = Duration.ofSeconds(3);

    public static class SkewedOrder implements Serializable {
        @Serial
        private static final long serialVersionUID = 1L;

        private long eventTime;
        private String shopId;
        private double amount;

        public SkewedOrder() {}

        public SkewedOrder(String shopId, double amount) {
            this.shopId = shopId;
            this.amount = amount;
        }

        public long getEventTime() { return eventTime; }
        public void setEventTime(long eventTime) { this.eventTime = eventTime; }
        public String getShopId() { return shopId; }
        public void setShopId(String shopId) { this.shopId = shopId; }
        public double getAmount() { return amount; }
        public void setAmount(double amount) { this.amount = amount; }
    }

    public static class SaltedOrder implements Serializable {
        @Serial
        private static final long serialVersionUID = 1L;

        private String saltedKey; // 如 "SHOP_SUPER_HOT_3"
        private double amount;

        public SaltedOrder() {}

        public SaltedOrder(String saltedKey, double amount) {
            this.saltedKey = saltedKey;
            this.amount = amount;
        }

        public String getSaltedKey() { return saltedKey; }
        public void setSaltedKey(String saltedKey) { this.saltedKey = saltedKey; }
        public double getAmount() { return amount; }
        public void setAmount(double amount) { this.amount = amount; }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(4);

        // 1. 模拟严重倾斜的数据源 (90% 数据来自 SHOP_SUPER_HOT)
        DataGeneratorSource<SkewedOrder> skewedSource = new DataGeneratorSource<>(
                new GeneratorFunction<>() {
                    @Serial
                    private static final long serialVersionUID = 1L;
                    private final Random random = new Random();

                    @Override
                    public SkewedOrder map(Long value) {
                        String shop = (random.nextInt(100) < 90) ? "SHOP_SUPER_HOT" : "SHOP_REGULAR_" + random.nextInt(5);
                        SkewedOrder order = new SkewedOrder(shop, 10.0 + random.nextDouble() * 50.0);
                        order.setEventTime(System.currentTimeMillis());
                        return order;
                    }
                },
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(100),
                TypeInformation.of(SkewedOrder.class)
        );

        WatermarkStrategy<SkewedOrder> watermarks = WatermarkStrategy
                .<SkewedOrder>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner((order, previousTimestamp) -> order.getEventTime());
        aggregateByShop(env.fromSource(skewedSource, watermarks, "Skew-Source")).print();
        env.execute("Two-Stage Key Salting Skew Mitigation Job");
    }

    /** 输入流携带订单事件时间与 watermark。 */
    public static DataStream<String> aggregateByShop(DataStream<SkewedOrder> sourceStream) {
        // 2. 阶段一：加盐局部打散 (加 0~4 随机盐，将单 key 拆分为 5 个独立计算通道)
        DataStream<SaltedOrder> saltedStream = sourceStream.map(new MapFunction<SkewedOrder, SaltedOrder>() {
            @Serial
            private static final long serialVersionUID = 1L;
            private final Random random = new Random();

            @Override
            public SaltedOrder map(SkewedOrder order) {
                int salt = random.nextInt(5);
                return new SaltedOrder(order.getShopId() + "_" + salt, order.getAmount());
            }
        });

        // 局部预聚合 (3秒滚动窗口)
        DataStream<SkewedOrder> localAggStream = saltedStream
                .keyBy(SaltedOrder::getSaltedKey)
                .window(TumblingEventTimeWindows.of(WINDOW_SIZE))
                .aggregate(new LocalSumAggFunction(), new LocalProcessWindowFunction());

        // 3. 阶段二：去盐全局还原汇总 (3秒滚动窗口)
        return localAggStream
                .keyBy(SkewedOrder::getShopId)
                .window(TumblingEventTimeWindows.of(WINDOW_SIZE))
                .aggregate(new GlobalSumAggFunction(), new GlobalProcessWindowFunction());
    }

    // 局部增量聚合
    public static class LocalSumAggFunction implements AggregateFunction<SaltedOrder, Double, Double> {
        @Serial private static final long serialVersionUID = 1L;
        @Override public Double createAccumulator() { return 0.0; }
        @Override public Double add(SaltedOrder value, Double accumulator) { return accumulator + value.getAmount(); }
        @Override public Double getResult(Double accumulator) { return accumulator; }
        @Override public Double merge(Double a, Double b) { return a + b; }
    }

    // 局部包装并剥离盐值
    public static class LocalProcessWindowFunction extends ProcessWindowFunction<Double, SkewedOrder, String, TimeWindow> {
        @Serial private static final long serialVersionUID = 1L;
        @Override
        public void process(String saltedKey, Context context, Iterable<Double> elements, Collector<SkewedOrder> out) {
            double sum = elements.iterator().next();
            // 剥离盐值：恢复原始 shopId
            String originalShopId = saltedKey.substring(0, saltedKey.lastIndexOf('_'));
            out.collect(new SkewedOrder(originalShopId, sum));
        }
    }

    // 全局增量聚合
    public static class GlobalSumAggFunction implements AggregateFunction<SkewedOrder, Double, Double> {
        @Serial private static final long serialVersionUID = 1L;
        @Override public Double createAccumulator() { return 0.0; }
        @Override public Double add(SkewedOrder value, Double accumulator) { return accumulator + value.getAmount(); }
        @Override public Double getResult(Double accumulator) { return accumulator; }
        @Override public Double merge(Double a, Double b) { return a + b; }
    }

    // 全局输出
    public static class GlobalProcessWindowFunction extends ProcessWindowFunction<Double, String, String, TimeWindow> {
        @Serial private static final long serialVersionUID = 1L;
        @Override
        public void process(String shopId, Context context, Iterable<Double> elements, Collector<String> out) {
            double total = elements.iterator().next();
            out.collect(String.format(Locale.ROOT, "🌟 [Global Balanced Agg] ShopId=%s FinalTotal=%.2f WindowEnd=%d",
                    shopId, total, context.window().getEnd()));
        }
    }
}
