package com.tech.interview.flink.source;

import com.tech.interview.flink.model.OrderEvent;
import org.apache.flink.api.common.eventtime.*;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serial;
import java.time.Duration;
import java.util.Random;

/**
 * 【Source 端核心考点：自定义 Watermark 生成器与标点符号式/周期性推进】
 * 
 * 核心架构考点：
 * 1. 周期性 (Periodic Watermark)：由框架定时在 onPeriodicEmit() 发射，依赖 pipeline.auto-watermark-interval (默认 200ms)；
 * 2. 标点符号式 (Punctuated Watermark)：针对特定核心事件 (如特定高优先级 VIP 订单、批次结束标记)，在 onEvent() 中立即发射 Watermark；
 * 3. 空闲流防死锁 (withIdleness)：防止个别分区无数据导致下游多输入通道取 min(W1, W2) 永远卡死。
 */
public class CustomPunctuatedWatermarkJob {

    private static final Logger LOG = LoggerFactory.getLogger(CustomPunctuatedWatermarkJob.class);

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);

        // 1. 模拟生成订单数据源
        DataGeneratorSource<OrderEvent> orderSource = new DataGeneratorSource<>(
                new GeneratorFunction<>() {
                    @Serial
                    private static final long serialVersionUID = 1L;
                    private final Random random = new Random();

                    @Override
                    public OrderEvent map(Long value) {
                        String orderId = "ORD-" + value;
                        // 随机模拟部分订单为 VIP 高优订单
                        String userId = (value % 5 == 0) ? "VIP-" + value : "USER-" + (100 + random.nextInt(20));
                        double amount = 50.0 + random.nextDouble() * 500.0;
                        long now = System.currentTimeMillis();
                        return new OrderEvent(orderId, userId, amount, now);
                    }
                },
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(20),
                TypeInformation.of(OrderEvent.class)
        );

        // 2. 自定义混合型 WatermarkStrategy：结合标点式 (VIP立即推进) 与周期性 (乱序兜底)
        WatermarkStrategy<OrderEvent> customWatermarkStrategy = WatermarkStrategy
                .forGenerator((WatermarkGeneratorSupplier<OrderEvent>) context -> new PunctuatedOrderWatermarkGenerator())
                .withTimestampAssigner((event, timestamp) -> event.getEventTime())
                .withIdleness(Duration.ofSeconds(10)); // 10秒无数据标记空闲，避免卡死下游窗口

        DataStream<OrderEvent> orderStream = env
                .fromSource(orderSource, customWatermarkStrategy, "Custom-Watermark-Source");

        // 3. 5秒滚动事件时间窗口验证 Watermark 推进
        orderStream
                .keyBy(OrderEvent::getUserId)
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(5)))
                .process(new ProcessWindowFunction<OrderEvent, String, String, TimeWindow>() {
                    @Serial
                    private static final long serialVersionUID = 1L;

                    @Override
                    public void process(String key, Context context, Iterable<OrderEvent> elements, Collector<String> out) {
                        int count = 0;
                        double sum = 0.0;
                        for (OrderEvent event : elements) {
                            count++;
                            sum += event.getAmount();
                        }
                        long start = context.window().getStart();
                        long end = context.window().getEnd();
                        out.collect(String.format("Window[%d - %d] Key=%s Count=%d TotalAmount=%.2f",
                                start, end, key, count, sum));
                    }
                })
                .print();

        env.execute("Custom Punctuated Watermark Demo Job");
    }

    /**
     * 自定义标点与周期混合型 WatermarkGenerator
     */
    public static class PunctuatedOrderWatermarkGenerator extends BoundedOutOfOrdernessWatermarks<OrderEvent> {
        public PunctuatedOrderWatermarkGenerator() {
            super(Duration.ofSeconds(3));
        }

        @Override
        public void onEvent(OrderEvent event, long eventTimestamp, WatermarkOutput output) {
            super.onEvent(event, eventTimestamp, output);
            // VIP 只触发一次提前发送，仍沿用本分区的最大时间戳和三秒乱序边界。
            if (event.getUserId() != null && event.getUserId().startsWith("VIP-")) {
                onPeriodicEmit(output);
                LOG.debug("VIP order {} triggered watermark emission", event.getOrderId());
            }
        }
    }
}
