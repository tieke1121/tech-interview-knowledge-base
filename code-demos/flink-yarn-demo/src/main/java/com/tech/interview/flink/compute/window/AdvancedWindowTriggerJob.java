package com.tech.interview.flink.compute.window;

import com.tech.interview.flink.model.OrderEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serial;
import java.time.Duration;
import java.util.Random;

/**
 * 【计算端核心考点：高级窗口机制、会话窗口、自定义 Trigger 与 Evictor】
 * 
 * 核心架构考点：
 * 1. 窗口触发器 TriggerResult 四大状态与内存语义：
 *    - CONTINUE: 保持现状，不触发计算，不清除窗口内容；
 *    - FIRE: 触发窗口计算并输出结果，但**保留**窗口内部状态（适合大窗口提前看趋势，后续数据依然可以累加）；
 *    - PURGE: 清空窗口内部状态，但不触发计算输出；
 *    - FIRE_AND_PURGE: 既触发计算输出，又立即销毁窗口状态，防止大窗口占用海量内存；
 * 2. 驱逐器 Evictor：
 *    - 在 Trigger 触发后、真正计算前 (evictBefore) 或计算后 (evictAfter) 剔除过期/异常数据 (如 CountEvictor 保留 TopN)；
 * 3. 会话窗口 (Session Window)：
 *    - EventTimeSessionWindows.withGap(Duration): 两个事件间隔超过 Gap 时自动分裂/切分会话，支持动态 Session 合并。
 */
public class AdvancedWindowTriggerJob {

    private static final Logger LOG = LoggerFactory.getLogger(AdvancedWindowTriggerJob.class);

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);

        // 1. 模拟订单事件流
        DataGeneratorSource<OrderEvent> orderSource = new DataGeneratorSource<>(
                new GeneratorFunction<>() {
                    @Serial
                    private static final long serialVersionUID = 1L;
                    private final Random random = new Random();

                    @Override
                    public OrderEvent map(Long value) {
                        String orderId = "ORD-" + value;
                        String userId = "USER-" + (100 + (value % 3)); // 3个用户触发密集累加
                        double amount = 50.0 + random.nextDouble() * 200.0;
                        return new OrderEvent(orderId, userId, amount, System.currentTimeMillis());
                    }
                },
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(10),
                TypeInformation.of(OrderEvent.class)
        );

        WatermarkStrategy<OrderEvent> watermarkStrategy = WatermarkStrategy
                .<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner((event, timestamp) -> event.getEventTime());

        DataStream<OrderEvent> orderStream = env
                .fromSource(orderSource, watermarkStrategy, "Order-Source");

        // 2. 场景 A: 1分钟滚动窗口 + 自定义提前触发器 (每满 3 条提前触发一次 FIRE，结束时全量 FIRE_AND_PURGE)
        DataStream<String> earlyTriggerStream = orderStream
                .keyBy(OrderEvent::getUserId)
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                .trigger(new EarlyCountOrTimeTrigger(3)) // 3条提前触发一次
                .evictor(CountEvictor.of(10))           // Evictor: 最多保留最近 10 条
                .process(new ProcessWindowFunction<OrderEvent, String, String, TimeWindow>() {
                    @Serial
                    private static final long serialVersionUID = 1L;

                    @Override
                    public void process(String key, Context context, Iterable<OrderEvent> elements, Collector<String> out) {
                        int count = 0;
                        double total = 0.0;
                        for (OrderEvent event : elements) {
                            count++;
                            total += event.getAmount();
                        }
                        out.collect(String.format("🔥 [Early Trigger Output] Window[%d - %d] Key=%s Count=%d TotalAmount=%.2f",
                                context.window().getStart(), context.window().getEnd(), key, count, total));
                    }
                });

        // 3. 场景 B: 会话窗口 (Session Window) 演示：用户行为停顿超过 10 秒算作独立会话
        DataStream<String> sessionStream = orderStream
                .keyBy(OrderEvent::getUserId)
                .window(EventTimeSessionWindows.withGap(Duration.ofSeconds(10)))
                .process(new ProcessWindowFunction<OrderEvent, String, String, TimeWindow>() {
                    @Serial
                    private static final long serialVersionUID = 1L;

                    @Override
                    public void process(String key, Context context, Iterable<OrderEvent> elements, Collector<String> out) {
                        int count = 0;
                        for (OrderEvent ignored : elements) {
                            count++;
                        }
                        out.collect(String.format("⏱️ [Session Window Closed] User=%s SessionSpan=[%d - %d] EventsCount=%d",
                                key, context.window().getStart(), context.window().getEnd(), count));
                    }
                });

        earlyTriggerStream.print();
        sessionStream.print();

        env.execute("Advanced Window Trigger and Session Demo Job");
    }

    /**
     * 自定义提前触发器：兼顾条数提前触发 (FIRE) 与窗口截止完全清理 (FIRE_AND_PURGE)
     */
    public static class EarlyCountOrTimeTrigger extends Trigger<OrderEvent, TimeWindow> {
        @Serial
        private static final long serialVersionUID = 1L;

        private final int maxCount;
        private final ValueStateDescriptor<Integer> countDesc =
                new ValueStateDescriptor<>("element-count", BasicTypeInfo.INT_TYPE_INFO);

        public EarlyCountOrTimeTrigger(int maxCount) {
            this.maxCount = maxCount;
        }

        @Override
        public TriggerResult onElement(OrderEvent element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            // 注册窗口截止事件时间定时器
            ctx.registerEventTimeTimer(window.maxTimestamp());

            ValueState<Integer> countState = ctx.getPartitionedState(countDesc);
            int currentCount = (countState.value() == null) ? 1 : countState.value() + 1;
            countState.update(currentCount);

            // 达到阈值提前触发计算，但不销毁状态 (FIRE)
            if (currentCount >= maxCount) {
                countState.clear();
                return TriggerResult.FIRE;
            }
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            // 窗口真正到期：触发最终汇总并清空销毁 (FIRE_AND_PURGE)
            if (time >= window.maxTimestamp()) {
                ctx.getPartitionedState(countDesc).clear();
                return TriggerResult.FIRE_AND_PURGE;
            }
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
            ctx.getPartitionedState(countDesc).clear();
            ctx.deleteEventTimeTimer(window.maxTimestamp());
        }
    }
}
