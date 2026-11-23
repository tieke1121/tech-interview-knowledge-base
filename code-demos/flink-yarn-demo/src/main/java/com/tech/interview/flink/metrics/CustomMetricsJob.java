package com.tech.interview.flink.metrics;

import com.tech.interview.flink.model.OrderEvent;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serial;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 【横向切面核心考点：自定义 MetricGroup 生产级监控告警与反压指标原理】
 * 
 * 核心架构考点：
 * 1. Flink 四大指标原语 (Metric Types)：
 *    - Counter: 累加计数 (如成功处理单量、丢弃单量、异常触发量)；
 *    - Gauge: 瞬时状态暴露 (如内存积压深度、活动线程数、JVM 堆外内存占用)；
 *    - Meter: 速率度量 (如 MeterView，滑动窗口实时 TPS)；
 *    - Histogram: 分布统计 (如请求端到端 P90/P99/P999 延迟分位数)；
 * 2. FLIP-33 / FLIP-123 现代反压度量底层原理：
 *    - 摒弃早期阻塞式的线程栈采样 (Stack Trace Sampling)；
 *    - 引入基于线程时间计数器：
 *      • backPressuredTimeMsPerSecond: 下游 Buffer 满导致当前算子写阻塞时间；
 *      • busyTimeMsPerSecond: 算子真正执行业务计算的时间；
 *      • idleTimeMsPerSecond: 算子等待上游数据到达的空闲时间；
 *      • 反压比率公式：BackPressureRatio = backPressuredTimeMsPerSecond / 1000。
 */
public class CustomMetricsJob {

    private static final Logger LOG = LoggerFactory.getLogger(CustomMetricsJob.class);

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);

        // 模拟订单流
        DataGeneratorSource<OrderEvent> orderSource = new DataGeneratorSource<>(
                new GeneratorFunction<>() {
                    @Serial
                    private static final long serialVersionUID = 1L;
                    private final Random random = new Random();

                    @Override
                    public OrderEvent map(Long value) {
                        String orderId = "ORD-" + value;
                        String userId = "USER-" + (100 + (value % 5));
                        double amount = 50.0 + random.nextDouble() * 200.0;
                        return new OrderEvent(orderId, userId, amount, System.currentTimeMillis());
                    }
                },
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(30),
                TypeInformation.of(OrderEvent.class)
        );

        DataStream<OrderEvent> orderStream = env
                .fromSource(orderSource, org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(), "Metric-Source");

        orderStream
                .map(new MonitoredOrderRichMapFunction())
                .print();

        env.execute("Custom MetricGroup Monitoring Job");
    }

    /**
     * 生产级埋点 Map 算子：集成 Counter, Gauge, MeterView
     */
    public static class MonitoredOrderRichMapFunction extends RichMapFunction<OrderEvent, String> {
        @Serial
        private static final long serialVersionUID = 1L;

        // 业务埋点指标
        private transient Counter processedOrdersCounter;
        private transient Counter flaggedRiskyOrdersCounter;
        private transient MeterView orderThroughputMeter;
        private transient AtomicInteger inFlightBacklog;

        @Override
        public void open(OpenContext openContext) {
            // 1. 注册 Counter
            this.processedOrdersCounter = getRuntimeContext()
                    .getMetricGroup()
                    .addGroup("business_sla")
                    .counter("orders_processed_total");

            this.flaggedRiskyOrdersCounter = getRuntimeContext()
                    .getMetricGroup()
                    .addGroup("business_sla")
                    .counter("orders_flagged_risky_total");

            // 2. 注册 MeterView (封装 processedOrdersCounter，统计 60 秒移动平均 TPS)
            this.orderThroughputMeter = getRuntimeContext()
                    .getMetricGroup()
                    .addGroup("business_sla")
                    .meter("orders_tps", new MeterView(processedOrdersCounter, 60));

            // 3. 注册 Gauge 暴露瞬时在途积压
            this.inFlightBacklog = new AtomicInteger(0);
            getRuntimeContext()
                    .getMetricGroup()
                    .addGroup("pipeline_health")
                    .gauge("in_flight_backlog", (Gauge<Integer>) inFlightBacklog::get);
        }

        @Override
        public String map(OrderEvent order) {
            inFlightBacklog.incrementAndGet();
            try {
                // 标记处理
                processedOrdersCounter.inc();

                // 规则判别埋点
                if (order.getAmount() > 150.0) {
                    flaggedRiskyOrdersCounter.inc();
                }

                return String.format("Processed Order %s, CurrentTPS=%.2f, RiskyTotal=%d",
                        order.getOrderId(), orderThroughputMeter.getRate(), flaggedRiskyOrdersCounter.getCount());
            } finally {
                inFlightBacklog.decrementAndGet();
            }
        }
    }
}
