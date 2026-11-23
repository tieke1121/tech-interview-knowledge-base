package com.tech.interview.flink;

import com.tech.interview.flink.function.OrderAggFunction;
import com.tech.interview.flink.function.OrderWindowProcessFunction;
import com.tech.interview.flink.model.OrderEvent;
import com.tech.interview.flink.model.RiskResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Random;

/**
 * Flink 1.20+ / 2.0 标准规范：实时订单风控流计算核心作业 (零过时 API 生产标准)
 *
 * 现代化技术特性：
 * 1. 【FLIP-27 现代 Source API】：采用 DataGeneratorSource + env.fromSource；
 * 2. 【声明式云原生 Configuration】：使用 CheckpointingOptions / StateBackendOptions / RestartStrategyOptions；
 * 3. 【原生 java.time.Duration】：开窗与迟到容忍全量采用 Duration；
 * 4. 【滚动事件时间窗口增量聚合】：AggregateFunction + ProcessWindowFunction 极致轻量聚合；
 * 5. 【两级迟到容错与死信路由】：allowedLateness 增量重算 + 侧输出流 (Side Output) 兜底。
 */
public class OrderRiskStreamingJob {

    private static final Logger LOG = LoggerFactory.getLogger(OrderRiskStreamingJob.class);

    /**
     * 迟到数据侧输出标签 (Side Output Tag)
     */
    public static final OutputTag<OrderEvent> LATE_ORDER_TAG =
            new OutputTag<OrderEvent>("late-order-side-output") {};

    public static void main(String[] args) throws Exception {
        LOG.info(">>> [Flink 1.20+] 启动实时风控增量聚合流计算作业...");

        // 1. 构建现代化声明式环境配置 (Flink 1.20+ 标准)
        Configuration configuration = buildProductionConfiguration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);

        // 2. 现代 FLIP-27 数据源 (模拟高并发轻度乱序订单流)
        DataGeneratorSource<OrderEvent> orderSource = new DataGeneratorSource<>(
                new GeneratorFunction<Long, OrderEvent>() {
                    private static final long serialVersionUID = 1L;
                    private final Random random = new Random();

                    @Override
                    public OrderEvent map(Long index) {
                        long now = System.currentTimeMillis();
                        // 构造轻度乱序 (0~2.5秒)，极少部分构造迟到数据
                        long eventTime = (index % 50 == 0) ? (now - 15000) : (now - random.nextInt(2500));
                        return new OrderEvent(
                                "ord_" + index,
                                "user_" + (index % 10 + 1),
                                50.0 + random.nextInt(450),
                                eventTime
                        );
                    }
                },
                1000L,
                RateLimiterStrategy.perSecond(50),
                Types.POJO(OrderEvent.class)
        );

        DataStream<OrderEvent> sourceStream = env.fromSource(
                orderSource,
                WatermarkStrategy
                        .<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((event, timestamp) -> event.getEventTime())
                        .withIdleness(Duration.ofSeconds(10)),
                "Source-Mock-Orders-FLIP27"
        ).name("Source-Mock-Orders").uid("source-mock-orders");

        // 3. 核心流式计算：分组、开窗 (Duration)、增量聚合、迟到容错 (Duration) 与侧道输出
        SingleOutputStreamOperator<RiskResult> riskMetricsStream = sourceStream
                .keyBy(OrderEvent::getUserId)
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                .allowedLateness(Duration.ofSeconds(5)) // 允许迟到 5 秒内触发增量重算
                .sideOutputLateData(LATE_ORDER_TAG)     // 超过 5 秒的极端迟到数据路由至侧输出流
                .aggregate(new OrderAggFunction(), new OrderWindowProcessFunction())
                .name("Window-Risk-Aggregator")
                .uid("operator-window-risk-agg");

        // 4. 主流输出：实时风控聚合指标 (对接下游 Kafka / Doris / 实时告警中枢)
        riskMetricsStream
                .print("Risk-Metrics-Main")
                .name("Sink-Risk-Metrics")
                .uid("sink-risk-metrics");

        // 5. 侧道输出：极端迟到数据分流，保全金融/业务审计一致性 (对接死信队列或离线数仓补数)
        DataStream<OrderEvent> lateDataStream = riskMetricsStream.getSideOutput(LATE_ORDER_TAG);
        lateDataStream
                .printToErr("Late-Order-DeadLetter")
                .name("Sink-Late-Orders")
                .uid("sink-late-orders");

        LOG.info(">>> 拓扑组装完成，执行作业调度...");
        env.execute("Realtime-Order-Risk-Streaming-Job");
    }

    /**
     * 现代化生产级环境参数声明 (Flink 1.20+ / 2.0 规范，零过时 API)
     */
    public static Configuration buildProductionConfiguration() {
        Configuration config = new Configuration();

        // 1. 状态后端声明式配置 (RocksDB 增量快照)
        config.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
        config.set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, true);

        // 2. Checkpoint 生产参数基线 (零过时 API)
        config.set(CheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(30));
        config.set(CheckpointingOptions.CHECKPOINTING_CONSISTENCY_MODE, CheckpointingMode.EXACTLY_ONCE);
        config.set(CheckpointingOptions.MIN_PAUSE_BETWEEN_CHECKPOINTS, Duration.ofSeconds(15));
        config.set(CheckpointingOptions.CHECKPOINTING_TIMEOUT, Duration.ofSeconds(60));
        config.set(CheckpointingOptions.MAX_CONCURRENT_CHECKPOINTS, 1);
        config.set(CheckpointingOptions.EXTERNALIZED_CHECKPOINT_RETENTION,
                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);

        // 3. 重试策略声明式配置 (指数退避)
        config.set(RestartStrategyOptions.RESTART_STRATEGY, "exponential-delay");
        config.set(RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_INITIAL_BACKOFF, Duration.ofMillis(500));
        config.set(RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_MAX_BACKOFF, Duration.ofSeconds(10));
        config.set(RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_BACKOFF_MULTIPLIER, 2.0);
        config.set(RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_RESET_BACKOFF_THRESHOLD, Duration.ofHours(1));
        config.set(RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_JITTER_FACTOR, 0.1);

        return config;
    }
}
