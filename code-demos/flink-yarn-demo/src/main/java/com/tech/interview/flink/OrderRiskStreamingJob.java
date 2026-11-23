package com.tech.interview.flink;

import com.tech.interview.flink.function.OrderAggFunction;
import com.tech.interview.flink.function.OrderWindowProcessFunction;
import com.tech.interview.flink.model.OrderEvent;
import com.tech.interview.flink.model.RiskResult;
import com.tech.interview.flink.source.MockOrderSourceFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * Flink 实时订单风控流计算核心作业入口 (面向大厂面试与生产标准)
 * 包含：
 * 1. 生产级 Exactly-Once Checkpoint 与 RocksDB 增量状态快照
 * 2. 具备 3 秒容错能力的 BoundedOutOfOrderness 水印策略与空闲分区探测
 * 3. 1 分钟滚动事件时间窗口 + 增量聚合 (AggregateFunction)
 * 4. 延迟 5 秒重算 (allowedLateness) 与极端迟到数据侧输出流 (Side Output) 兜底
 */
public class OrderRiskStreamingJob {

    private static final Logger LOG = LoggerFactory.getLogger(OrderRiskStreamingJob.class);

    /**
     * 迟到数据侧输出标签 (Side Output Tag)
     */
    public static final OutputTag<OrderEvent> LATE_ORDER_TAG = 
            new OutputTag<OrderEvent>("late-order-side-output") {};

    public static void main(String[] args) throws Exception {
        LOG.info("Initializing Flink Order Risk Streaming Job...");

        // 1. 初始化流计算执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 生产级容错机制：Checkpoint 与状态后端配置
        configureCheckpointAndStateBackend(env);

        // 3. 构建数据源并提取事件时间 Watermark
        DataStream<OrderEvent> sourceStream = env
                .addSource(new MockOrderSourceFunction(1000, 50))
                .name("Mock-Order-Source")
                .uid("source-mock-orders")
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((event, timestamp) -> event.getEventTime())
                                .withIdleness(Duration.ofSeconds(10)) // 避免空闲分区卡住全局 Watermark 推进
                );

        // 4. 核心流式计算：分组、开窗、增量聚合、迟到容错与侧道输出
        SingleOutputStreamOperator<RiskResult> riskMetricsStream = sourceStream
                .keyBy(OrderEvent::getUserId)
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.minutes(1)))
                .allowedLateness(org.apache.flink.streaming.api.windowing.time.Time.seconds(5)) // 允许迟到 5 秒内的数据触发窗口增量重算
                .sideOutputLateData(LATE_ORDER_TAG) // 超过 5 秒的严重迟到数据路由至侧输出流
                .aggregate(new OrderAggFunction(), new OrderWindowProcessFunction())
                .name("Window-Risk-Aggregator")
                .uid("operator-window-risk-agg");

        // 5. 主流输出：实时风控聚合指标 (对接下游 Kafka / Doris / 实时告警中枢)
        riskMetricsStream
                .print("Risk-Metrics-Main")
                .name("Sink-Risk-Metrics")
                .uid("sink-risk-metrics");

        // 6. 侧道输出：极端迟到数据分流，保全金融/业务审计一致性 (对接死信队列或离线数仓补数)
        DataStream<OrderEvent> lateDataStream = riskMetricsStream.getSideOutput(LATE_ORDER_TAG);
        lateDataStream
                .printToErr("Late-Order-DeadLetter")
                .name("Sink-Late-Orders")
                .uid("sink-late-orders");

        LOG.info("Execution topology prepared. Triggering env.execute()...");
        env.execute("Realtime-Order-Risk-Streaming-Job");
    }

    /**
     * 配置生产级 Checkpoint 策略与 RocksDB 增量状态后端
     */
    private static void configureCheckpointAndStateBackend(StreamExecutionEnvironment env) {
        // 开启精确一次 Checkpoint，每 30 秒周期触发
        env.enableCheckpointing(30000, CheckpointingMode.EXACTLY_ONCE);

        CheckpointConfig config = env.getCheckpointConfig();
        // 最小快照暂停时间：前一次 Checkpoint 结束后至少等待 15 秒才触发下一次，防止频繁快照拖死集群网络
        config.setMinPauseBetweenCheckpoints(15000);
        // 单次 Checkpoint 超时丢弃上限：60 秒
        config.setCheckpointTimeout(60000);
        // 最大并发快照数：1，避免同一算子多快照争用磁盘与网络
        config.setMaxConcurrentCheckpoints(1);
        // 作业被主动 cancel 时保留 Checkpoint，便于后续 Savepoint 恢复或回溯
        config.setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );

        // 状态后端选型：TB 级超大状态生产必备首选 RocksDB (启用增量快照 incremental=true)
        env.setStateBackend(new EmbeddedRocksDBStateBackend(true));

        // 生产容错重启策略：指数退避重试 (避免短时网络或下游服务抖动引发雪崩式重试)
        env.setRestartStrategy(RestartStrategies.exponentialDelayRestart(
                Time.milliseconds(500), // 初始等待 500ms
                Time.seconds(10),       // 最大等待 10s
                2.0,                    // 退避倍数
                Time.hours(1),          // 统计重置时间窗口
                0.1                     // 抖动因子 (0.1 表示加入 10% 随机抖动避免羊群效应)
        ));
    }
}
