package com.tech.interview.flink;

import com.tech.interview.flink.function.OrderPaymentCoProcessFunction;
import com.tech.interview.flink.model.OrderEvent;
import com.tech.interview.flink.model.PaymentEvent;
import com.tech.interview.flink.model.ReconcileResult;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Random;

/**
 * Flink 1.20+ / 2.0 标准规范：多数据源协同、Watermark 对齐与事件时间定时器双流对账核心作业
 *
 * 现代化技术特性 (100% 消除过时 API)：
 * 1. 【FLIP-27 现代 Source API】：采用 DataGeneratorSource + env.fromSource 取代已废弃的 SourceFunction；
 * 2. 【声明式云原生 Configuration】：通过 CheckpointingOptions、StateBackendOptions、RestartStrategyOptions
 *    解耦配置，淘汰已废弃的 enableCheckpointing / setStateBackend / RestartStrategies；
 * 3. 【原生 Java 8+ Duration】：全面使用 java.time.Duration 替代已过时的 Flink Time；
 * 4. 【Watermark 动态协同推进】：双流独立提取 BoundedOutOfOrderness 水印，底层按 min(W1, W2) 对齐；
 * 5. 【空闲分区探测 (withIdleness)】：防止空闲分区拉低全局 Watermark；
 * 6. 【KeyedCoProcessFunction 与 EventTime 定时器】：实现超时关单与孤儿支付两级侧道输出 (Side Output)。
 */
public class DualStreamReconciliationJob {

    private static final Logger LOG = LoggerFactory.getLogger(DualStreamReconciliationJob.class);

    public static void main(String[] args) throws Exception {
        LOG.info(">>> [Flink 1.20+] 启动双流对账、Watermark 协同与定时器现代作业...");

        // 1. 构建现代化声明式配置上下文 (Flink 1.20+ / 2.0 最佳实践)
        Configuration configuration = buildProductionConfiguration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);

        // 2. 数据源 1：订单事件流 (基于 FLIP-27 DataGeneratorSource + env.fromSource)
        DataGeneratorSource<OrderEvent> orderSource = new DataGeneratorSource<>(
                new GeneratorFunction<Long, OrderEvent>() {
                    private static final long serialVersionUID = 1L;
                    private final Random random = new Random();

                    @Override
                    public OrderEvent map(Long index) {
                        long now = System.currentTimeMillis();
                        // 构造轻度乱序事件时间
                        long eventTime = now - random.nextInt(2500);
                        return new OrderEvent(
                                "ord_" + index,
                                "user_" + (index % 10 + 1),
                                100.0 + random.nextInt(400),
                                eventTime
                        );
                    }
                },
                1000L,
                RateLimiterStrategy.perSecond(30),
                Types.POJO(OrderEvent.class)
        );

        DataStream<OrderEvent> orderStream = env.fromSource(
                orderSource,
                WatermarkStrategy
                        .<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((event, timestamp) -> event.getEventTime())
                        .withIdleness(Duration.ofSeconds(10)), // 避免空闲分区阻塞全局 Watermark
                "Source-Orders-FLIP27"
        ).name("Source-Orders").uid("source-orders");

        // 3. 数据源 2：支付事件流 (基于 FLIP-27 DataGeneratorSource + env.fromSource)
        DataGeneratorSource<PaymentEvent> paymentSource = new DataGeneratorSource<>(
                new GeneratorFunction<Long, PaymentEvent>() {
                    private static final long serialVersionUID = 1L;
                    private final Random random = new Random();

                    @Override
                    public PaymentEvent map(Long index) {
                        long now = System.currentTimeMillis();
                        long payTime = now - random.nextInt(2000);
                        String channel = (index % 2 == 0) ? "ALIPAY" : "WECHAT_PAY";
                        return new PaymentEvent(
                                "pay_" + index,
                                "ord_" + index,
                                100.0 + (index % 400),
                                channel,
                                payTime
                        );
                    }
                },
                1000L,
                RateLimiterStrategy.perSecond(25),
                Types.POJO(PaymentEvent.class)
        );

        DataStream<PaymentEvent> paymentStream = env.fromSource(
                paymentSource,
                WatermarkStrategy
                        .<PaymentEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((event, timestamp) -> event.getPayTime())
                        .withIdleness(Duration.ofSeconds(10)),
                "Source-Payments-FLIP27"
        ).name("Source-Payments").uid("source-payments");

        // 4. 双流连接与协同计算 (Connect + KeyBy 同一个 orderId + KeyedCoProcessFunction)
        SingleOutputStreamOperator<ReconcileResult> reconciliationStream = orderStream
                .connect(paymentStream)
                .keyBy(OrderEvent::getOrderId, PaymentEvent::getOrderId)
                .process(new OrderPaymentCoProcessFunction(10000L)) // 10 秒等待超时
                .name("Operator-Order-Payment-CoProcess")
                .uid("operator-reconcile-coprocess");

        // 5. 主流输出：对账成功结果 (输出至实时数仓或财务大屏)
        reconciliationStream
                .print("Matched-Reconcile")
                .name("Sink-Matched-Results")
                .uid("sink-matched");

        // 6. 侧道输出 1：超时未支付订单 (输出至关单解冻库存中枢)
        DataStream<OrderEvent> timeoutOrders = reconciliationStream
                .getSideOutput(OrderPaymentCoProcessFunction.TIMEOUT_ORDER_TAG);
        timeoutOrders
                .printToErr("Timeout-Unpaid-Alert")
                .name("Sink-Timeout-Unpaid")
                .uid("sink-timeout-orders");

        // 7. 侧道输出 2：孤儿支付异常流水 (输出至财务风控人工核查队列)
        DataStream<PaymentEvent> orphanPayments = reconciliationStream
                .getSideOutput(OrderPaymentCoProcessFunction.ORPHAN_PAYMENT_TAG);
        orphanPayments
                .printToErr("Orphan-Payment-Alert")
                .name("Sink-Orphan-Payments")
                .uid("sink-orphan-payments");

        LOG.info(">>> 拓扑组装完成，执行作业调度...");
        env.execute("Dual-Stream-Reconciliation-Streaming-Job");
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

        // 3. 重试策略声明式配置 (指数退避，消灭过时的 RestartStrategies)
        config.set(RestartStrategyOptions.RESTART_STRATEGY, "exponential-delay");
        config.set(RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_INITIAL_BACKOFF, Duration.ofMillis(500));
        config.set(RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_MAX_BACKOFF, Duration.ofSeconds(10));
        config.set(RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_BACKOFF_MULTIPLIER, 2.0);
        config.set(RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_RESET_BACKOFF_THRESHOLD, Duration.ofHours(1));
        config.set(RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_JITTER_FACTOR, 0.1);

        return config;
    }
}
