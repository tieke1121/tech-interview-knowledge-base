package com.tech.interview.flink;

import com.tech.interview.flink.function.OrderPaymentCoProcessFunction;
import com.tech.interview.flink.model.OrderEvent;
import com.tech.interview.flink.model.PaymentEvent;
import com.tech.interview.flink.model.ReconcileResult;
import com.tech.interview.flink.source.MockOrderSourceFunction;
import com.tech.interview.flink.source.MockPaymentSourceFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * Flink 多数据源、Watermark 协同对齐与事件时间定时器对账核心作业
 *
 * 核心技术全景：
 * 1. 【多数据源输入】：订单流 (OrderStream) + 支付流水 (PaymentStream)；
 * 2. 【Watermark 协同推进】：双流独立生成 BoundedOutOfOrderness 水印，底层算子自动按 min(W1, W2) 对齐推进；
 * 3. 【空闲分区探测 (withIdleness)】：防止特定 Kafka 分区无数据写入拖死全局 Watermark 推进；
 * 4. 【KeyedCoProcessFunction 与定时器】：基于事件时间注册超时定时器，实现超时关单与孤儿流水审计；
 * 5. 【两级侧道输出 (Side Output)】：超时未支付与孤儿流水精确分流，保全对账 100% 闭环。
 */
public class DualStreamReconciliationJob {

    private static final Logger LOG = LoggerFactory.getLogger(DualStreamReconciliationJob.class);

    public static void main(String[] args) throws Exception {
        LOG.info(">>> 启动 Flink 双流对账、Watermark 协同与定时器作业...");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1. 生产级 Checkpoint 与增量 RocksDB 快照配置
        configureProductionEnvironment(env);

        // 2. 数据源 1：订单事件流 (配置 3 秒乱序 Watermark + 10 秒空闲分区探测)
        DataStream<OrderEvent> orderStream = env
                .addSource(new MockOrderSourceFunction(500, 80))
                .name("Source-Order-Stream")
                .uid("source-orders")
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((event, timestamp) -> event.getEventTime())
                                .withIdleness(Duration.ofSeconds(10)) // 避免空闲分区阻塞全局 Watermark
                );

        // 3. 数据源 2：支付事件流 (配置 3 秒乱序 Watermark + 10 秒空闲分区探测)
        DataStream<PaymentEvent> paymentStream = env
                .addSource(new MockPaymentSourceFunction(500, 100))
                .name("Source-Payment-Stream")
                .uid("source-payments")
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<PaymentEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((event, timestamp) -> event.getPayTime())
                                .withIdleness(Duration.ofSeconds(10))
                );

        // 4. 双流连接与协同计算 (Connect + KeyBy 同一个 orderId + KeyedCoProcessFunction)
        SingleOutputStreamOperator<ReconcileResult> reconciliationStream = orderStream
                .connect(paymentStream)
                .keyBy(OrderEvent::getOrderId, PaymentEvent::getOrderId)
                .process(new OrderPaymentCoProcessFunction(10000L)) // 10 秒等待对账超时
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

    private static void configureProductionEnvironment(StreamExecutionEnvironment env) {
        env.enableCheckpointing(30000, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig config = env.getCheckpointConfig();
        config.setMinPauseBetweenCheckpoints(15000);
        config.setCheckpointTimeout(60000);
        config.setMaxConcurrentCheckpoints(1);
        config.setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );

        env.setStateBackend(new EmbeddedRocksDBStateBackend(true));

        env.setRestartStrategy(RestartStrategies.exponentialDelayRestart(
                Time.milliseconds(500),
                Time.seconds(10),
                2.0,
                Time.hours(1),
                0.1
        ));
    }
}
