package com.tech.interview.flink.function;

import com.tech.interview.flink.model.OrderEvent;
import com.tech.interview.flink.model.PaymentEvent;
import com.tech.interview.flink.model.ReconcileResult;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serial;
import java.time.Duration;

/**
 * 订单与支付双流连接协同处理算子 (KeyedCoProcessFunction)
 *
 * 核心技术考点与生产机制：
 * 1. 多数据源协同 (Multi-Source Connect)：处理异步到达的订单流与支付流；
 * 2. 托管状态协同 (Managed KeyedState)：使用 ValueState 缓存未对账成功的单边事件；
 * 3. 事件时间定时器 (EventTime Timer)：注册 10 秒超时定时器，检测超时未支付订单；
 * 4. 侧道输出分流 (Side Output)：未支付超时订单与孤儿支付流水打入死信侧道输出；
 * 5. 状态防膨胀与泄漏治理 (State TTL)：配置 TTL 防范恶意未决状态耗尽内存。
 */
public class OrderPaymentCoProcessFunction 
        extends KeyedCoProcessFunction<String, OrderEvent, PaymentEvent, ReconcileResult> {

    @Serial
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(OrderPaymentCoProcessFunction.class);

    // 侧道输出：超时未支付订单 (触发库存解冻与用户催付)
    public static final OutputTag<OrderEvent> TIMEOUT_ORDER_TAG =
            new OutputTag<OrderEvent>("timeout-unpaid-orders") {};

    // 侧道输出：孤儿支付流水 (先支付但长时间无订单，触发异常财务对账告警)
    public static final OutputTag<PaymentEvent> ORPHAN_PAYMENT_TAG =
            new OutputTag<PaymentEvent>("orphan-payments") {};

    // 状态定义
    private transient ValueState<OrderEvent> orderState;
    private transient ValueState<PaymentEvent> paymentState;
    private transient ValueState<Long> timerTimestampState;

    // 超时等待时长 (毫秒)：演示设置为 10 秒，生产中通常为 15 分钟 (900_000ms)
    private final long timeoutMs;

    public OrderPaymentCoProcessFunction() {
        this(10000L); // 默认 10 秒超时
    }

    public OrderPaymentCoProcessFunction(long timeoutMs) {
        this.timeoutMs = timeoutMs;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // 配置 State TTL：生产必备防泄漏规范，保障即便机器极端断电重启也能兜底清理
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Duration.ofHours(2))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();

        ValueStateDescriptor<OrderEvent> orderDesc = 
                new ValueStateDescriptor<>("order-state", OrderEvent.class);
        orderDesc.enableTimeToLive(ttlConfig);
        orderState = getRuntimeContext().getState(orderDesc);

        ValueStateDescriptor<PaymentEvent> paymentDesc = 
                new ValueStateDescriptor<>("payment-state", PaymentEvent.class);
        paymentDesc.enableTimeToLive(ttlConfig);
        paymentState = getRuntimeContext().getState(paymentDesc);

        ValueStateDescriptor<Long> timerDesc = 
                new ValueStateDescriptor<>("timer-ts-state", Long.class);
        timerTimestampState = getRuntimeContext().getState(timerDesc);
    }

    /**
     * 处理订单流事件 (Stream 1)
     */
    @Override
    public void processElement1(
            OrderEvent order, 
            Context ctx, 
            Collector<ReconcileResult> out) throws Exception {

        String orderId = ctx.getCurrentKey();
        PaymentEvent payment = paymentState.value();

        if (payment != null) {
            // 场景 A：支付流水已提前到达，当前订单到达即完成对账匹配！
            long duration = Math.abs(payment.getPayTime() - order.getEventTime());
            ReconcileResult result = new ReconcileResult(
                    orderId,
                    order.getUserId(),
                    order.getAmount(),
                    payment.getPayAmount(),
                    "SUCCESS_MATCHED_ORDER_LATE",
                    ctx.timerService().currentWatermark(),
                    duration
            );
            out.collect(result);

            // 删除此前为支付流水注册的等待定时器
            Long registeredTimer = timerTimestampState.value();
            if (registeredTimer != null) {
                ctx.timerService().deleteEventTimeTimer(registeredTimer);
            }

            // 清理状态，释放内存
            clearAllState();
            LOG.info("[对账成功] 订单先来后到完成对账: orderId={}, 耗时={}ms", orderId, duration);

        } else {
            // 场景 B：订单先到，支付尚未到达 -> 暂存订单，并注册事件时间超时定时器
            orderState.update(order);

            // 定时器触发点：以事件时间为基准，推进至 orderTime + timeoutMs 时触发
            long timerTs = order.getEventTime() + timeoutMs;
            ctx.timerService().registerEventTimeTimer(timerTs);
            timerTimestampState.update(timerTs);

            LOG.debug("[订单暂存] 注册对账定时器: orderId={}, 触发时间戳={}", orderId, timerTs);
        }
    }

    /**
     * 处理支付流事件 (Stream 2)
     */
    @Override
    public void processElement2(
            PaymentEvent payment, 
            Context ctx, 
            Collector<ReconcileResult> out) throws Exception {

        String orderId = ctx.getCurrentKey();
        OrderEvent order = orderState.value();

        if (order != null) {
            // 场景 C：订单已在等待，支付流水正常到达 -> 对账成功！
            long duration = Math.max(0, payment.getPayTime() - order.getEventTime());
            ReconcileResult result = new ReconcileResult(
                    orderId,
                    order.getUserId(),
                    order.getAmount(),
                    payment.getPayAmount(),
                    "SUCCESS_MATCHED",
                    ctx.timerService().currentWatermark(),
                    duration
            );
            out.collect(result);

            // 删除为订单注册的超时未支付定时器
            Long registeredTimer = timerTimestampState.value();
            if (registeredTimer != null) {
                ctx.timerService().deleteEventTimeTimer(registeredTimer);
            }

            // 清理状态
            clearAllState();
            LOG.info("[对账成功] 支付到达完成闭环: orderId={}, 支付金额={}, 耗时={}ms", 
                    orderId, payment.getPayAmount(), duration);

        } else {
            // 场景 D：支付先到 (可能因网络延迟或分流导致订单落后) -> 暂存支付，并注册防孤儿定时器
            paymentState.update(payment);

            long timerTs = payment.getPayTime() + timeoutMs;
            ctx.timerService().registerEventTimeTimer(timerTs);
            timerTimestampState.update(timerTs);

            LOG.warn("[支付先行到达] 暂存等待订单到达: orderId={}, 支付Id={}", orderId, payment.getPaymentId());
        }
    }

    /**
     * 事件时间定时器触发回调
     */
    @Override
    public void onTimer(
            long timestamp, 
            OnTimerContext ctx, 
            Collector<ReconcileResult> out) throws Exception {

        String orderId = ctx.getCurrentKey();
        OrderEvent order = orderState.value();
        PaymentEvent payment = paymentState.value();

        if (order != null && payment == null) {
            // 【超时未支付】: 超过容忍等待时间，仍无支付流水 -> 侧道分流，触发库存解冻与关单
            LOG.warn("[对账超时未支付] orderId={}, 用户={}, 下发关单侧流通知", orderId, order.getUserId());
            ctx.output(TIMEOUT_ORDER_TAG, order);

        } else if (payment != null && order == null) {
            // 【孤儿支付流水】: 只有支付没有订单 -> 侧道分流至财务人工风控核查表
            LOG.error("[对账异常: 孤儿支付] orderId={}, 支付Id={}, 金额={}", 
                    orderId, payment.getPaymentId(), payment.getPayAmount());
            ctx.output(ORPHAN_PAYMENT_TAG, payment);
        }

        // 定时器生命周期终结，彻底清理状态
        clearAllState();
    }

    private void clearAllState() throws Exception {
        orderState.clear();
        paymentState.clear();
        timerTimestampState.clear();
    }
}
