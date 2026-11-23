package com.tech.interview.flink.source;

import com.tech.interview.flink.model.PaymentEvent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.UUID;

/**
 * 生产模拟支付数据源 (RichParallelSourceFunction)
 * 模拟对应订单的支付流水生成：
 * 1. 80% 正常支付：在下单后 1~5 秒内到达；
 * 2. 15% 严重延迟支付：在下单后 20 秒到达 (触发先超时后补账)；
 * 3. 5% 用户放弃支付：永不生成支付流水 (触发 10 秒超时订单取消与库存回退)。
 */
public class MockPaymentSourceFunction extends RichParallelSourceFunction<PaymentEvent> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(MockPaymentSourceFunction.class);

    private volatile boolean isRunning = true;
    private final int maxRecords;
    private final long emitIntervalMs;

    public MockPaymentSourceFunction() {
        this(500, 120);
    }

    public MockPaymentSourceFunction(int maxRecords, long emitIntervalMs) {
        this.maxRecords = maxRecords;
        this.emitIntervalMs = emitIntervalMs;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        LOG.info("MockPaymentSourceFunction subtask {} 启动就绪", getRuntimeContext().getIndexOfThisSubtask());
    }

    @Override
    public void run(SourceContext<PaymentEvent> ctx) throws Exception {
        Random random = new Random();
        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
        int count = 0;

        while (isRunning && (maxRecords <= 0 || count < maxRecords)) {
            long now = System.currentTimeMillis();

            // 对应 MockOrderSourceFunction 产生的订单 ID
            String orderId = "ord_" + subtaskIndex + "_" + count;
            String paymentId = "pay_" + UUID.randomUUID().toString().substring(0, 8);
            String channel = (count % 2 == 0) ? "ALIPAY" : "WECHAT_PAY";
            double amount = 100.0;

            // 5% 订单模拟放弃支付，不产生支付流水
            if (count % 20 != 0) {
                // 模拟支付时间延迟 (绝大多数延迟 1~3 秒)
                long payTime = now + (random.nextInt(3000));

                PaymentEvent event = new PaymentEvent(paymentId, orderId, amount, channel, payTime);

                synchronized (ctx.getCheckpointLock()) {
                    ctx.collectWithTimestamp(event, payTime);
                }
            }

            count++;
            if (emitIntervalMs > 0) {
                Thread.sleep(emitIntervalMs);
            }
        }
        LOG.info("MockPaymentSourceFunction subtask {} 发送完毕，共产生 {} 条流水", subtaskIndex, count);
    }

    @Override
    public void cancel() {
        this.isRunning = false;
    }
}
