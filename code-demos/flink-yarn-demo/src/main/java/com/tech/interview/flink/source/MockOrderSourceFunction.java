package com.tech.interview.flink.source;

import com.tech.interview.flink.model.OrderEvent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.UUID;

/**
 * 生产模拟数据源 (RichParallelSourceFunction)
 * 支持多并行度并发生成实时订单事件，特意注入可控的轻度乱序与极少数极端迟到数据，
 * 供本地演练与面试中直观验证 Watermark 机制与 Side Output 侧道分流。
 */
public class MockOrderSourceFunction extends RichParallelSourceFunction<OrderEvent> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(MockOrderSourceFunction.class);

    private volatile boolean isRunning = true;
    private final int maxRecords;
    private final long emitIntervalMs;

    public MockOrderSourceFunction() {
        this(500, 100);
    }

    public MockOrderSourceFunction(int maxRecords, long emitIntervalMs) {
        this.maxRecords = maxRecords;
        this.emitIntervalMs = emitIntervalMs;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
        LOG.info("MockOrderSourceFunction subtask {} started.", subtaskIndex);
    }

    @Override
    public void run(SourceContext<OrderEvent> ctx) throws Exception {
        Random random = new Random();
        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
        int count = 0;

        while (isRunning && (maxRecords <= 0 || count < maxRecords)) {
            long now = System.currentTimeMillis();
            
            // 构造事件时间：90% 为轻微乱序 (0~2 秒)，5% 正常，5% 构造极端迟到 (迟到 15 秒，触发 Side Output)
            long eventTime;
            int disorderSeed = random.nextInt(100);
            if (disorderSeed < 5) {
                // 极端迟到数据 (超过 allowedLateness 5s + 3s watermark = 8s 窗口关闭阈值)
                eventTime = now - 15000;
            } else if (disorderSeed < 95) {
                // 正常轻微乱序 (在 3 秒 boundedOutOfOrderness 容忍范围内)
                eventTime = now - random.nextInt(2500);
            } else {
                eventTime = now;
            }

            String orderId = "ord_" + subtaskIndex + "_" + UUID.randomUUID().toString().substring(0, 8);
            String userId = "user_" + (random.nextInt(10) + 1); // 10 个热点用户模拟倾斜与分组
            double amount = 50.0 + random.nextInt(450);

            OrderEvent event = new OrderEvent(orderId, userId, amount, eventTime);

            // 发送数据并同步上报时间戳
            synchronized (ctx.getCheckpointLock()) {
                ctx.collectWithTimestamp(event, eventTime);
            }

            count++;
            if (emitIntervalMs > 0) {
                Thread.sleep(emitIntervalMs);
            }
        }

        LOG.info("MockOrderSourceFunction subtask {} finished emitting {} records.", subtaskIndex, count);
    }

    @Override
    public void cancel() {
        this.isRunning = false;
    }
}
