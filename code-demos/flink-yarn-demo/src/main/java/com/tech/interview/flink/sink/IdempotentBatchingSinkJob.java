package com.tech.interview.flink.sink;

import com.tech.interview.flink.model.OrderEvent;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serial;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.*;

/**
 * 演示按 50 个不同订单键或 1 秒触发批次输出。示例在内存中按键覆盖，输出为日志。
 * 对外部表重试时，需要在存储端用主键及业务版本实现幂等写入。
 */
public class IdempotentBatchingSinkJob {

    private static final Logger LOG = LoggerFactory.getLogger(IdempotentBatchingSinkJob.class);

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);

        // 模拟高吞吐订单流
        DataGeneratorSource<OrderEvent> orderSource = new DataGeneratorSource<>(
                new GeneratorFunction<>() {
                    @Serial
                    private static final long serialVersionUID = 1L;
                    private final Random random = new Random();

                    @Override
                    public OrderEvent map(Long value) {
                        String orderId = "ORD-" + value;
                        String userId = "USER-" + (100 + (value % 10));
                        return new OrderEvent(orderId, userId, 99.0 + random.nextDouble(), System.currentTimeMillis());
                    }
                },
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(100),
                TypeInformation.of(OrderEvent.class)
        );

        DataStream<OrderEvent> orderStream = env
                .fromSource(orderSource, org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(), "Order-Source");

        // 接入现代 SinkV2 微批幂等缓冲 Sink
        orderStream.sinkTo(new ModernIdempotentBatchingSink(50, 1000));

        env.execute("Modern Idempotent Batching Sink Job");
    }

    /**
     * 现代 SinkV2 拓扑入口类
     */
    public static class ModernIdempotentBatchingSink implements Sink<OrderEvent>, Serializable {
        @Serial
        private static final long serialVersionUID = 1L;

        private final int batchSizeThreshold;
        private final long flushIntervalMs;

        public ModernIdempotentBatchingSink(int batchSizeThreshold, long flushIntervalMs) {
            this.batchSizeThreshold = batchSizeThreshold;
            this.flushIntervalMs = flushIntervalMs;
        }

        @Override
        @SuppressWarnings("deprecation")
        public SinkWriter<OrderEvent> createWriter(InitContext context) {
            return new IdempotentBatchingSinkWriter(batchSizeThreshold, flushIntervalMs);
        }
    }

    /**
     * 核心写入与微批缓冲器：SinkWriter
     */
    public static class IdempotentBatchingSinkWriter implements SinkWriter<OrderEvent> {

        private final int batchSizeThreshold;
        private final Map<String, OrderEvent> memoryBatchBuffer = new LinkedHashMap<>();
        private final Object lock = new Object();
        private final ScheduledExecutorService scheduler;

        public IdempotentBatchingSinkWriter(int batchSizeThreshold, long flushIntervalMs) {
            this.batchSizeThreshold = batchSizeThreshold;

            // 定时冲刷调度器
            this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "sink-batch-flusher");
                t.setDaemon(true);
                return t;
            });
            this.scheduler.scheduleWithFixedDelay(
                    this::flushBuffer, flushIntervalMs, flushIntervalMs, TimeUnit.MILLISECONDS
            );
        }

        @Override
        public void write(OrderEvent element, Context context) {
            synchronized (lock) {
                // 批内按 orderId 幂等去重覆盖
                memoryBatchBuffer.put(element.getOrderId(), element);

                if (memoryBatchBuffer.size() >= batchSizeThreshold) {
                    flushBuffer();
                }
            }
        }

        @Override
        public void flush(boolean endOfInput) {
            flushBuffer();
        }

        private void flushBuffer() {
            List<OrderEvent> batchToFlush;
            synchronized (lock) {
                if (memoryBatchBuffer.isEmpty()) {
                    return;
                }
                batchToFlush = new ArrayList<>(memoryBatchBuffer.values());
                memoryBatchBuffer.clear();
            }

            // 模拟批量执行 UPSERT 写入下游存储
            LOG.info("🚀 [SinkV2 Batch Flushed] Simulated upsert of {} records. Sample: {}",
                    batchToFlush.size(), batchToFlush.get(0).getOrderId());
        }

        @Override
        public void close() {
            if (scheduler != null) {
                scheduler.shutdown();
            }
            flushBuffer(); // 优雅关闭时排空剩余数据
        }
    }
}
