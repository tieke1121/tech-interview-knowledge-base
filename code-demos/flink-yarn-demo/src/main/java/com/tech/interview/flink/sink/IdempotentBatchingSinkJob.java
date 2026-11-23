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
 * 【Sink 端核心考点：现代 SinkV2 微批缓冲攒批写入、优雅排空与主键幂等去重】
 * 
 * 核心架构考点：
 * 1. 为什么不能单条写入外部存储：
 *    - 实时流每秒几万条，如果单条发起 RPC 写入 MySQL/ClickHouse/HBase，外部存储连接池与磁盘瞬间被打爆；
 * 2. 现代 SinkV2 批次攒批机制：
 *    - 摒弃已废弃的旧版 SinkFunction/RichSinkFunction，全面升级为 SinkV2 SinkWriter；
 *    - 数量阈值 (Batch Size): 累积达到 500 条立即触发批量刷写；
 *    - 时间阈值 (Flush Interval): 达到 1000ms 未满批次强制触发定时刷写，兼顾高吞吐与低延迟；
 * 3. 幂等写入设计哲学 (Idempotency)：
 *    - 基于业务唯一主键 orderId 在内存及下游做 UPSERT (如 MySQL ON DUPLICATE KEY UPDATE、Redis HSET、ClickHouse ReplacingMergeTree)；
 *    - At-Least-Once + 业务主键幂等 = 等效端到端 Exactly-Once (超高吞吐场景下的首选妥协架构)；
 * 4. 优雅停机与排空 (Graceful Drain)：
 *    - 在 close() 与 flush(true) 触发时排空剩余在途数据，避免进程退出时滞留数据丢失。
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
            LOG.info("🚀 [SinkV2 Batch Flushed] Idempotently written {} records to downstream database. Sample: {}",
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
