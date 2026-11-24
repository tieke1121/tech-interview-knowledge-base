package com.tech.interview.flink.compute.async;

import com.tech.interview.flink.model.OrderEvent;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serial;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.*;

/**
 * 【计算端核心考点：异步 I/O (Async I/O) 外部维表关联与本地缓存防击穿】
 * 
 * 核心架构考点：
 * 1. 为什么不用同步 map()：
 *    - 同步阻塞 RPC 会将线程挂起等待外部网络响应，单并行度吞吐从几万直接跌落至几百 TPS，造成上游剧烈级联反压；
 * 2. orderedWait vs unorderedWait 语义差异：
 *    - orderedWait: 严格按照输入顺序下发结果，依赖内部顺序队列，存在慢查询“队头阻塞 (Head-of-Line Blocking)”；
 *    - unorderedWait: 仅在遇到 Watermark 时充当对齐屏障，Watermark 之间的记录谁先返回谁先下发，吞吐提升 3~5 倍；
 * 3. 生产防穿透与防雪崩机制：
 *    - 在 AsyncFunction 内部集成堆内 LRU + TTL 缓存，命中缓存时直接同步返回，未命中才发起异步 RPC；
 * 4. 超时容错与优雅降级：
 *    - 重写 timeout() 方法，遇到外部存储超时/故障时返回默认兜底值，防止整个 Flink 作业级联崩溃。
 */
public class AsyncEnrichmentStreamJob {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncEnrichmentStreamJob.class);

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);

        // 1. 模拟输入订单流
        DataGeneratorSource<OrderEvent> orderSource = new DataGeneratorSource<>(
                new GeneratorFunction<>() {
                    @Serial
                    private static final long serialVersionUID = 1L;
                    private final Random random = new Random();

                    @Override
                    public OrderEvent map(Long value) {
                        String orderId = "ORD-" + value;
                        String userId = "USER-" + (1000 + (value % 20)); // 20个固定用户触发缓存命中
                        double amount = 10.0 + random.nextDouble() * 500.0;
                        return new OrderEvent(orderId, userId, amount, System.currentTimeMillis());
                    }
                },
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(50),
                TypeInformation.of(OrderEvent.class)
        );

        DataStream<OrderEvent> orderStream = env
                .fromSource(orderSource, org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(), "Order-Source");

        // 2. 核心异步维表关联算子 (unorderedWait + 1000ms 超时 + 100 队列容量)
        DataStream<EnrichedOrderEvent> enrichedStream = AsyncDataStream.unorderedWait(
                orderStream,
                new UserDimensionAsyncFunction(),
                1000,
                TimeUnit.MILLISECONDS,
                100
        );

        enrichedStream.print();

        env.execute("Async IO Dim Enrichment Job");
    }

    /**
     * 核心异步补全算子 (含本地缓存 + 异步线程池模拟 RPC + 超时兜底)
     */
    public static class UserDimensionAsyncFunction extends RichAsyncFunction<OrderEvent, EnrichedOrderEvent> {
        @Serial
        private static final long serialVersionUID = 1L;

        // 本地堆内 LRU 缓存：容量 1000，TTL 10 分钟
        private transient LocalTtlCache<String, String[]> userProfileCache;
        // 专有异步 RPC 线程池 (与 Flink 算子线程隔离)
        private transient ExecutorService rpcExecutor;

        @Override
        public void open(Configuration parameters) {
            this.userProfileCache = new LocalTtlCache<>(1000, TimeUnit.MINUTES.toMillis(10));
            this.rpcExecutor = Executors.newFixedThreadPool(8, new ThreadFactory() {
                private int counter = 0;
                @Override
                public Thread newThread(Runnable r) {
                    Thread t = new Thread(r, "dim-rpc-worker-" + (++counter));
                    t.setDaemon(true);
                    return t;
                }
            });
        }

        @Override
        public void close() {
            if (rpcExecutor != null) {
                rpcExecutor.shutdown();
            }
        }

        @Override
        public void asyncInvoke(OrderEvent order, ResultFuture<EnrichedOrderEvent> resultFuture) {
            long startTime = System.currentTimeMillis();

            // 1. 优先查本地堆内缓存 (纳秒级响应，防击穿外部存储)
            String[] cachedProfile = userProfileCache.get(order.getUserId());
            if (cachedProfile != null) {
                EnrichedOrderEvent enriched = new EnrichedOrderEvent(
                        order.getOrderId(), order.getUserId(), order.getAmount(),
                        cachedProfile[0], cachedProfile[1], System.currentTimeMillis() - startTime
                );
                resultFuture.complete(Collections.singletonList(enriched));
                return;
            }

            // 2. 未命中缓存，发起异步非阻塞查询 (CompletableFuture)
            CompletableFuture.supplyAsync(() -> {
                try {
                    // 模拟外部 Redis/HBase 查询延迟 10~30ms
                    Thread.sleep(15);
                    String city = "City-" + (Math.abs(order.getUserId().hashCode()) % 5);
                    String tier = (order.getAmount() > 200.0) ? "VIP" : "REGULAR";
                    return new String[]{city, tier};
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return new String[]{"UnknownCity", "DEFAULT"};
                }
            }, rpcExecutor).thenAccept(profile -> {
                // 写入本地缓存
                userProfileCache.put(order.getUserId(), profile);

                EnrichedOrderEvent enriched = new EnrichedOrderEvent(
                        order.getOrderId(), order.getUserId(), order.getAmount(),
                        profile[0], profile[1], System.currentTimeMillis() - startTime
                );
                resultFuture.complete(Collections.singletonList(enriched));
            }).exceptionally(ex -> {
                LOG.error("Failed to fetch user dim async: {}", ex.getMessage());
                // 异常兜底降级
                EnrichedOrderEvent fallback = new EnrichedOrderEvent(
                        order.getOrderId(), order.getUserId(), order.getAmount(),
                        "FallbackCity", "DEFAULT", System.currentTimeMillis() - startTime
                );
                resultFuture.complete(Collections.singletonList(fallback));
                return null;
            });
        }

        /**
         * 异步请求超时回调处理 (防止阻塞算子与队列溢出)
         */
        @Override
        public void timeout(OrderEvent order, ResultFuture<EnrichedOrderEvent> resultFuture) {
            LOG.warn("Async query TIMED OUT for order: {}, using fallback value", order.getOrderId());
            EnrichedOrderEvent timeoutFallback = new EnrichedOrderEvent(
                    order.getOrderId(), order.getUserId(), order.getAmount(),
                    "TimeoutCity", "TIMEOUT_DEGRADED", 1000L
            );
            resultFuture.complete(Collections.singletonList(timeoutFallback));
        }
    }
}
