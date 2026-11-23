package com.tech.interview.distributed;

import com.tech.interview.distributed.pool.MonitoredThreadPoolExecutor;
import com.tech.interview.distributed.service.AsyncEnrichmentGatewayService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 动态监控线程池与 CompletableFuture 异步网关单元测试
 */
class DistributedConcurrencyTest {

    private MonitoredThreadPoolExecutor executor;

    @BeforeEach
    void setUp() {
        executor = new MonitoredThreadPoolExecutor(
                "TestPool",
                2,
                4,
                30,
                TimeUnit.SECONDS,
                10
        );
    }

    @AfterEach
    void tearDown() {
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    @Test
    @DisplayName("验证线程池动态容量热调优与监控指标采集")
    void testDynamicThreadPoolTuning() {
        assertEquals(2, executor.getCorePoolSize());
        assertEquals(4, executor.getMaximumPoolSize());

        // 动态调优
        executor.adjustPoolSize(4, 8);
        assertEquals(4, executor.getCorePoolSize());
        assertEquals(8, executor.getMaximumPoolSize());

        String metrics = executor.getHealthMetrics();
        assertNotNull(metrics);
        assertTrue(metrics.contains("TestPool"));
    }

    @Test
    @DisplayName("验证 CompletableFuture 多路异步补全与聚合正确性")
    void testAsyncEnrichmentGateway() throws Exception {
        AsyncEnrichmentGatewayService gateway = new AsyncEnrichmentGatewayService(executor);

        CompletableFuture<AsyncEnrichmentGatewayService.EnrichedUserData> future =
                gateway.enrichUserDataAsync("user_9999");

        AsyncEnrichmentGatewayService.EnrichedUserData data = future.get(3, TimeUnit.SECONDS);

        assertNotNull(data);
        assertEquals("user_9999", data.getUserId());
        assertEquals("VIP_LEVEL_3", data.getUserProfile());
        assertEquals("LOW_RISK", data.getRiskScore());
        assertEquals(50000.0, data.getCreditLimit(), 0.001);
    }
}
