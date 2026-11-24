package com.tech.interview.distributed.service;

import com.tech.interview.distributed.pool.MonitoredThreadPoolExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * 微服务高并发数据补全网关服务 (CompletableFuture 异步编排)
 *
 * 核心技术点：
 * 1. 消除传统串行 RPC 累加延迟：多路子系统并发异步拉取；
 * 2. 隔离业务自定义线程池，禁止使用公共 ForkJoinPool.commonPool()，防止慢任务拖垮整机；
 * 3. 超时强熔断兜底机制 (completeOnTimeout)，保障网关端到端 SLA 严苛履约。
 */
public class AsyncEnrichmentGatewayService {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncEnrichmentGatewayService.class);

    private final MonitoredThreadPoolExecutor executor;

    public AsyncEnrichmentGatewayService(MonitoredThreadPoolExecutor executor) {
        this.executor = executor;
    }

    /**
     * 聚合结果 DTO
     */
    public static class EnrichedUserData implements Serializable {
        private String userId;
        private String userProfile;
        private String riskScore;
        private double creditLimit;

        public EnrichedUserData(String userId, String userProfile, String riskScore, double creditLimit) {
            this.userId = userId;
            this.userProfile = userProfile;
            this.riskScore = riskScore;
            this.creditLimit = creditLimit;
        }

        public String getUserId() { return userId; }
        public String getUserProfile() { return userProfile; }
        public String getRiskScore() { return riskScore; }
        public double getCreditLimit() { return creditLimit; }

        @Override
        public String toString() {
            return String.format("EnrichedUserData[User=%s, Profile=%s, Risk=%s, Credit=%.2f]",
                    userId, userProfile, riskScore, creditLimit);
        }
    }

    /**
     * 并发拉取并补全用户画像、风控分与信贷额度
     */
    public CompletableFuture<EnrichedUserData> enrichUserDataAsync(String userId) {
        long start = System.currentTimeMillis();

        // 异步任务 1：拉取用户基础画像 (耗时约 50ms)
        CompletableFuture<String> profileFuture = CompletableFuture.supplyAsync(() -> {
            simulateLatency(50);
            return "VIP_LEVEL_3";
        }, executor).completeOnTimeout("DEFAULT_PROFILE", 200, TimeUnit.MILLISECONDS)
          .exceptionally(ex -> {
              LOG.warn("用户画像拉取异常降级: ", ex);
              return "FALLBACK_PROFILE";
          });

        // 异步任务 2：拉取实时风控评分 (模拟偶发超时，设置 100ms 熔断)
        CompletableFuture<String> riskFuture = CompletableFuture.supplyAsync(() -> {
            simulateLatency(80);
            return "LOW_RISK";
        }, executor).completeOnTimeout("HIGH_RISK_SHIELD", 120, TimeUnit.MILLISECONDS)
          .exceptionally(ex -> "DEFAULT_RISK");

        // 异步任务 3：拉取授信额度 (耗时约 60ms)
        CompletableFuture<Double> creditFuture = CompletableFuture.supplyAsync(() -> {
            simulateLatency(60);
            return 50000.0;
        }, executor).completeOnTimeout(0.0, 150, TimeUnit.MILLISECONDS)
          .exceptionally(ex -> 0.0);

        // 多路异步汇聚
        return CompletableFuture.allOf(profileFuture, riskFuture, creditFuture)
                .thenApply(v -> {
                    String profile = profileFuture.join();
                    String risk = riskFuture.join();
                    double credit = creditFuture.join();
                    LOG.info("用户 [{}] 多路数据补全完成，总耗时 {} ms", userId, (System.currentTimeMillis() - start));
                    return new EnrichedUserData(userId, profile, risk, credit);
                });
    }

    private void simulateLatency(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }
}
