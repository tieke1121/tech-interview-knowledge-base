package com.tech.interview.distributed.pool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 生产级高可用动态监控线程池 (MonitoredThreadPoolExecutor)
 *
 * 核心设计优势：
 * 1. 杜绝 Executors 默认无界队列（LinkedBlockingQueue 易导致 JVM OOM）；
 * 2. 采用显式有界队列 (ArrayBlockingQueue) 并支持自定义降级/告警拒绝策略；
 * 3. 拦截 beforeExecute/afterExecute 监控任务执行耗时与异常；
 * 4. 支持线上无重启热更新（Dynamic Tuning）核心线程数与最大线程数。
 */
public class MonitoredThreadPoolExecutor extends ThreadPoolExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(MonitoredThreadPoolExecutor.class);

    private final String poolName;
    private final AtomicLong totalTaskTime = new AtomicLong(0);
    private final AtomicLong completedTasks = new AtomicLong(0);
    private final AtomicLong rejectedTasks = new AtomicLong(0);
    private final ThreadLocal<Long> startTimeThreadLocal = new ThreadLocal<>();

    public MonitoredThreadPoolExecutor(
            String poolName,
            int corePoolSize,
            int maximumPoolSize,
            long keepAliveTime,
            TimeUnit unit,
            int queueCapacity) {
        super(
                corePoolSize,
                maximumPoolSize,
                keepAliveTime,
                unit,
                new ArrayBlockingQueue<>(queueCapacity),
                new NamedThreadFactory(poolName),
                new LoggingCallerRunsPolicy(poolName)
        );
        this.poolName = poolName;
    }

    @Override
    protected void beforeExecute(Thread t, Runnable r) {
        super.beforeExecute(t, r);
        startTimeThreadLocal.set(System.currentTimeMillis());
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        try {
            Long startTime = startTimeThreadLocal.get();
            if (startTime != null) {
                long duration = System.currentTimeMillis() - startTime;
                totalTaskTime.addAndGet(duration);
                completedTasks.incrementAndGet();

                // 慢任务告警监控 (超过 500ms 打印告警日志)
                if (duration > 500) {
                    LOG.warn("[{}] 探测到长尾慢任务: 耗时 {} ms, 当前队列积压: {}",
                            poolName, duration, getQueue().size());
                }
            }

            if (t != null) {
                LOG.error("[{}] 任务执行抛出未捕获异常: ", poolName, t);
            }
        } finally {
            startTimeThreadLocal.remove();
            super.afterExecute(r, t);
        }
    }

    /**
     * 动态热调优核心与最大线程数 (无需重启应用)
     */
    public synchronized void adjustPoolSize(int newCore, int newMax) {
        if (newCore <= 0 || newMax < newCore) {
            throw new IllegalArgumentException("参数不合法: newCore 必须 > 0 且 newMax >= newCore");
        }
        LOG.info("[{}] 动态调整线程池容量: 核心线程 {} -> {}, 最大线程 {} -> {}",
                poolName, getCorePoolSize(), newCore, getMaximumPoolSize(), newMax);
        
        // 必须先调大 max 再调大 core，或先调小 core 再调小 max，防止触发 IllegalArgumentException
        if (newMax > getMaximumPoolSize()) {
            setMaximumPoolSize(newMax);
            setCorePoolSize(newCore);
        } else {
            setCorePoolSize(newCore);
            setMaximumPoolSize(newMax);
        }
    }

    /**
     * 获取线程池健康度快照
     */
    public String getHealthMetrics() {
        long completed = completedTasks.get();
        double avgTime = completed > 0 ? (double) totalTaskTime.get() / completed : 0.0;
        return String.format(
                "ThreadPool[%s] => 活跃线程: %d/%d, 核心: %d, 队列排队: %d/%d, 已完成: %d, 拒绝: %d, 平均耗时: %.2f ms",
                poolName,
                getActiveCount(),
                getMaximumPoolSize(),
                getCorePoolSize(),
                getQueue().size(),
                getQueue().remainingCapacity() + getQueue().size(),
                completed,
                rejectedTasks.get(),
                avgTime
        );
    }

    public void incrementRejectedCount() {
        rejectedTasks.incrementAndGet();
    }

    public long getRejectedCount() {
        return rejectedTasks.get();
    }

    public String getPoolName() {
        return poolName;
    }

    /**
     * 规范化命名的线程工厂
     */
    public static class NamedThreadFactory implements ThreadFactory {
        private final String prefix;
        private final AtomicInteger threadNum = new AtomicInteger(1);

        public NamedThreadFactory(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, prefix + "-worker-" + threadNum.getAndIncrement());
            t.setDaemon(false);
            t.setPriority(Thread.NORM_PRIORITY);
            return t;
        }
    }

    /**
     * 生产级带告警监控的 CallerRuns 拒绝策略
     */
    public static class LoggingCallerRunsPolicy implements RejectedExecutionHandler {
        private final String poolName;

        public LoggingCallerRunsPolicy(String poolName) {
            this.poolName = poolName;
        }

        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            if (executor instanceof MonitoredThreadPoolExecutor) {
                ((MonitoredThreadPoolExecutor) executor).incrementRejectedCount();
            }
            LOG.error("[{}] 触发线程池饱和拒绝策略！队列打满 (积压: {}), 由调用线程 ({}) 同步降级执行！",
                    poolName, executor.getQueue().size(), Thread.currentThread().getName());

            if (!executor.isShutdown()) {
                r.run();
            }
        }
    }
}
