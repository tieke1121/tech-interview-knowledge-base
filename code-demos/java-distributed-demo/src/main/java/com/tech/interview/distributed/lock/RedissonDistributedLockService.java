package com.tech.interview.distributed.lock;

import org.redisson.api.RLock;
import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 * 生产级分布式锁与原子扣减服务 (Redisson + Lua)
 *
 * 核心技术机制与面试深水区：
 * 1. Redisson 看门狗 (Watchdog) 自动续期原理：
 *    - 必须使用 lock.tryLock(waitTimeout, unit) 而不能传入 leaseTime，否则看门狗续期自动失效！
 *    - 默认 leaseTime=30s，后台定时任务每 1/3 周期 (10s) 自动延长锁过期时间；
 * 2. 避免主从切换锁丢失：分析 Redlock 算法争议与基于业务幂等/版本号最终一致兜底；
 * 3. 高性能秒杀场景下的 Redis Lua 脚本原子库存扣减。
 */
public class RedissonDistributedLockService {

    private static final Logger LOG = LoggerFactory.getLogger(RedissonDistributedLockService.class);

    private final RedissonClient redissonClient;

    public RedissonDistributedLockService(RedissonClient redissonClient) {
        this.redissonClient = redissonClient;
    }

    /**
     * 安全获取分布式锁并执行业务逻辑 (受看门狗保护)
     */
    public boolean executeWithWatchdogLock(String lockKey, long waitTimeSeconds, Runnable businessTask) {
        RLock lock = redissonClient.getLock(lockKey);
        boolean locked = false;

        try {
            // 注意：绝不能传入 leaseTime！传入 leaseTime 将绕过 Watchdog，业务执行超长时锁提前释放！
            locked = lock.tryLock(waitTimeSeconds, TimeUnit.SECONDS);
            if (!locked) {
                LOG.warn("获取分布式锁失败: lockKey={}, 放弃执行或触发降级", lockKey);
                return false;
            }

            LOG.info("成功获取分布式锁: lockKey={}, 由 Redisson Watchdog 自动续期", lockKey);
            businessTask.run();
            return true;

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("获取分布式锁被中断: lockKey={}", lockKey, e);
            return false;
        } finally {
            // 严格防御：仅由持有锁的线程释放自己的锁，防止误释放他人已获取的锁
            if (locked && lock.isHeldByCurrentThread()) {
                lock.unlock();
                LOG.info("分布式锁释放成功: lockKey={}", lockKey);
            }
        }
    }

    /**
     * 高并发秒杀：使用 Lua 脚本实现原子库存校验与扣减 (避免分布式锁网络开销)
     * Lua 脚本在 Redis 单线程引擎中以原子方式执行，消灭 Check-Then-Act 竞态条件
     */
    public Long deductStockWithLua(String stockKey, int deductAmount) {
        String luaScript =
                "local current = redis.call('get', KEYS[1]) " +
                "if not current then " +
                "    return -1 " + // 库存 Key 不存在
                "end " +
                "local stock = tonumber(current) " +
                "if stock >= tonumber(ARGV[1]) then " +
                "    redis.call('decrby', KEYS[1], ARGV[1]) " +
                "    return stock - tonumber(ARGV[1]) " + // 返回扣减后的剩余库存
                "else " +
                "    return -2 " + // 库存不足超卖防御
                "end";

        return redissonClient.getScript().eval(
                RScript.Mode.READ_WRITE,
                luaScript,
                RScript.ReturnType.INTEGER,
                Collections.singletonList(stockKey),
                deductAmount
        );
    }
}
