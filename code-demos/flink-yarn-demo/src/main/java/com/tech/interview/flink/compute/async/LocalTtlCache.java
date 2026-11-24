package com.tech.interview.flink.compute.async;

import java.io.Serial;
import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 生产级轻量高并发堆内 LRU + TTL 本地缓存
 * 架构用途：集成在 Flink 算子中，避免高频请求击穿外部 Redis/HBase/MySQL 维表存储。
 */
public class LocalTtlCache<K, V> implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    private static class CacheEntry<V> {
        final V value;
        final long expireAt;

        CacheEntry(V value, long ttlMillis) {
            this.value = value;
            this.expireAt = System.currentTimeMillis() + ttlMillis;
        }

        boolean isExpired() {
            return System.currentTimeMillis() > expireAt;
        }
    }

    private final int maxCapacity;
    private final long ttlMillis;
    private final Map<K, CacheEntry<V>> lruMap;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public LocalTtlCache(int maxCapacity, long ttlMillis) {
        this.maxCapacity = maxCapacity;
        this.ttlMillis = ttlMillis;
        this.lruMap = new LinkedHashMap<>(maxCapacity, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<K, CacheEntry<V>> eldest) {
                return size() > maxCapacity;
            }
        };
    }

    public V get(K key) {
        lock.readLock().lock();
        try {
            CacheEntry<V> entry = lruMap.get(key);
            if (entry == null || entry.isExpired()) {
                return null;
            }
            return entry.value;
        } finally {
            lock.readLock().unlock();
        }
    }

    public void put(K key, V value) {
        lock.writeLock().lock();
        try {
            lruMap.put(key, new CacheEntry<>(value, ttlMillis));
        } finally {
            lock.writeLock().unlock();
        }
    }

    public int size() {
        lock.readLock().lock();
        try {
            return lruMap.size();
        } finally {
            lock.readLock().unlock();
        }
    }
}
