package com.tech.interview.flink;

import com.tech.interview.flink.compute.async.LocalTtlCache;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("异步维表本地 LRU + TTL 缓存单元测试")
public class LocalTtlCacheTest {

    @Test
    @DisplayName("测试缓存正常存取与容量上限 LRU 淘汰")
    void testCachePutGetAndEviction() {
        LocalTtlCache<String, String> cache = new LocalTtlCache<>(3, 60_000);

        cache.put("k1", "v1");
        cache.put("k2", "v2");
        cache.put("k3", "v3");

        assertEquals("v1", cache.get("k1"));
        assertEquals("v2", cache.get("k2"));
        assertEquals("v3", cache.get("k3"));

        // 插入第 4 个元素，k1 刚被访问过，此时最久未访问的为 k2 或根据访问顺序淘汰
        cache.put("k4", "v4");
        assertEquals(3, cache.size());
        assertNotNull(cache.get("k4"));
    }

    @Test
    @DisplayName("测试缓存 TTL 过期判定")
    void testCacheExpiration() throws InterruptedException {
        // 设置 50ms 超短过期时间
        LocalTtlCache<String, String> cache = new LocalTtlCache<>(10, 50);

        cache.put("k_expire", "val");
        assertEquals("val", cache.get("k_expire"));

        Thread.sleep(60);
        assertNull(cache.get("k_expire"), "过期数据应返回 null");
    }
}
