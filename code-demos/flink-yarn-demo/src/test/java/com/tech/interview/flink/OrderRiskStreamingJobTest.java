package com.tech.interview.flink;

import com.tech.interview.flink.function.OrderAggFunction;
import com.tech.interview.flink.function.OrderWindowProcessFunction;
import com.tech.interview.flink.model.OrderEvent;
import com.tech.interview.flink.model.RiskResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * 实时风控核心增量聚合与窗口处理算子单元测试
 */
class OrderRiskStreamingJobTest {

    @Test
    @DisplayName("验证 OrderAggFunction 增量累加与合并逻辑的正确性")
    void testOrderAggFunction() {
        OrderAggFunction aggFunction = new OrderAggFunction();
        OrderAggFunction.OrderAccumulator acc = aggFunction.createAccumulator();

        assertNotNull(acc);
        assertEquals(0.0, acc.getTotalAmount());
        assertEquals(0L, acc.getCount());

        long now = System.currentTimeMillis();
        acc = aggFunction.add(new OrderEvent("ord_1", "user_100", 150.50, now), acc);
        acc = aggFunction.add(new OrderEvent("ord_2", "user_100", 249.50, now + 1000), acc);

        assertEquals(400.00, acc.getTotalAmount(), 0.0001);
        assertEquals(2L, acc.getCount());

        // 验证 merge 逻辑 (在跨分区状态聚合或 SessionWindow 中触发)
        OrderAggFunction.OrderAccumulator acc2 = aggFunction.createAccumulator();
        acc2 = aggFunction.add(new OrderEvent("ord_3", "user_100", 100.00, now + 2000), acc2);

        OrderAggFunction.OrderAccumulator merged = aggFunction.merge(acc, acc2);
        assertEquals(500.00, merged.getTotalAmount(), 0.0001);
        assertEquals(3L, merged.getCount());
    }

    @Test
    @DisplayName("验证 OrderWindowProcessFunction 补充窗口元数据上下文的能力")
    void testOrderWindowProcessFunction() {
        OrderWindowProcessFunction processFunction = new OrderWindowProcessFunction();

        OrderAggFunction.OrderAccumulator acc = new OrderAggFunction.OrderAccumulator(888.88, 5L);
        long windowStart = 1700000000000L;
        long windowEnd = windowStart + 60000L;

        TimeWindow mockWindow = new TimeWindow(windowStart, windowEnd);
        List<RiskResult> collected = new ArrayList<>();

        // 简易测试 Collector
        Collector<RiskResult> testCollector = new Collector<RiskResult>() {
            @Override
            public void collect(RiskResult record) {
                collected.add(record);
            }

            @Override
            public void close() {}
        };

        // 构造简易 Mock Context (基于 processFunction 封闭实例)
        OrderWindowProcessFunction.Context mockContext = processFunction.new Context() {
            @Override
            public TimeWindow window() {
                return mockWindow;
            }

            @Override
            public long currentProcessingTime() {
                return windowEnd;
            }

            @Override
            public long currentWatermark() {
                return windowEnd;
            }

            @Override
            public org.apache.flink.api.common.state.KeyedStateStore windowState() {
                return null;
            }

            @Override
            public org.apache.flink.api.common.state.KeyedStateStore globalState() {
                return null;
            }

            @Override
            public <X> void output(org.apache.flink.util.OutputTag<X> outputTag, X value) {}
        };

        processFunction.process("user_888", mockContext, Collections.singletonList(acc), testCollector);

        assertEquals(1, collected.size());
        RiskResult result = collected.get(0);
        assertEquals("user_888", result.getUserId());
        assertEquals(windowStart, result.getWindowStart());
        assertEquals(windowEnd, result.getWindowEnd());
        assertEquals(888.88, result.getTotalAmount(), 0.0001);
        assertEquals(5L, result.getOrderCount());
    }
}
