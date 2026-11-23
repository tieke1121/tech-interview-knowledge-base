package com.tech.interview.flink;

import com.tech.interview.flink.model.OrderEvent;
import com.tech.interview.flink.source.CustomPunctuatedWatermarkJob;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("自定义标点符号式 Watermark 生成器单元测试")
public class PunctuatedWatermarkTest {

    @Test
    @DisplayName("测试普通订单不触发立即下发，VIP 订单立即触发标点 Watermark")
    void testPunctuatedWatermarkEmission() {
        CustomPunctuatedWatermarkJob.PunctuatedOrderWatermarkGenerator generator =
                new CustomPunctuatedWatermarkJob.PunctuatedOrderWatermarkGenerator();

        List<Watermark> emittedWatermarks = new ArrayList<>();
        WatermarkOutput mockOutput = new WatermarkOutput() {
            @Override
            public void emitWatermark(Watermark watermark) {
                emittedWatermarks.add(watermark);
            }

            @Override
            public void markIdle() {}

            @Override
            public void markActive() {}
        };

        long baseTime = 10_000L;

        // 1. 发送普通用户订单
        OrderEvent regularOrder = new OrderEvent("ORD-1", "USER-100", 50.0, baseTime);
        generator.onEvent(regularOrder, baseTime, mockOutput);
        assertTrue(emittedWatermarks.isEmpty(), "普通订单在 onEvent 中不应立即下发 Watermark");

        // 2. 发送 VIP 订单
        long vipTime = 15_000L;
        OrderEvent vipOrder = new OrderEvent("ORD-2", "VIP-999", 500.0, vipTime);
        generator.onEvent(vipOrder, vipTime, mockOutput);
        assertEquals(1, emittedWatermarks.size(), "VIP 订单在 onEvent 中必须立即发射标点 Watermark");
        assertEquals(vipTime - 3000L, emittedWatermarks.get(0).getTimestamp(), "水位线应准确扣减 3 秒乱序容忍");

        // 3. 周期性发射
        generator.onPeriodicEmit(mockOutput);
        assertEquals(2, emittedWatermarks.size(), "周期性检查应发射当前最新的 Watermark");
        assertEquals(vipTime - 3000L, emittedWatermarks.get(1).getTimestamp());
    }
}
