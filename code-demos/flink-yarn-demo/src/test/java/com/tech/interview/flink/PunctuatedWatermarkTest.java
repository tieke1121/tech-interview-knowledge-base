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
        WatermarkOutput mockOutput = captureInto(emittedWatermarks);

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
        assertEquals(vipTime - 3000L - 1, emittedWatermarks.get(0).getTimestamp(), "恰好落在 3 秒乱序边界上的记录仍有机会到达");

        // 3. 周期性发射
        generator.onPeriodicEmit(mockOutput);
        assertEquals(2, emittedWatermarks.size(), "周期性检查应发射当前最新的 Watermark");
        assertEquals(vipTime - 3000L - 1, emittedWatermarks.get(1).getTimestamp());
    }

    @Test
    @DisplayName("空分区第一次周期回调不会溢出，迟到的 VIP 不会让水位线倒退")
    void idleStartAndOutOfOrderVip() {
        var generator = new CustomPunctuatedWatermarkJob.PunctuatedOrderWatermarkGenerator();
        List<Watermark> emitted = new ArrayList<>();
        WatermarkOutput output = captureInto(emitted);

        generator.onPeriodicEmit(output);
        assertEquals(Long.MIN_VALUE, emitted.get(0).getTimestamp());

        generator.onEvent(new OrderEvent("A", "USER-1", 1, 20_000L), 20_000L, output);
        generator.onPeriodicEmit(output);
        generator.onEvent(new OrderEvent("B", "VIP-1", 1, 18_000L), 18_000L, output);
        assertEquals(List.of(Long.MIN_VALUE, 16_999L, 16_999L),
                emitted.stream().map(Watermark::getTimestamp).toList());
    }

    private WatermarkOutput captureInto(List<Watermark> emittedWatermarks) {
        return new WatermarkOutput() {
            @Override
            public void emitWatermark(Watermark watermark) {
                emittedWatermarks.add(watermark);
            }

            @Override
            public void markIdle() {}

            @Override
            public void markActive() {}
        };
    }
}
