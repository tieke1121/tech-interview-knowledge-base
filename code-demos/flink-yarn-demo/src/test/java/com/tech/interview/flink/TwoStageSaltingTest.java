package com.tech.interview.flink;

import com.tech.interview.flink.compute.skew.TwoStageSaltingJob;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@DisplayName("两阶段加盐防倾斜聚合算子单元测试")
public class TwoStageSaltingTest {

    @Test
    @DisplayName("乱序订单经两次分组后仍归入原店铺的事件时间窗口")
    void keepsAdjacentEventTimeWindowsSeparate() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        var orders = env.fromData(
                order("SHOP_A", 1, 2999),
                order("SHOP_A", 2, 3000),
                order("SHOP_A", 4, 2000),
                order("SHOP_B", 8, 2999))
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<TwoStageSaltingJob.SkewedOrder>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((event, previousTimestamp) -> event.getEventTime()));

        try (var results = TwoStageSaltingJob.aggregateByShop(orders).executeAndCollect()) {
            var actual = new ArrayList<String>();
            results.forEachRemaining(actual::add);
            assertEquals(List.of(
                    "🌟 [Global Balanced Agg] ShopId=SHOP_A FinalTotal=2.00 WindowEnd=6000",
                    "🌟 [Global Balanced Agg] ShopId=SHOP_A FinalTotal=5.00 WindowEnd=3000",
                    "🌟 [Global Balanced Agg] ShopId=SHOP_B FinalTotal=8.00 WindowEnd=3000"
            ), actual.stream().sorted().toList());
        }
    }

    private TwoStageSaltingJob.SkewedOrder order(String shop, double amount, long timestamp) {
        var order = new TwoStageSaltingJob.SkewedOrder(shop, amount);
        order.setEventTime(timestamp);
        return order;
    }

    @Test
    @DisplayName("测试局部加盐累加与全局去盐汇总增量正确性")
    void testLocalAndGlobalAggregation() {
        // 局部累加验证
        TwoStageSaltingJob.LocalSumAggFunction localAgg = new TwoStageSaltingJob.LocalSumAggFunction();
        Double acc1 = localAgg.createAccumulator();
        acc1 = localAgg.add(new TwoStageSaltingJob.SaltedOrder("SHOP_1_0", 100.0), acc1);
        acc1 = localAgg.add(new TwoStageSaltingJob.SaltedOrder("SHOP_1_0", 150.0), acc1);
        assertEquals(250.0, localAgg.getResult(acc1), 0.001);

        // 全局去盐累加验证
        TwoStageSaltingJob.GlobalSumAggFunction globalAgg = new TwoStageSaltingJob.GlobalSumAggFunction();
        Double acc2 = globalAgg.createAccumulator();
        acc2 = globalAgg.add(new TwoStageSaltingJob.SkewedOrder("SHOP_1", 250.0), acc2);
        acc2 = globalAgg.add(new TwoStageSaltingJob.SkewedOrder("SHOP_1", 350.0), acc2);
        assertEquals(600.0, globalAgg.getResult(acc2), 0.001);
    }
}
