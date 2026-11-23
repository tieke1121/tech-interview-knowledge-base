package com.tech.interview.flink;

import com.tech.interview.flink.compute.skew.TwoStageSaltingJob;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

@DisplayName("两阶段加盐防倾斜聚合算子单元测试")
public class TwoStageSaltingTest {

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
