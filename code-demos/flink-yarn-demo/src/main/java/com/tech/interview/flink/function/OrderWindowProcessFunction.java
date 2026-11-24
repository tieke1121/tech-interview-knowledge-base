package com.tech.interview.flink.function;

import com.tech.interview.flink.model.RiskResult;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.Serial;

/**
 * 窗口处理包装函数 (ProcessWindowFunction)
 * 职责分工：与 OrderAggFunction 结合使用。OrderAggFunction 负责高效增量聚合，
 * 本函数仅在窗口闭合触发时被调用一次，用于补充窗口元数据上下文（Window Start、Window End）。
 */
public class OrderWindowProcessFunction 
        extends ProcessWindowFunction<OrderAggFunction.OrderAccumulator, RiskResult, String, TimeWindow> {

    @Serial
    private static final long serialVersionUID = 1L;

    @Override
    public void process(
            String userId,
            Context context,
            Iterable<OrderAggFunction.OrderAccumulator> elements,
            Collector<RiskResult> out) {

        // 由于上游是 AggregateFunction 增量聚合，迭代器中严格仅有 1 个累加器元素
        OrderAggFunction.OrderAccumulator accumulator = elements.iterator().next();

        long windowStart = context.window().getStart();
        long windowEnd = context.window().getEnd();

        RiskResult result = new RiskResult(
                userId,
                windowStart,
                windowEnd,
                accumulator.getTotalAmount(),
                accumulator.getCount()
        );

        out.collect(result);
    }
}
