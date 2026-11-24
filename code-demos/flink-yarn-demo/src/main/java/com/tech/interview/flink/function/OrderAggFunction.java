package com.tech.interview.flink.function;

import com.tech.interview.flink.model.OrderEvent;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.io.Serial;
import java.io.Serializable;

/**
 * 高性能增量聚合函数 (AggregateFunction)
 * 核心设计优势：每个流事件到达时实时触发累加，内存中仅保留一个轻量级累加器对象，
 * 避免了传统全窗口缓存（如直接使用 ProcessWindowFunction）在大流量场景下打满 JVM 堆内存的隐患。
 */
public class OrderAggFunction implements AggregateFunction<OrderEvent, OrderAggFunction.OrderAccumulator, OrderAggFunction.OrderAccumulator> {

    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * 增量聚合累加器 (POJO)
     */
    public static class OrderAccumulator implements Serializable {
        @Serial
        private static final long serialVersionUID = 1L;

        private double totalAmount = 0.0;
        private long count = 0L;

        public OrderAccumulator() {
        }

        public OrderAccumulator(double totalAmount, long count) {
            this.totalAmount = totalAmount;
            this.count = count;
        }

        public double getTotalAmount() {
            return totalAmount;
        }

        public void setTotalAmount(double totalAmount) {
            this.totalAmount = totalAmount;
        }

        public long getCount() {
            return count;
        }

        public void setCount(long count) {
            this.count = count;
        }

        public void add(double amount) {
            this.totalAmount += amount;
            this.count += 1;
        }

        public void merge(OrderAccumulator other) {
            if (other != null) {
                this.totalAmount += other.totalAmount;
                this.count += other.count;
            }
        }
    }

    @Override
    public OrderAccumulator createAccumulator() {
        return new OrderAccumulator();
    }

    @Override
    public OrderAccumulator add(OrderEvent value, OrderAccumulator accumulator) {
        if (value != null) {
            accumulator.add(value.getAmount());
        }
        return accumulator;
    }

    @Override
    public OrderAccumulator getResult(OrderAccumulator accumulator) {
        return accumulator;
    }

    @Override
    public OrderAccumulator merge(OrderAccumulator a, OrderAccumulator b) {
        a.merge(b);
        return a;
    }
}
