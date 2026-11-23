package com.tech.interview.flink.model;

import java.io.Serializable;
import java.util.Objects;

/**
 * 实时窗口风控聚合输出指标模型 (POJO)
 */
public class RiskResult implements Serializable {
    private static final long serialVersionUID = 1L;

    private String userId;
    private long windowStart;
    private long windowEnd;
    private double totalAmount;
    private long orderCount;

    public RiskResult() {
    }

    public RiskResult(String userId, long windowStart, long windowEnd, double totalAmount, long orderCount) {
        this.userId = userId;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.totalAmount = totalAmount;
        this.orderCount = orderCount;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public long getWindowStart() {
        return windowStart;
    }

    public void setWindowStart(long windowStart) {
        this.windowStart = windowStart;
    }

    public long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(long windowEnd) {
        this.windowEnd = windowEnd;
    }

    public double getTotalAmount() {
        return totalAmount;
    }

    public void setTotalAmount(double totalAmount) {
        this.totalAmount = totalAmount;
    }

    public long getOrderCount() {
        return orderCount;
    }

    public void setOrderCount(long orderCount) {
        this.orderCount = orderCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RiskResult that = (RiskResult) o;
        return windowStart == that.windowStart &&
                windowEnd == that.windowEnd &&
                Double.compare(that.totalAmount, totalAmount) == 0 &&
                orderCount == that.orderCount &&
                Objects.equals(userId, that.userId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(userId, windowStart, windowEnd, totalAmount, orderCount);
    }

    @Override
    public String toString() {
        return "RiskResult{" +
                "userId='" + userId + '\'' +
                ", window=[" + windowStart + ", " + windowEnd + ")" +
                ", totalAmount=" + String.format("%.2f", totalAmount) +
                ", orderCount=" + orderCount +
                '}';
    }
}
