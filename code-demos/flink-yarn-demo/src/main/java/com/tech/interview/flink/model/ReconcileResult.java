package com.tech.interview.flink.model;

import java.io.Serializable;
import java.util.Objects;

/**
 * 订单与支付双流对账结果模型 (POJO)
 */
public class ReconcileResult implements Serializable {
    private static final long serialVersionUID = 1L;

    private String orderId;
    private String userId;
    private double orderAmount;
    private double payAmount;
    private String status;
    private long matchTime;
    private long durationMs;

    public ReconcileResult() {
    }

    public ReconcileResult(String orderId, String userId, double orderAmount, double payAmount, String status, long matchTime, long durationMs) {
        this.orderId = orderId;
        this.userId = userId;
        this.orderAmount = orderAmount;
        this.payAmount = payAmount;
        this.status = status;
        this.matchTime = matchTime;
        this.durationMs = durationMs;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public double getOrderAmount() {
        return orderAmount;
    }

    public void setOrderAmount(double orderAmount) {
        this.orderAmount = orderAmount;
    }

    public double getPayAmount() {
        return payAmount;
    }

    public void setPayAmount(double payAmount) {
        this.payAmount = payAmount;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public long getMatchTime() {
        return matchTime;
    }

    public void setMatchTime(long matchTime) {
        this.matchTime = matchTime;
    }

    public long getDurationMs() {
        return durationMs;
    }

    public void setDurationMs(long durationMs) {
        this.durationMs = durationMs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReconcileResult that = (ReconcileResult) o;
        return Double.compare(that.orderAmount, orderAmount) == 0 &&
                Double.compare(that.payAmount, payAmount) == 0 &&
                matchTime == that.matchTime &&
                durationMs == that.durationMs &&
                Objects.equals(orderId, that.orderId) &&
                Objects.equals(userId, that.userId) &&
                Objects.equals(status, that.status);
    }

    @Override
    public int hashCode() {
        return Objects.hash(orderId, userId, orderAmount, payAmount, status, matchTime, durationMs);
    }

    @Override
    public String toString() {
        return "ReconcileResult{" +
                "orderId='" + orderId + '\'' +
                ", userId='" + userId + '\'' +
                ", orderAmount=" + orderAmount +
                ", payAmount=" + payAmount +
                ", status='" + status + '\'' +
                ", matchTime=" + matchTime +
                ", duration=" + durationMs + "ms" +
                '}';
    }
}
