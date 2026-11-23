package com.tech.interview.flink.model;

import java.io.Serializable;
import java.util.Objects;

/**
 * 订单交易事件明细模型 (POJO)
 * 严格遵循 Flink POJO 规范：具备公共无参构造、Getter/Setter、实现序列化
 */
public class OrderEvent implements Serializable {
    private static final long serialVersionUID = 1L;

    private String orderId;
    private String userId;
    private double amount;
    private long eventTime;

    public OrderEvent() {
    }

    public OrderEvent(String orderId, String userId, double amount, long eventTime) {
        this.orderId = orderId;
        this.userId = userId;
        this.amount = amount;
        this.eventTime = eventTime;
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

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    public long getEventTime() {
        return eventTime;
    }

    public void setEventTime(long eventTime) {
        this.eventTime = eventTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OrderEvent that = (OrderEvent) o;
        return Double.compare(that.amount, amount) == 0 &&
                eventTime == that.eventTime &&
                Objects.equals(orderId, that.orderId) &&
                Objects.equals(userId, that.userId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(orderId, userId, amount, eventTime);
    }

    @Override
    public String toString() {
        return "OrderEvent{" +
                "orderId='" + orderId + '\'' +
                ", userId='" + userId + '\'' +
                ", amount=" + amount +
                ", eventTime=" + eventTime +
                '}';
    }
}
