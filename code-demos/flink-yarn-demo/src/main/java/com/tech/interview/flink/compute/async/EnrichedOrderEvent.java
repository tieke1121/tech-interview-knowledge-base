package com.tech.interview.flink.compute.async;

import java.io.Serial;
import java.io.Serializable;

/**
 * 经外部维表异步补全后的订单事件模型 (POJO)
 */
public class EnrichedOrderEvent implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    private String orderId;
    private String userId;
    private double amount;
    private String userCity;
    private String userTier;
    private long latencyMs;

    public EnrichedOrderEvent() {
    }

    public EnrichedOrderEvent(String orderId, String userId, double amount, String userCity, String userTier, long latencyMs) {
        this.orderId = orderId;
        this.userId = userId;
        this.amount = amount;
        this.userCity = userCity;
        this.userTier = userTier;
        this.latencyMs = latencyMs;
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

    public String getUserCity() {
        return userCity;
    }

    public void setUserCity(String userCity) {
        this.userCity = userCity;
    }

    public String getUserTier() {
        return userTier;
    }

    public void setUserTier(String userTier) {
        this.userTier = userTier;
    }

    public long getLatencyMs() {
        return latencyMs;
    }

    public void setLatencyMs(long latencyMs) {
        this.latencyMs = latencyMs;
    }

    @Override
    public String toString() {
        return "EnrichedOrderEvent{" +
                "orderId='" + orderId + '\'' +
                ", userId='" + userId + '\'' +
                ", amount=" + amount +
                ", userCity='" + userCity + '\'' +
                ", userTier='" + userTier + '\'' +
                ", latencyMs=" + latencyMs +
                '}';
    }
}
