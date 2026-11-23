package com.tech.interview.flink.model;

import java.io.Serializable;
import java.util.Objects;

/**
 * 支付流水事件模型 (POJO)
 * 供双流 Join / Connect 对账使用
 */
public class PaymentEvent implements Serializable {
    private static final long serialVersionUID = 1L;

    private String paymentId;
    private String orderId;
    private double payAmount;
    private String payChannel;
    private long payTime;

    public PaymentEvent() {
    }

    public PaymentEvent(String paymentId, String orderId, double payAmount, String payChannel, long payTime) {
        this.paymentId = paymentId;
        this.orderId = orderId;
        this.payAmount = payAmount;
        this.payChannel = payChannel;
        this.payTime = payTime;
    }

    public String getPaymentId() {
        return paymentId;
    }

    public void setPaymentId(String paymentId) {
        this.paymentId = paymentId;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public double getPayAmount() {
        return payAmount;
    }

    public void setPayAmount(double payAmount) {
        this.payAmount = payAmount;
    }

    public String getPayChannel() {
        return payChannel;
    }

    public void setPayChannel(String payChannel) {
        this.payChannel = payChannel;
    }

    public long getPayTime() {
        return payTime;
    }

    public void setPayTime(long payTime) {
        this.payTime = payTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PaymentEvent that = (PaymentEvent) o;
        return Double.compare(that.payAmount, payAmount) == 0 &&
                payTime == that.payTime &&
                Objects.equals(paymentId, that.paymentId) &&
                Objects.equals(orderId, that.orderId) &&
                Objects.equals(payChannel, that.payChannel);
    }

    @Override
    public int hashCode() {
        return Objects.hash(paymentId, orderId, payAmount, payChannel, payTime);
    }

    @Override
    public String toString() {
        return "PaymentEvent{" +
                "paymentId='" + paymentId + '\'' +
                ", orderId='" + orderId + '\'' +
                ", payAmount=" + payAmount +
                ", payChannel='" + payChannel + '\'' +
                ", payTime=" + payTime +
                '}';
    }
}
