package com.tech.interview.kafka.model;

import java.io.Serializable;
import java.util.Objects;

/**
 * 金融交易明细领域模型
 */
public class TransactionRecord implements Serializable {
    private static final long serialVersionUID = 1L;

    private String txId;
    private String fromAccount;
    private String toAccount;
    private double amount;
    private long timestamp;
    private String status;

    public TransactionRecord() {
    }

    public TransactionRecord(String txId, String fromAccount, String toAccount, double amount, long timestamp, String status) {
        this.txId = txId;
        this.fromAccount = fromAccount;
        this.toAccount = toAccount;
        this.amount = amount;
        this.timestamp = timestamp;
        this.status = status;
    }

    public String getTxId() {
        return txId;
    }

    public void setTxId(String txId) {
        this.txId = txId;
    }

    public String getFromAccount() {
        return fromAccount;
    }

    public void setFromAccount(String fromAccount) {
        this.fromAccount = fromAccount;
    }

    public String getToAccount() {
        return toAccount;
    }

    public void setToAccount(String toAccount) {
        this.toAccount = toAccount;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TransactionRecord that = (TransactionRecord) o;
        return Double.compare(that.amount, amount) == 0 &&
                timestamp == that.timestamp &&
                Objects.equals(txId, that.txId) &&
                Objects.equals(fromAccount, that.fromAccount) &&
                Objects.equals(toAccount, that.toAccount) &&
                Objects.equals(status, that.status);
    }

    @Override
    public int hashCode() {
        return Objects.hash(txId, fromAccount, toAccount, amount, timestamp, status);
    }

    @Override
    public String toString() {
        return "TransactionRecord{" +
                "txId='" + txId + '\'' +
                ", from='" + fromAccount + '\'' +
                ", to='" + toAccount + '\'' +
                ", amount=" + amount +
                ", timestamp=" + timestamp +
                ", status='" + status + '\'' +
                '}';
    }
}
