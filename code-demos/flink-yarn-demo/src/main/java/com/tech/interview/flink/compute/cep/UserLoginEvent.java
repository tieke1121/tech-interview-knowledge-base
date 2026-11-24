package com.tech.interview.flink.compute.cep;

import java.io.Serial;
import java.io.Serializable;

/**
 * 用户安全登录与操作事件模型 (POJO)
 */
public class UserLoginEvent implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    private String userId;
    private String ip;
    private String eventType; // "LOGIN_FAIL", "LOGIN_SUCCESS", "TRANSFER_LARGE_AMOUNT", "PASSWORD_RESET"
    private long eventTime;

    public UserLoginEvent() {
    }

    public UserLoginEvent(String userId, String ip, String eventType, long eventTime) {
        this.userId = userId;
        this.ip = ip;
        this.eventType = eventType;
        this.eventTime = eventTime;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public long getEventTime() {
        return eventTime;
    }

    public void setEventTime(long eventTime) {
        this.eventTime = eventTime;
    }

    @Override
    public String toString() {
        return "UserLoginEvent{" +
                "userId='" + userId + '\'' +
                ", ip='" + ip + '\'' +
                ", eventType='" + eventType + '\'' +
                ", eventTime=" + eventTime +
                '}';
    }
}
