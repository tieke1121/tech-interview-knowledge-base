package com.tech.interview.flink.compute.broadcast;

import java.io.Serial;
import java.io.Serializable;

/**
 * 动态风控规则模型 (POJO)
 * 支持通过广播流在不停机的情况下动态增删改风控规则
 */
public class RuleModel implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    public enum Action {
        ADD, UPDATE, DELETE
    }

    private String ruleId;
    private String category;
    private double maxAmountThreshold;
    private Action action;

    public RuleModel() {
    }

    public RuleModel(String ruleId, String category, double maxAmountThreshold, Action action) {
        this.ruleId = ruleId;
        this.category = category;
        this.maxAmountThreshold = maxAmountThreshold;
        this.action = action;
    }

    public String getRuleId() {
        return ruleId;
    }

    public void setRuleId(String ruleId) {
        this.ruleId = ruleId;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public double getMaxAmountThreshold() {
        return maxAmountThreshold;
    }

    public void setMaxAmountThreshold(double maxAmountThreshold) {
        this.maxAmountThreshold = maxAmountThreshold;
    }

    public Action getAction() {
        return action;
    }

    public void setAction(Action action) {
        this.action = action;
    }

    @Override
    public String toString() {
        return "RuleModel{" +
                "ruleId='" + ruleId + '\'' +
                ", category='" + category + '\'' +
                ", maxAmountThreshold=" + maxAmountThreshold +
                ", action=" + action +
                '}';
    }
}
