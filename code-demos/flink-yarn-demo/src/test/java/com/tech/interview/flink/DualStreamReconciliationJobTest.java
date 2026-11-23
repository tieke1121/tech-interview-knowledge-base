package com.tech.interview.flink;

import com.tech.interview.flink.function.OrderPaymentCoProcessFunction;
import com.tech.interview.flink.model.OrderEvent;
import com.tech.interview.flink.model.PaymentEvent;
import com.tech.interview.flink.model.ReconcileResult;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 双流连接、Watermark 协同与事件时间定时器对账单元测试
 */
class DualStreamReconciliationJobTest {

    @Test
    @DisplayName("验证 ReconcileResult 实体模型与字段计算")
    void testReconcileResultModel() {
        long matchTime = System.currentTimeMillis();
        ReconcileResult result = new ReconcileResult(
                "ord_1001",
                "user_88",
                299.0,
                299.0,
                "SUCCESS_MATCHED",
                matchTime,
                1500L
        );

        assertEquals("ord_1001", result.getOrderId());
        assertEquals("user_88", result.getUserId());
        assertEquals(299.0, result.getOrderAmount());
        assertEquals(299.0, result.getPayAmount());
        assertEquals("SUCCESS_MATCHED", result.getStatus());
        assertEquals(1500L, result.getDurationMs());
        assertTrue(result.toString().contains("SUCCESS_MATCHED"));
    }

    @Test
    @DisplayName("验证 PaymentEvent 实体模型与序列化支持")
    void testPaymentEventModel() {
        long now = System.currentTimeMillis();
        PaymentEvent payment = new PaymentEvent("pay_999", "ord_1001", 299.0, "ALIPAY", now);

        assertEquals("pay_999", payment.getPaymentId());
        assertEquals("ord_1001", payment.getOrderId());
        assertEquals("ALIPAY", payment.getPayChannel());
        assertEquals(now, payment.getPayTime());
    }

    @Test
    @DisplayName("验证 OrderPaymentCoProcessFunction 侧道输出 OutputTag 定义规范")
    void testOutputTags() {
        assertNotNull(OrderPaymentCoProcessFunction.TIMEOUT_ORDER_TAG);
        assertNotNull(OrderPaymentCoProcessFunction.ORPHAN_PAYMENT_TAG);
        assertEquals("timeout-unpaid-orders", OrderPaymentCoProcessFunction.TIMEOUT_ORDER_TAG.getId());
        assertEquals("orphan-payments", OrderPaymentCoProcessFunction.ORPHAN_PAYMENT_TAG.getId());
    }
}
