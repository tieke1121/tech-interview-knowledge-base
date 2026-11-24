package com.tech.interview.kafka;

import com.tech.interview.kafka.listener.SafeRebalanceListener;
import com.tech.interview.kafka.model.TransactionRecord;
import com.tech.interview.kafka.service.TransactionalPipelineService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Kafka 事务管道配置与业务逻辑单元测试
 */
class TransactionalPipelineTest {

    @Test
    @DisplayName("验证 Producer 核心事务与幂等性参数配置合规性")
    void testProducerProperties() {
        TransactionalPipelineService service = new TransactionalPipelineService(
                "localhost:9092",
                "in-topic",
                "out-topic",
                "test-group",
                "test-tx-id"
        );

        Properties props = service.buildProducerProperties();
        assertEquals("test-tx-id", props.getProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG));
        assertEquals("true", props.getProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
        assertEquals("all", props.getProperty(ProducerConfig.ACKS_CONFIG));
        assertEquals("5", String.valueOf(props.get(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION)));
    }

    @Test
    @DisplayName("验证 Consumer read_committed 与非自动提交配置")
    void testConsumerProperties() {
        TransactionalPipelineService service = new TransactionalPipelineService(
                "localhost:9092",
                "in-topic",
                "out-topic",
                "test-group",
                "test-tx-id"
        );

        Properties props = service.buildConsumerProperties();
        assertEquals("read_committed", props.getProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG));
        assertEquals("false", props.getProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
    }

    @Test
    @DisplayName("验证 SafeRebalanceListener 分区分配与撤销状态流转")
    void testRebalanceListener() {
        SafeRebalanceListener listener = new SafeRebalanceListener();
        TopicPartition p0 = new TopicPartition("orders", 0);
        TopicPartition p1 = new TopicPartition("orders", 1);

        listener.onPartitionsAssigned(Collections.singletonList(p0));
        assertTrue(listener.getCurrentAssignedPartitions().contains(p0));
        assertFalse(listener.isRebalanceOccurred());

        // 模拟触发撤销
        listener.onPartitionsRevoked(Collections.singletonList(p0));
        assertTrue(listener.isRebalanceOccurred());
        assertFalse(listener.getCurrentAssignedPartitions().contains(p0));
    }

    @Test
    @DisplayName("验证业务数据纯函数清洗转换逻辑")
    void testTransform() {
        TransactionalPipelineService service = new TransactionalPipelineService(
                "localhost:9092", "in", "out", "grp", "tx"
        );

        TransactionRecord record = service.transform("raw_json_payload");
        assertNotNull(record);
        assertNotNull(record.getTxId());
        assertEquals("CLEANED_SUCCESS", record.getStatus());
        assertEquals(1000.0, record.getAmount(), 0.0001);
    }
}
