package com.tech.interview.flink;

import com.tech.interview.flink.sink.ExactlyOnceSinkV2Job;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

@DisplayName("SinkV2 两阶段提交事务凭据序列化器单元测试")
public class SinkPayloadSerializerTest {

    @Test
    @DisplayName("测试事务载荷在快照与故障恢复时的序列化与反序列化一致性")
    void testSerializationRoundTrip() throws IOException {
        ExactlyOnceSinkV2Job.TransactionalPayloadSerializer serializer =
                new ExactlyOnceSinkV2Job.TransactionalPayloadSerializer();

        ExactlyOnceSinkV2Job.TransactionalPayload original =
                new ExactlyOnceSinkV2Job.TransactionalPayload("TXN-TEST-1001", 42, 12890.55);

        byte[] bytes = serializer.serialize(original);
        ExactlyOnceSinkV2Job.TransactionalPayload deserialized =
                serializer.deserialize(serializer.getVersion(), bytes);

        assertEquals(original.getTxnId(), deserialized.getTxnId());
        assertEquals(original.getEventCount(), deserialized.getEventCount());
        assertEquals(original.getTotalAmount(), deserialized.getTotalAmount(), 0.0001);
    }
}
