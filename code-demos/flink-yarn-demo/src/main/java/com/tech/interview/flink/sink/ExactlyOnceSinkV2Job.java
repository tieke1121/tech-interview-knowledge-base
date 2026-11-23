package com.tech.interview.flink.sink;

import com.tech.interview.flink.model.OrderEvent;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.*;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.time.Duration;
import java.util.*;

/**
 * 【Sink 端核心考点：现代 SinkV2 架构与端到端 Exactly-Once 两阶段提交 (2PC)】
 * 
 * 核心架构考点：
 * 1. 现代 SinkV2 解耦架构 (FLIP-191)：
 *    - 彻底废弃旧版 TwoPhaseCommitSinkFunction；
 *    - 写入与提交解耦：Sink (拓扑入口) + SinkWriter (并发写/预提交) + Committer (事务全局提交)；
 * 2. 两阶段提交 2PC 状态机流转：
 *    - 阶段一 (Pre-commit): Checkpoint Barrier 穿过 SinkWriter 时触发 prepareCommit()，
 *      将当前在途数据生成事务快照凭据 (TransactionalPayload) 保存到 Checkpoint 状态，并滚动开启下一段新事务；
 *    - 阶段二 (Commit): JobManager 收集全算子 ACK 并确认 Checkpoint 成功后，广播通知 Committer
 *      执行 commit() 正式使外部数据可见；若有故障则触发 abort() 回滚；
 * 3. 生产超时协同死锁防范：
 *    - Kafka 事务最大超时 transaction.max.timeout.ms (默认 15m) 必须 > Flink Checkpoint 超时加上故障恢复耗时；
 *    - 下游必须配置 isolation.level=read_committed，屏蔽未提交事务脏读。
 */
public class ExactlyOnceSinkV2Job {

    private static final Logger LOG = LoggerFactory.getLogger(ExactlyOnceSinkV2Job.class);

    /**
     * 事务凭据载荷 (Committable Payload)
     */
    public static class TransactionalPayload implements Serializable {
        @Serial
        private static final long serialVersionUID = 1L;

        private final String txnId;
        private final int eventCount;
        private final double totalAmount;

        public TransactionalPayload(String txnId, int eventCount, double totalAmount) {
            this.txnId = txnId;
            this.eventCount = eventCount;
            this.totalAmount = totalAmount;
        }

        public String getTxnId() { return txnId; }
        public int getEventCount() { return eventCount; }
        public double getTotalAmount() { return totalAmount; }

        @Override
        public String toString() {
            return "TransactionalPayload{" +
                    "txnId='" + txnId + '\'' +
                    ", eventCount=" + eventCount +
                    ", totalAmount=" + totalAmount +
                    '}';
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set(CheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(10));
        conf.set(CheckpointingOptions.CHECKPOINTING_CONSISTENCY_MODE, CheckpointingMode.EXACTLY_ONCE);
        conf.set(CheckpointingOptions.CHECKPOINTING_TIMEOUT, Duration.ofMinutes(2));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);

        // 模拟订单输入流
        DataGeneratorSource<OrderEvent> orderSource = new DataGeneratorSource<>(
                new GeneratorFunction<>() {
                    @Serial
                    private static final long serialVersionUID = 1L;
                    private final Random random = new Random();

                    @Override
                    public OrderEvent map(Long value) {
                        String orderId = "ORD-" + value;
                        String userId = "USER-" + (100 + random.nextInt(10));
                        double amount = 20.0 + random.nextDouble() * 100.0;
                        return new OrderEvent(orderId, userId, amount, System.currentTimeMillis());
                    }
                },
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(20),
                TypeInformation.of(OrderEvent.class)
        );

        DataStream<OrderEvent> orderStream = env
                .fromSource(orderSource, org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(), "Order-Source");

        // 接入现代 SinkV2 两阶段提交 Sink (彻底取代旧版 TwoPhaseCommitSinkFunction)
        orderStream.sinkTo(new ModernTransactionalSink());

        env.execute("Modern Exactly-Once SinkV2 2PC Demo Job");
    }

    /**
     * 现代 SinkV2 两阶段提交 Sink：实现 Sink 与 SupportsCommitter
     */
    public static class ModernTransactionalSink 
            implements Sink<OrderEvent>, SupportsCommitter<TransactionalPayload> {

        @Serial
        private static final long serialVersionUID = 1L;

        @Override
        @SuppressWarnings("deprecation")
        public SinkWriter<OrderEvent> createWriter(InitContext context) {
            int subtaskIndex = context.getSubtaskId();
            return new TransactionalSinkWriter(subtaskIndex);
        }

        @Override
        public Committer<TransactionalPayload> createCommitter(CommitterInitContext context) {
            return new TransactionalCommitter();
        }

        @Override
        public SimpleVersionedSerializer<TransactionalPayload> getCommittableSerializer() {
            return new TransactionalPayloadSerializer();
        }
    }

    /**
     * 第一阶段写入与预提交器：CommittingSinkWriter
     */
    public static class TransactionalSinkWriter 
            implements CommittingSinkWriter<OrderEvent, TransactionalPayload> {

        private final int subtaskIndex;
        private final List<OrderEvent> transactionBuffer = new ArrayList<>();
        private String currentTxnId;
        private double runningAmount = 0.0;

        public TransactionalSinkWriter(int subtaskIndex) {
            this.subtaskIndex = subtaskIndex;
            this.currentTxnId = generateNewTxnId();
        }

        private String generateNewTxnId() {
            return "TXN-SUBTASK-" + subtaskIndex + "-" + UUID.randomUUID();
        }

        @Override
        public void write(OrderEvent element, Context context) {
            transactionBuffer.add(element);
            runningAmount += element.getAmount();
        }

        @Override
        public void flush(boolean endOfInput) {
            // 预留 flush 逻辑
        }

        /**
         * Checkpoint 触发预提交：将当前批次生成凭据返回，并开启新事务
         */
        @Override
        public Collection<TransactionalPayload> prepareCommit() {
            if (transactionBuffer.isEmpty()) {
                return Collections.emptyList();
            }

            TransactionalPayload payload = new TransactionalPayload(
                    currentTxnId, transactionBuffer.size(), runningAmount
            );

            LOG.info(">>> [2PC Phase 1: Pre-Commit] Subtask {} frozen Txn: {} (count={}, amount={})",
                    subtaskIndex, currentTxnId, payload.getEventCount(), payload.getTotalAmount());

            // 滚动开启下一段新事务
            this.transactionBuffer.clear();
            this.runningAmount = 0.0;
            this.currentTxnId = generateNewTxnId();

            return Collections.singletonList(payload);
        }

        @Override
        public void close() {
            transactionBuffer.clear();
        }
    }

    /**
     * 第二阶段最终提交器：Committer
     */
    public static class TransactionalCommitter implements Committer<TransactionalPayload> {

        @Override
        public void commit(Collection<CommitRequest<TransactionalPayload>> requests) {
            for (CommitRequest<TransactionalPayload> request : requests) {
                TransactionalPayload committable = request.getCommittable();
                // 模拟向下游存储 (Kafka 事务 / 数据库两阶段提交) 正式提交
                LOG.info("✅ [2PC Phase 2: Final Commit] Downstream txn committed: {}", committable);
            }
        }

        @Override
        public void close() {
        }
    }

    /**
     * 事务凭据序列化器：用于故障恢复与快照持久化
     */
    public static class TransactionalPayloadSerializer 
            implements SimpleVersionedSerializer<TransactionalPayload> {

        private static final int VERSION = 1;

        @Override
        public int getVersion() {
            return VERSION;
        }

        @Override
        public byte[] serialize(TransactionalPayload obj) throws IOException {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(baos);
            out.writeUTF(obj.getTxnId());
            out.writeInt(obj.getEventCount());
            out.writeDouble(obj.getTotalAmount());
            out.flush();
            return baos.toByteArray();
        }

        @Override
        public TransactionalPayload deserialize(int version, byte[] serialized) throws IOException {
            ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
            DataInputStream in = new DataInputStream(bais);
            String txnId = in.readUTF();
            int count = in.readInt();
            double amount = in.readDouble();
            return new TransactionalPayload(txnId, count, amount);
        }
    }
}
