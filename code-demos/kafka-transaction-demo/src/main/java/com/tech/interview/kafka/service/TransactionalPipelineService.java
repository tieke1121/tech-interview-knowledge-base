package com.tech.interview.kafka.service;

import com.tech.interview.kafka.listener.SafeRebalanceListener;
import com.tech.interview.kafka.model.TransactionRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Kafka 生产级 Consume-Transform-Produce 事务管道服务
 *
 * 核心设计哲学：
 * 1. 采用 Kafka 事务 API 实现跨 Topic 的原子“消费-计算-生产”闭环；
 * 2. 消费位移不通过 Consumer.commitSync() 单独提交，而是通过 producer.sendOffsetsToTransaction()
 *    写入事务协调器（Transaction Coordinator），保证下游写入与上游位移提交在同一个 2PC 事务中原子生效；
 * 3. 下游消费端配置 isolation.level=read_committed，彻底杜绝未提交脏数据外溢。
 */
public class TransactionalPipelineService {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionalPipelineService.class);

    private final String bootstrapServers;
    private final String inputTopic;
    private final String outputTopic;
    private final String consumerGroupId;
    private final String transactionalId;

    private KafkaProducer<String, String> producer;
    private KafkaConsumer<String, String> consumer;
    private SafeRebalanceListener rebalanceListener;

    private volatile boolean isRunning = true;

    public TransactionalPipelineService(
            String bootstrapServers,
            String inputTopic,
            String outputTopic,
            String consumerGroupId,
            String transactionalId) {
        this.bootstrapServers = bootstrapServers;
        this.inputTopic = inputTopic;
        this.outputTopic = outputTopic;
        this.consumerGroupId = consumerGroupId;
        this.transactionalId = transactionalId;
    }

    /**
     * 构建生产者配置 (幂等 + 事务 + acks=all)
     */
    public Properties buildProducerProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 核心事务与幂等配置
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.ACKS_CONFIG, "all"); // 必须为 all / -1，确保 ISR 全部落盘
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5); // 启用幂等时最大允许为 5

        // 高吞吐内存缓冲调优基线
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536); // 64 KB 批次
        props.put(ProducerConfig.LINGER_MS_CONFIG, 20);      // 20ms 等待微批，极大降低系统调用频次
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67108864); // 64 MB 记录累加器缓冲池
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4"); // 低 CPU 高压缩比首选

        return props;
    }

    /**
     * 构建消费者配置 (已提交读 + 禁用自动位移提交)
     */
    public Properties buildConsumerProperties() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // 核心事务隔离级别：仅读取已提交事务数据
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        // 彻底关闭自动提交，位移由事务协调器托管
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // 分区重平衡策略推荐：协作粘性重平衡 (Cooperative Sticky) 避免全量 Stop-The-World
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");

        return props;
    }

    public void init() {
        LOG.info("初始化 Kafka 事务管道: transactionalId={}, groupId={}", transactionalId, consumerGroupId);
        this.producer = new KafkaProducer<>(buildProducerProperties());
        this.consumer = new KafkaConsumer<>(buildConsumerProperties());
        this.rebalanceListener = new SafeRebalanceListener();

        // 关键步骤 1：初始化事务 (向 Transaction Coordinator 注册 ProducerId 与 Epoch)
        producer.initTransactions();
        consumer.subscribe(Collections.singletonList(inputTopic), rebalanceListener);
    }

    /**
     * 运行主事件流处理循环
     */
    public void startLoop(int maxBatches) {
        int processedBatches = 0;

        try {
            while (isRunning && (maxBatches <= 0 || processedBatches < maxBatches)) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));
                if (records.isEmpty()) {
                    continue;
                }

                LOG.info("拉取到 {} 条消息，开启分布式事务处理...", records.count());

                // 关键步骤 2：开启本地事务上下文
                producer.beginTransaction();

                try {
                    Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();

                    for (ConsumerRecord<String, String> record : records) {
                        // 业务转换逻辑
                        TransactionRecord cleanRecord = transform(record.value());

                        // 步骤 3：向下游写入业务消息 (包含在当前未决事务中)
                        producer.send(new ProducerRecord<>(
                                outputTopic,
                                cleanRecord.getTxId(),
                                cleanRecord.toString()
                        ));

                        // 累积各分区待提交的位移 (指向下一次拉取的 offset + 1)
                        offsetsToCommit.put(
                                new TopicPartition(record.topic(), record.partition()),
                                new OffsetAndMetadata(record.offset() + 1)
                        );
                    }

                    // 关键步骤 4：将消费位移提交与写入事务原子绑定！
                    // 此操作直接向 Transaction Coordinator 发送 AddOffsetsToTxnRequest
                    producer.sendOffsetsToTransaction(offsetsToCommit, consumer.groupMetadata());

                    // 关键步骤 5：提交事务 (两阶段提交终态确认)
                    producer.commitTransaction();
                    processedBatches++;
                    LOG.info("分布式事务提交成功，完成批次: {}", processedBatches);

                } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
                    // 致命异常：当前 Producer 已被新 Epoch 隔离或认证失效，必须终止
                    LOG.error("检测到致命生产者隔离异常，作业退出: ", e);
                    break;
                } catch (KafkaException e) {
                    // 可恢复异常：放弃当前事务，执行 Abort
                    LOG.warn("事务处理异常，执行回滚并重新寻位: {}", e.getMessage());
                    producer.abortTransaction();
                    // 重新寻位到已提交位移点，准备下一轮重试
                    seekToCommittedOffsets();
                }
            }
        } finally {
            close();
        }
    }

    /**
     * 业务纯函数转换
     */
    public TransactionRecord transform(String rawValue) {
        // 模拟清洗规范与业务风控字段附加
        long now = System.currentTimeMillis();
        return new TransactionRecord(
                "tx_" + System.nanoTime(),
                "account_from_sample",
                "account_to_sample",
                1000.0,
                now,
                "CLEANED_SUCCESS"
        );
    }

    private void seekToCommittedOffsets() {
        try {
            for (TopicPartition partition : consumer.assignment()) {
                OffsetAndMetadata committed = consumer.committed(Collections.singleton(partition)).get(partition);
                if (committed != null) {
                    consumer.seek(partition, committed.offset());
                }
            }
        } catch (Exception ex) {
            LOG.error("寻位至已提交位移失败: ", ex);
        }
    }

    public void stop() {
        this.isRunning = false;
    }

    public void close() {
        LOG.info("关闭 Kafka 事务管道资源...");
        if (producer != null) {
            try { producer.close(Duration.ofSeconds(5)); } catch (Exception ignored) {}
        }
        if (consumer != null) {
            try { consumer.close(Duration.ofSeconds(5)); } catch (Exception ignored) {}
        }
    }

    public SafeRebalanceListener getRebalanceListener() {
        return rebalanceListener;
    }
}
