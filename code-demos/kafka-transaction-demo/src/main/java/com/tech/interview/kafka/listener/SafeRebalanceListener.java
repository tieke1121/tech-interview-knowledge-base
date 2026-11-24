package com.tech.interview.kafka.listener;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * 生产级安全重平衡监听器 (ConsumerRebalanceListener)
 *
 * 解决生产痛点：
 * 1. 发生 Rebalance 时，若上游已拉取但尚未提交位移，新接管的 Consumer 会重复消费；
 * 2. 在 Kafka 事务管道中，分区被剥夺时必须清理或阻断当前挂起的未决事务，防止脏提交。
 */
public class SafeRebalanceListener implements ConsumerRebalanceListener {

    private static final Logger LOG = LoggerFactory.getLogger(SafeRebalanceListener.class);

    private final Set<TopicPartition> currentAssignedPartitions = Collections.synchronizedSet(new HashSet<>());
    private volatile boolean rebalanceOccurred = false;

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        LOG.warn("Rebalance 触发: 分区将被剥夺: {}", partitions);
        rebalanceOccurred = true;

        // 关键动作：清理本地分配集合
        currentAssignedPartitions.removeAll(partitions);

        // 架构提示：若非事务型流，在此处应同步提交已完成的局部 offset；
        // 若处于事务管道中，不能在此盲提，而应标记中断当前 batch，让后续主循环走 abortTransaction()
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        LOG.info("Rebalance 完成: 成功分配到新分区: {}", partitions);
        currentAssignedPartitions.addAll(partitions);
        rebalanceOccurred = false;
    }

    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
        LOG.error("严重异常: 消费者被 Coordinator 判定离组，分区异常丢失: {}", partitions);
        currentAssignedPartitions.removeAll(partitions);
        rebalanceOccurred = true;
    }

    public boolean isRebalanceOccurred() {
        return rebalanceOccurred;
    }

    public void resetRebalanceFlag() {
        this.rebalanceOccurred = false;
    }

    public Set<TopicPartition> getCurrentAssignedPartitions() {
        return Collections.unmodifiableSet(currentAssignedPartitions);
    }
}
