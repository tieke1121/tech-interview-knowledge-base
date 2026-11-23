package com.tech.interview.kafka;

import com.tech.interview.kafka.service.TransactionalPipelineService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 事务管道启动入口主类
 */
public class KafkaTransactionalApp {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaTransactionalApp.class);

    public static void main(String[] args) {
        String bootstrapServers = System.getProperty("kafka.bootstrap.servers", "localhost:9092");
        String inputTopic = System.getProperty("kafka.topic.input", "orders.raw");
        String outputTopic = System.getProperty("kafka.topic.output", "orders.transformed");
        String groupId = System.getProperty("kafka.group.id", "order-trans-pipeline-group");
        String transactionalId = System.getProperty("kafka.transactional.id", "order-tx-instance-01");

        TransactionalPipelineService service = new TransactionalPipelineService(
                bootstrapServers,
                inputTopic,
                outputTopic,
                groupId,
                transactionalId
        );

        // 注册 JVM 优雅停机钩子
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("捕获退出信号，触发优雅停机...");
            service.stop();
        }));

        try {
            service.init();
            LOG.info("Kafka 事务管道启动就绪，开始拉取消息...");
            service.startLoop(0); // 持续运行
        } catch (Exception e) {
            LOG.error("Kafka 事务管道运行异常中断: ", e);
        }
    }
}
