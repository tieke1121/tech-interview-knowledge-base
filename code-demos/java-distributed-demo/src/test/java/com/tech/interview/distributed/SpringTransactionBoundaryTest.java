package com.tech.interview.distributed;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabase;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.sql.DataSource;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@DisplayName("Spring 事务边界：订单失败后还有哪些行")
class SpringTransactionBoundaryTest {
    private AnnotationConfigApplicationContext context;
    private OrderService orders;
    private JdbcTemplate jdbc;

    @BeforeEach
    void setUp() {
        context = new AnnotationConfigApplicationContext(TxConfig.class);
        orders = context.getBean(OrderService.class);
        jdbc = context.getBean(JdbcTemplate.class);
        jdbc.execute("CREATE TABLE orders (id INT PRIMARY KEY)");
        jdbc.execute("CREATE TABLE order_audit (order_id INT PRIMARY KEY)");
    }

    @AfterEach
    void tearDown() {
        context.close();
    }

    @Test
    @DisplayName("同类自调用跳过 REQUIRES_NEW，审计随订单回滚")
    void selfInvocationRollsBackAuditWithOrder() {
        assertAfterRejection(orders::rejectAfterSelfCall, 0);
    }

    @Test
    @DisplayName("经代理进入 REQUIRES_NEW，外层回滚后审计仍在")
    void proxiedRequiresNewCommitsAudit() {
        assertAfterRejection(() -> orders.rejectAfter(orders::writeIndependentAudit), 1);
    }

    @Test
    @DisplayName("同线程经代理进入 REQUIRED，审计加入外层事务")
    void requiredJoinsCallerOnSameThread() {
        assertAfterRejection(() -> orders.rejectAfter(orders::writeRequiredAudit), 0);
    }

    @Test
    @DisplayName("工作线程经代理进入 REQUIRED，独立提交的审计保留")
    void requiredOnWorkerStartsItsOwnTransaction() {
        ExecutorService worker = Executors.newSingleThreadExecutor();
        try {
            assertAfterRejection(() -> orders.rejectAfter(
                    () -> CompletableFuture.runAsync(orders::writeRequiredAudit, worker).join()), 1);
        } finally {
            worker.shutdownNow();
        }
    }

    private void assertAfterRejection(Runnable attempt, int expectedAuditRows) {
        var failure = assertThrows(IllegalStateException.class, attempt::run);
        assertEquals("reject order", failure.getMessage());
        // 从代理返回后，在事务外读取已提交结果。
        assertEquals(0, jdbc.queryForObject("SELECT COUNT(*) FROM orders", Integer.class));
        assertEquals(expectedAuditRows, jdbc.queryForObject("SELECT COUNT(*) FROM order_audit", Integer.class));
    }

    @Configuration(proxyBeanMethods = false)
    @EnableTransactionManagement
    static class TxConfig {
        @Bean(destroyMethod = "shutdown")
        EmbeddedDatabase dataSource() {
            return new EmbeddedDatabaseBuilder().generateUniqueName(true).setType(EmbeddedDatabaseType.H2).build();
        }

        @Bean
        DataSourceTransactionManager transactionManager(DataSource dataSource) {
            return new DataSourceTransactionManager(dataSource);
        }

        @Bean
        JdbcTemplate jdbcTemplate(DataSource dataSource) {
            return new JdbcTemplate(dataSource);
        }

        @Bean
        OrderService orders(JdbcTemplate jdbc) {
            return new OrderService(jdbc);
        }
    }

    static class OrderService {
        private final JdbcTemplate jdbc;

        OrderService(JdbcTemplate jdbc) {
            this.jdbc = jdbc;
        }

        @Transactional
        public void rejectAfterSelfCall() {
            rejectAfter(this::writeIndependentAudit);
        }

        // 写入和故障保持相同，每个实验只改变审计方法的调用路径。
        @Transactional
        public void rejectAfter(Runnable writeAudit) {
            jdbc.update("INSERT INTO orders VALUES (1)");
            writeAudit.run();
            throw new IllegalStateException("reject order");
        }

        @Transactional(propagation = Propagation.REQUIRES_NEW)
        public void writeIndependentAudit() {
            jdbc.update("INSERT INTO order_audit VALUES (1)");
        }

        @Transactional
        public void writeRequiredAudit() {
            jdbc.update("INSERT INTO order_audit VALUES (1)");
        }
    }
}
