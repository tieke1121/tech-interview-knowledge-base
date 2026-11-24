package com.tech.interview.flink.compute.cep;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serial;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * 【计算端核心考点：复杂事件处理 (Flink CEP) NFA 状态机与超时匹配】
 * 
 * 核心架构考点：
 * 1. NFA 底层机制：
 *    - Flink CEP 底层采用非确定有限状态自动机 (NFA)，使用 SharedBuffer 共享状态树避免笛卡尔积复制；
 * 2. 状态爆炸防范与连续性控制：
 *    - next(): 严格连续，要求事件必须紧挨着发生；
 *    - followedBy(): 宽松连续，允许中间插入无关事件；
 *    - consecutive(): 循环模式严格连续性；
 *    - 必须设置 within(Duration) 时间窗口进行剪枝，否则未匹配的状态会永久滞留在 RocksDB 导致大状态 OOM；
 * 3. 超时部分匹配 (TimedOutPartialMatchHandler)：
 *    - 满足了前置条件 (如连续3次登录失败)，但在规定时间内未触发后续动作 (如未发起转账)，
 *      通过 OutputTag 旁路捕获超时事件，触发账号临时风控冻结。
 */
public class FraudDetectionCepJob {

    private static final Logger LOG = LoggerFactory.getLogger(FraudDetectionCepJob.class);

    // 侧输出标签：捕获超时未完成的风控序列 (如连续失败但未发起敏感操作)
    public static final OutputTag<String> TIMEOUT_PATTERN_TAG = new OutputTag<>("timeout-login-risk-tag") {};

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);

        // 1. 模拟登录与敏感操作事件流
        DataGeneratorSource<UserLoginEvent> loginSource = new DataGeneratorSource<>(
                new GeneratorFunction<>() {
                    @Serial
                    private static final long serialVersionUID = 1L;
                    private final Random random = new Random();

                    @Override
                    public UserLoginEvent map(Long value) {
                        String userId = "USER-" + (100 + (value % 5));
                        String ip = "192.168.1." + (10 + random.nextInt(5));
                        String type;
                        if (value % 4 == 0) {
                            type = "TRANSFER_LARGE_AMOUNT";
                        } else if (value % 7 == 0) {
                            type = "LOGIN_SUCCESS";
                        } else {
                            type = "LOGIN_FAIL";
                        }
                        return new UserLoginEvent(userId, ip, type, System.currentTimeMillis());
                    }
                },
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(10),
                TypeInformation.of(UserLoginEvent.class)
        );

        WatermarkStrategy<UserLoginEvent> watermarkStrategy = WatermarkStrategy
                .<UserLoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner((event, timestamp) -> event.getEventTime());

        DataStream<UserLoginEvent> loginStream = env
                .fromSource(loginSource, watermarkStrategy, "Login-Source");

        // 2. 定义 CEP 模式：在 3 分钟内连续 3 次登录失败，紧接着 (followedBy) 发生大额转账
        Pattern<UserLoginEvent, ?> fraudPattern = Pattern
                .<UserLoginEvent>begin("failed_logins")
                .where(SimpleCondition.of(event -> "LOGIN_FAIL".equals(event.getEventType())))
                .times(3)
                .consecutive() // 连续失败
                .followedBy("risky_transfer") // 宽松近邻：发生大额转账
                .where(SimpleCondition.of(event -> "TRANSFER_LARGE_AMOUNT".equals(event.getEventType())))
                .within(Duration.ofMinutes(3)); // 严格限制生命周期防状态爆炸

        // 3. 在 keyedStream 上执行模式匹配
        PatternStream<UserLoginEvent> patternStream = CEP.pattern(
                loginStream.keyBy(UserLoginEvent::getUserId),
                fraudPattern
        );

        // 4. 处理匹配成功事件，并使用 TimedOutPartialMatchHandler 捕获超时部分匹配
        SingleOutputStreamOperator<String> matchedStream = patternStream.process(new FraudPatternProcessFunction());

        // 主流输出高危转账欺诈告警
        matchedStream.print();

        // 侧输出流输出连续登录失败超时的疑似暴力破解告警
        matchedStream.getSideOutput(TIMEOUT_PATTERN_TAG).print();

        env.execute("Fraud Detection CEP Job");
    }

    /**
     * 模式处理核心函数：实现常规匹配与超时降级捕获双闭环
     */
    public static class FraudPatternProcessFunction 
            extends PatternProcessFunction<UserLoginEvent, String>
            implements TimedOutPartialMatchHandler<UserLoginEvent> {

        @Serial
        private static final long serialVersionUID = 1L;

        /**
         * 完整匹配：连续3次登录失败并在3分钟内发起大额转账 (最高危阻断事件)
         */
        @Override
        public void processMatch(Map<String, List<UserLoginEvent>> match, Context ctx, Collector<String> out) {
            List<UserLoginEvent> failedLogins = match.get("failed_logins");
            List<UserLoginEvent> riskyTransfers = match.get("risky_transfer");

            String userId = riskyTransfers.get(0).getUserId();
            String alert = String.format("🚨 [CRITICAL FRAUD ALERT] User %s had %d consecutive LOGIN_FAILs then performed TRANSFER_LARGE_AMOUNT from IP %s!",
                    userId, failedLogins.size(), riskyTransfers.get(0).getIp());
            out.collect(alert);
        }

        /**
         * 超时匹配捕获：连续登录失败但超过3分钟未做敏感操作 (潜在撞库/暴力破解告警)
         */
        @Override
        public void processTimedOutMatch(Map<String, List<UserLoginEvent>> partialMatch, Context ctx) {
            List<UserLoginEvent> failedLogins = partialMatch.get("failed_logins");
            if (failedLogins != null && !failedLogins.isEmpty()) {
                String userId = failedLogins.get(0).getUserId();
                String warning = String.format("⚠️ [BRUTE FORCE SUSPECT] User %s had %d consecutive failed logins but timed out without transfer. Locking account temporarily.",
                        userId, failedLogins.size());
                ctx.output(TIMEOUT_PATTERN_TAG, warning);
            }
        }
    }
}
