package com.tech.interview.flink.compute.broadcast;

import com.tech.interview.flink.model.OrderEvent;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serial;
import java.util.Random;

/**
 * 【计算端核心考点：广播状态模式 (Broadcast State Pattern)】
 * 
 * 核心架构考点：
 * 1. 动态规则热加载：业务大流 (高频) 与控制规则流 (低频) 动态关联，实现不停机实时变更风控/营销规则；
 * 2. 读写隔离保障：
 *    - 在 processBroadcastElement() 中获取的是可写的 BroadcastState，由控制流单向更新；
 *    - 在 processElement() 中获取的是只读的 ReadOnlyBroadcastState，杜绝数据流修改规则引起的数据不一致；
 * 3. 内存与 Checkpoint 机制：
 *    - 广播状态属于 Operator State，每个 TaskManager / Subtask 独享一份堆内存副本；
 *    - 严禁在广播流中推送大宽表数据，否则导致广播状态暴涨触发频繁 GC 甚至 OOM；
 *    - 做 Checkpoint 时每个并行度实例都会将自身广播状态全量序列化进快照中。
 */
public class DynamicBroadcastRuleJob {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicBroadcastRuleJob.class);

    // 定义广播状态描述器 (Key: ruleId, Value: RuleModel)
    public static final MapStateDescriptor<String, RuleModel> RULE_STATE_DESCRIPTOR =
            new MapStateDescriptor<>(
                    "RulesBroadcastState",
                    BasicTypeInfo.STRING_TYPE_INFO,
                    TypeInformation.of(RuleModel.class)
            );

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);

        // 1. 模拟业务大流：订单交易流 (100条/秒)
        DataGeneratorSource<OrderEvent> orderSource = new DataGeneratorSource<>(
                new GeneratorFunction<>() {
                    @Serial
                    private static final long serialVersionUID = 1L;
                    private final Random random = new Random();

                    @Override
                    public OrderEvent map(Long value) {
                        String orderId = "ORD-" + value;
                        String userId = "USER-" + (100 + random.nextInt(10));
                        double amount = 100.0 + random.nextDouble() * 2000.0;
                        return new OrderEvent(orderId, userId, amount, System.currentTimeMillis());
                    }
                },
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(20),
                TypeInformation.of(OrderEvent.class)
        );

        DataStream<OrderEvent> orderStream = env
                .fromSource(orderSource, org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(), "Order-Source");

        // 2. 模拟控制规则流：动态下发风控阈值规则 (低频变更)
        DataGeneratorSource<RuleModel> ruleSource = new DataGeneratorSource<>(
                new GeneratorFunction<>() {
                    @Serial
                    private static final long serialVersionUID = 1L;

                    @Override
                    public RuleModel map(Long value) {
                        // 每生成一次规则，动态变更限额阈值
                        double threshold = 500.0 + (value % 3) * 500.0; // 500, 1000, 1500 动态轮换
                        return new RuleModel("RULE-AMOUNT-LIMIT", "PAYMENT_RISK", threshold, RuleModel.Action.ADD);
                    }
                },
                50L,
                RateLimiterStrategy.perSecond(1), // 1秒更新一次规则模拟热更新
                TypeInformation.of(RuleModel.class)
        );

        DataStream<RuleModel> ruleStream = env
                .fromSource(ruleSource, org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(), "Rule-Source");

        // 3. 将规则流广播化
        BroadcastStream<RuleModel> broadcastRuleStream = ruleStream.broadcast(RULE_STATE_DESCRIPTOR);

        // 4. 业务流与广播规则流 Connect，并在 KeyedBroadcastProcessFunction 中完成动态匹配
        DataStream<String> alertStream = orderStream
                .keyBy(OrderEvent::getUserId)
                .connect(broadcastRuleStream)
                .process(new DynamicRuleEvaluationFunction());

        alertStream.print();

        env.execute("Dynamic Broadcast Rule Pattern Job");
    }

    /**
     * 核心算子：KeyedBroadcastProcessFunction 动态规则匹配
     */
    public static class DynamicRuleEvaluationFunction 
            extends KeyedBroadcastProcessFunction<String, OrderEvent, RuleModel, String> {

        @Serial
        private static final long serialVersionUID = 1L;

        /**
         * 处理常规业务数据流 (只能只读访问广播状态)
         */
        @Override
        public void processElement(OrderEvent order, ReadOnlyContext ctx, Collector<String> out) throws Exception {
            ReadOnlyBroadcastState<String, RuleModel> ruleState = ctx.getBroadcastState(RULE_STATE_DESCRIPTOR);

            // 获取当前动态生效的风控规则
            RuleModel activeRule = ruleState.get("RULE-AMOUNT-LIMIT");
            if (activeRule != null) {
                if (order.getAmount() > activeRule.getMaxAmountThreshold()) {
                    String alert = String.format("⚠️ [RISK ALERT] Order %s for User %s Amount %.2f EXCEEDED Threshold %.2f (RuleId=%s)",
                            order.getOrderId(), order.getUserId(), order.getAmount(), 
                            activeRule.getMaxAmountThreshold(), activeRule.getRuleId());
                    out.collect(alert);
                }
            }
        }

        /**
         * 处理控制规则流 (拥有独占写权限，更新广播状态)
         */
        @Override
        public void processBroadcastElement(RuleModel rule, Context ctx, Collector<String> out) throws Exception {
            BroadcastState<String, RuleModel> ruleState = ctx.getBroadcastState(RULE_STATE_DESCRIPTOR);

            if (rule.getAction() == RuleModel.Action.DELETE) {
                ruleState.remove(rule.getRuleId());
                LOG.info(">>> [Broadcast State] Rule DELETED: {}", rule.getRuleId());
            } else {
                ruleState.put(rule.getRuleId(), rule);
                LOG.info(">>> [Broadcast State] Rule APPLIED: {} Threshold={}", rule.getRuleId(), rule.getMaxAmountThreshold());
            }
        }
    }
}
