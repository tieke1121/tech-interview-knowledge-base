#!/usr/bin/env bash
# ==============================================================================
# Flink on YARN Application Mode 生产级一键提交脚本
#
# 优势与架构特点：
# 1. Application Mode 下，main() 方法直接在 YARN 的 ApplicationMaster (JobManager) 内部执行；
# 2. 客户端机器仅作为指令触发端，无需在本地下载/解析超大 Jar，规避客户端网络瓶颈与 OOM；
# 3. 每个作业独享独立的 JobManager 与 TaskManager，实现底座物理多租户隔离。
# ==============================================================================

set -euo pipefail

# 1. 基础环境与路径配置
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

FLINK_HOME="${FLINK_HOME:-/opt/flink}"
HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-/etc/hadoop/conf}"
export HADOOP_CONF_DIR

# 2. 支持通过参数灵活指定运行的作业类 (默认为双流对账核心作业)
# 可选作业:
#   - reconcile (默认): com.tech.interview.flink.DualStreamReconciliationJob (多源、Watermark、定时器双流对账)
#   - risk:            com.tech.interview.flink.OrderRiskStreamingJob (滚动窗口增量聚合、迟到侧道输出)
JOB_TYPE="${1:-reconcile}"

if [ "${JOB_TYPE}" = "risk" ]; then
    JOB_NAME="Realtime-Order-Risk-Metrics"
    MAIN_CLASS="com.tech.interview.flink.OrderRiskStreamingJob"
else
    JOB_NAME="DualStream-Order-Payment-Reconciliation"
    MAIN_CLASS="com.tech.interview.flink.DualStreamReconciliationJob"
fi

YARN_QUEUE="root.realtime.prod"
JAR_PATH="${BASE_DIR}/target/flink-yarn-demo-1.0.0-SNAPSHOT.jar"

# 3. 内存与并发规划 (面向中高并发核心业务链路)
JM_PROCESS_MEMORY="4g"
TM_PROCESS_MEMORY="8g"
SLOTS_PER_TM="4"
VCORES_PER_CONTAINER="2"
PARALLELISM="8"

# 4. Checkpoint 与状态后端存储基线
HDFS_BASE="hdfs:///flink/checkpoints/order_reconciliation"
HDFS_SAVEPOINT_BASE="hdfs:///flink/savepoints/order_reconciliation"

echo "=============================================================================="
echo ">>> [Flink on YARN] 准备提交 Application 模式作业: ${JOB_NAME}"
echo ">>> 主类: ${MAIN_CLASS}"
echo ">>> 队列: ${YARN_QUEUE} | 并行度: ${PARALLELISM} | 单TM Slot: ${SLOTS_PER_TM}"
echo "=============================================================================="

if [ ! -f "${JAR_PATH}" ]; then
    echo "[-] 提示: 未检测到已打包的 Jar: ${JAR_PATH}"
    echo "[-] 集群提交前，请在 code-demos 目录下执行打包: mvn clean package -DskipTests"
fi

# 5. 执行提交命令
"${FLINK_HOME}/bin/flink" run-application -t yarn-application \
  -Dyarn.application.name="${JOB_NAME}" \
  -Dyarn.application.queue="${YARN_QUEUE}" \
  -Djobmanager.memory.process.size="${JM_PROCESS_MEMORY}" \
  -Dtaskmanager.memory.process.size="${TM_PROCESS_MEMORY}" \
  -Dtaskmanager.numberOfTaskSlots="${SLOTS_PER_TM}" \
  -Dyarn.containers.vcores="${VCORES_PER_CONTAINER}" \
  -Dparallelism.default="${PARALLELISM}" \
  -Dstate.backend.type=rocksdb \
  -Dstate.backend.incremental=true \
  -Dstate.checkpoints.dir="${HDFS_BASE}" \
  -Dstate.savepoints.dir="${HDFS_SAVEPOINT_BASE}" \
  -Dexecution.checkpointing.interval=30000 \
  -Dexecution.checkpointing.mode=EXACTLY_ONCE \
  -Dexecution.checkpointing.min-pause-between-checkpoints=15000 \
  -Dexecution.checkpointing.timeout=60000 \
  -Drestart-strategy.type=exponential-delay \
  -c "${MAIN_CLASS}" \
  "${JAR_PATH}"

echo "[+] 提交指令已下发，请在 YARN ResourceManager Web UI (http://<rm-host>:8088) 查看 Application 状态。"
