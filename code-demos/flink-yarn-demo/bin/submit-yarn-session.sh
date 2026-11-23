#!/usr/bin/env bash
# ==============================================================================
# Flink on YARN Session Mode 提交脚本 (开发测试与 Ad-hoc 低延迟调试)
#
# 注意事项：
# 1. 需预先拉起常驻 YARN Session (如: yarn-session.sh -d -jm 2048m -tm 4096m -s 2)
# 2. 适用于快速验证代码拓扑，生产长期运行的核心作业强烈推荐使用 Application 模式。
# ==============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

FLINK_HOME="${FLINK_HOME:-/opt/flink}"
JAR_PATH="${BASE_DIR}/target/flink-yarn-demo-1.0.0-SNAPSHOT.jar"
MAIN_CLASS="com.tech.interview.flink.OrderRiskStreamingJob"

if [ "$#" -lt 1 ]; then
    echo "使用方式: $0 <YARN_APPLICATION_ID>"
    echo "示例: $0 application_1688888888888_0002"
    exit 1
fi

YARN_APP_ID="$1"

if [ ! -f "${JAR_PATH}" ]; then
    echo "[-] 错误: 未找到目标 Jar 文件: ${JAR_PATH}"
    echo "[-] 请先在根工程执行: mvn clean package"
    exit 1
fi

echo "=============================================================================="
echo ">>> [Flink on YARN] 提交作业至已有 YARN Session: ${YARN_APP_ID}"
echo "=============================================================================="

"${FLINK_HOME}/bin/flink" run \
  -t yarn-session \
  -Dyarn.application.id="${YARN_APP_ID}" \
  -c "${MAIN_CLASS}" \
  "${JAR_PATH}"
