#!/usr/bin/env bash
# ==============================================================================
# Flink on YARN 生产优雅停机脚本 (触发 Savepoint 并安全退出)
#
# 生产运维规范：
# 发布上线、版本热升级或机器下线迁移时，必须通过触发 Savepoint 优雅停止作业，
# 严禁直接通过 yarn application -kill 暴力杀死进程，防止状态损坏或事务悬挂。
# ==============================================================================

set -euo pipefail

FLINK_HOME="${FLINK_HOME:-/opt/flink}"
HDFS_SAVEPOINT_BASE="${HDFS_SAVEPOINT_BASE:-hdfs:///flink/savepoints/order_risk}"

if [ "$#" -lt 2 ]; then
    echo "使用方式: $0 <YARN_APPLICATION_ID> <FLINK_JOB_ID>"
    echo "示例: $0 application_1688888888888_0001 5f042e946a3625b57f02241e330a8456"
    exit 1
fi

YARN_APP_ID="$1"
JOB_ID="$2"

echo "=============================================================================="
echo ">>> [Flink on YARN] 触发 Savepoint 并优雅停机"
echo ">>> YARN App ID: ${YARN_APP_ID}"
echo ">>> Flink Job ID: ${JOB_ID}"
echo ">>> Savepoint 目标路径: ${HDFS_SAVEPOINT_BASE}"
echo "=============================================================================="

"${FLINK_HOME}/bin/flink" stop "${JOB_ID}" \
  -t yarn-application \
  -Dyarn.application.id="${YARN_APP_ID}" \
  --savepointPath "${HDFS_SAVEPOINT_BASE}"

echo "[+] 作业已成功保存状态并优雅停机。"
