#!/usr/bin/env bash
# ========================================================================
# HDFS Data-Lake Layout Bootstrap Script for FraudStream
# Deliverable: W2-03 / W3-01 (HDFS data-lake layout)
# Owner: Lê Trọng Đạt
# Chạy từ gốc repo: bash infra/hdfs/setup_datalake.sh
# ========================================================================
#
# Script tạo 6 thư mục data-lake trên HDFS. Idempotent: chạy lặp lại
# an toàn nhờ `hdfs dfs -mkdir -p` không lỗi khi thư mục đã tồn tại.
#
# Tiêu chí hoàn thành:
#   - NameNode pod phải đang Running và Ready.
#   - Đủ 6 thư mục: raw, cleaned, curated, late_events, checkpoints, models.
#   - Script trả exit code 0 khi thành công, khác 0 khi có lỗi.

set -euo pipefail

NAMESPACE="fraudstream"
TIMEOUT_READY=180

echo "============================================================"
echo " HDFS Data-Lake Layout Bootstrap — FraudStream"
echo " Namespace: ${NAMESPACE}"
echo " Thời gian: $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
echo "============================================================"

# ------------------------------------------------------------------
# 1. Tìm NameNode pod
# ------------------------------------------------------------------
echo ""
echo "[1/3] Tìm HDFS NameNode pod..."

# Dùng cùng selector như verify_infra.sh để đảm bảo nhất quán.
# Thử app=hdfs-namenode trước (selector 1), fallback sang component=namenode (selector 2).
NAMENODE_POD=$(kubectl get pods -n "${NAMESPACE}" \
    -l app=hdfs-namenode \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)

if [[ -z "${NAMENODE_POD}" ]]; then
  NAMENODE_POD=$(kubectl get pods -n "${NAMESPACE}" \
      -l app.kubernetes.io/component=namenode \
      -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
fi

if [[ -z "${NAMENODE_POD}" ]]; then
  echo "ERROR: Không tìm thấy HDFS NameNode pod trong namespace ${NAMESPACE}."
  echo "       Hãy deploy HDFS trước: kubectl apply -f infra/hdfs/hdfs-namenode.yaml"
  exit 1
fi
echo "       Tìm thấy pod: ${NAMENODE_POD}"

# ------------------------------------------------------------------
# 2. Chờ NameNode Ready
# ------------------------------------------------------------------
echo ""
echo "[2/3] Chờ NameNode Ready (timeout ${TIMEOUT_READY}s)..."
kubectl wait --namespace "${NAMESPACE}" \
    --for=condition=Ready pod/"${NAMENODE_POD}" \
    --timeout="${TIMEOUT_READY}s"
echo "       NameNode Ready."

# Kiểm tra HDFS có live DataNode không (cần có ít nhất 1 DataNode để ghi được).
# Không coi cảnh báo là PASS: mkdir có thể thành công khi DataNode chưa usable,
# nhưng file write sau đó sẽ không bền vững.
echo "       Kiểm tra live DataNodes..."
DEADLINE=$((SECONDS + TIMEOUT_READY))
while true; do
  DFSREPORT=$(kubectl exec -n "${NAMESPACE}" "${NAMENODE_POD}" -- \
      hdfs dfsadmin -report 2>/dev/null || true)
  if echo "${DFSREPORT}" | grep -Eq 'Live datanodes \([1-9][0-9]*\)'; then
    echo "       Live DataNode đã sẵn sàng."
    break
  fi
  if [[ "${SECONDS}" -ge "${DEADLINE}" ]]; then
    echo "ERROR: Không có Live DataNode sau ${TIMEOUT_READY}s; dừng bootstrap."
    echo "       Kiểm tra: kubectl get pods -n ${NAMESPACE} -l app.kubernetes.io/component=datanode"
    exit 1
  fi
  echo "       Chưa có Live DataNode — thử lại sau 10s..."
  sleep 10
done

# ------------------------------------------------------------------
# 3. Tạo 6 thư mục data-lake (idempotent)
# ------------------------------------------------------------------
echo ""
echo "[3/3] Tạo cấu trúc thư mục data-lake tại /fraud-data/..."

REQUIRED_PATHS=(
  "/fraud-data/raw"
  "/fraud-data/cleaned"
  "/fraud-data/curated"
  "/fraud-data/late_events"
  "/fraud-data/checkpoints"
  "/fraud-data/models"
)

for PATH_ITEM in "${REQUIRED_PATHS[@]}"; do
  # -mkdir -p: tạo cả parent nếu cần, không lỗi khi đã tồn tại
  kubectl exec -n "${NAMESPACE}" "${NAMENODE_POD}" -- \
      hdfs dfs -mkdir -p "${PATH_ITEM}"
  echo "  [OK] ${PATH_ITEM}"
done

# Xác minh bằng cách list
echo ""
echo "Xác minh layout /fraud-data/:"
kubectl exec -n "${NAMESPACE}" "${NAMENODE_POD}" -- \
    hdfs dfs -ls /fraud-data

echo ""
echo "============================================================"
echo " PASS: Data-lake layout đã được tạo đầy đủ (6/6 thư mục)."
echo "============================================================"
