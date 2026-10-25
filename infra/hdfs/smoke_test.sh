#!/usr/bin/env bash
# ========================================================================
# HDFS Smoke Test — FraudStream
# Deliverable: W2-03 / W3-01 (HDFS I/O và persistence test)
# Owner: Lê Trọng Đạt
# Chạy từ gốc repo:
#   bash infra/hdfs/smoke_test.sh                   # Kiểm tra I/O + checksum
#   bash infra/hdfs/smoke_test.sh --test-persistence # Thêm persistence test
# ========================================================================
#
# Luồng kiểm thử:
#   [1] Xác nhận NameNode pod Running & Ready
#   [2] Xác nhận PVC đã Bound
#   [3] Xác nhận HDFS healthy (Live datanodes >= 1)
#   [4] Xác nhận 6 data-lake paths tồn tại
#   [5] Upload file JSONL mẫu vào /fraud-data/raw/
#   [6] List — xác nhận file có trong HDFS
#   [7] Cat + SHA-256 checksum — xác nhận nội dung khớp
#   [8] [--test-persistence] Xóa NameNode pod, chờ recreate, đọc lại data

set -euo pipefail

NAMESPACE="fraudstream"
HDFS_SMOKE_PATH="/fraud-data/raw/smoke_test_hdfs.jsonl"
TIMEOUT_READY=180
FAILED=0

# Sự kiện mẫu hợp lệ theo data_dictionary.md:
#   - Đủ 12 trường bắt buộc
#   - card_id_hash / device_id_hash / ip_hash: "sha256:" + 64 lowercase hex chars
#   - amount > 0, currency ISO-4217, channel ∈ {web,mobile,api}
#   - transaction_status ∈ {success,failed}
# Giá trị hash là hex hợp lệ (a-f, 0-9), 64 ký tự.
SAMPLE_JSON='{"transaction_id":"tx_hdfs_smoke_20261019_001","event_time":"2026-10-19T09:00:00Z","customer_id":"cus_hdfs_smoke_001","card_id_hash":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","merchant_id":"merchant_0001","amount":150000.0,"currency":"VND","channel":"web","device_id_hash":"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","ip_hash":"sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc","country":"VN","transaction_status":"success"}'

log_pass() { echo -e " [\033[32mPASS\033[0m] $1"; }
log_fail() { echo -e " [\033[31mFAIL\033[0m] $1"; FAILED=$((FAILED + 1)); }
log_info() { echo "       $1"; }

echo "============================================================"
echo " HDFS Smoke Test — FraudStream"
echo " Namespace : ${NAMESPACE}"
echo " Target    : ${HDFS_SMOKE_PATH}"
echo " Thời gian : $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
echo "============================================================"

# ------------------------------------------------------------------
# Tìm NameNode pod (dùng cùng selector với verify_infra.sh)
# ------------------------------------------------------------------
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
  echo "       Deploy HDFS trước: kubectl apply -f infra/hdfs/hdfs-namenode.yaml"
  exit 1
fi
log_info "NameNode pod: ${NAMENODE_POD}"

# ------------------------------------------------------------------
# [1] NameNode Running & Ready
# ------------------------------------------------------------------
echo ""
echo "--- [1/7] Trạng thái HDFS NameNode pod ---"
NN_PHASE=$(kubectl get pod "${NAMENODE_POD}" -n "${NAMESPACE}" \
    -o jsonpath='{.status.phase}' 2>/dev/null || echo "Unknown")
if [[ "${NN_PHASE}" == "Running" ]]; then
  log_info "Chờ NameNode Ready (timeout ${TIMEOUT_READY}s)..."
  kubectl wait --namespace "${NAMESPACE}" \
      --for=condition=Ready pod/"${NAMENODE_POD}" \
      --timeout="${TIMEOUT_READY}s" >/dev/null
  log_pass "NameNode pod ${NAMENODE_POD} Running và Ready."
else
  log_fail "NameNode pod phase: ${NN_PHASE} (cần Running)"
fi

# ------------------------------------------------------------------
# [2] PVC đã Bound
# ------------------------------------------------------------------
echo ""
echo "--- [2/7] PersistentVolumeClaim NameNode ---"
NN_PVC="hdfs-namenode-data-hdfs-namenode-0"
PVC_STATUS=$(kubectl get pvc "${NN_PVC}" -n "${NAMESPACE}" \
    -o jsonpath='{.status.phase}' 2>/dev/null || echo "NotFound")
if [[ "${PVC_STATUS}" == "Bound" ]]; then
  log_pass "PVC ${NN_PVC} đã Bound."
else
  log_fail "PVC ${NN_PVC} ở trạng thái ${PVC_STATUS} (cần Bound)"
fi

# DataNode PVC
DN_PVC="hdfs-datanode-data-hdfs-datanode-0"
DN_PVC_STATUS=$(kubectl get pvc "${DN_PVC}" -n "${NAMESPACE}" \
    -o jsonpath='{.status.phase}' 2>/dev/null || echo "NotFound")
if [[ "${DN_PVC_STATUS}" == "Bound" ]]; then
  log_pass "PVC ${DN_PVC} đã Bound."
else
  log_fail "PVC ${DN_PVC} ở trạng thái ${DN_PVC_STATUS} (cần Bound)"
fi

# Dừng sớm nếu pod/PVC chưa sẵn sàng để tránh lỗi tiếp theo
if [[ "${FAILED}" -gt 0 ]]; then
  echo ""
  echo "Dừng lại vì pod hoặc PVC chưa sẵn sàng. Kiểm tra:"
  echo "  kubectl get pods,pvc -n ${NAMESPACE}"
  exit 1
fi

# ------------------------------------------------------------------
# [3] HDFS healthy — dfsadmin report
# ------------------------------------------------------------------
echo ""
echo "--- [3/7] HDFS health — dfsadmin report ---"
DFSREPORT=$(kubectl exec -n "${NAMESPACE}" "${NAMENODE_POD}" -- \
    hdfs dfsadmin -report 2>/dev/null || true)
echo "${DFSREPORT}"

if echo "${DFSREPORT}" | grep -Eq 'Live datanodes \([1-9][0-9]*\)'; then
  log_pass "HDFS healthy — có ít nhất một Live DataNode."
else
  log_fail "Không xác nhận được Live DataNode từ dfsadmin report."
fi

# ------------------------------------------------------------------
# [4] 6 data-lake paths tồn tại
# ------------------------------------------------------------------
echo ""
echo "--- [4/7] Data-lake layout ---"
REQUIRED_PATHS=(
  "/fraud-data/raw"
  "/fraud-data/cleaned"
  "/fraud-data/curated"
  "/fraud-data/late_events"
  "/fraud-data/checkpoints"
  "/fraud-data/models"
)

LS_OUTPUT=$(kubectl exec -n "${NAMESPACE}" "${NAMENODE_POD}" -- \
    hdfs dfs -ls /fraud-data 2>/dev/null || true)
echo "${LS_OUTPUT}"

for P in "${REQUIRED_PATHS[@]}"; do
  if echo "${LS_OUTPUT}" | grep -q "${P}"; then
    log_pass "Path tồn tại: ${P}"
  else
    log_fail "Path thiếu: ${P}  (chạy: bash infra/hdfs/setup_datalake.sh)"
  fi
done

if [[ "${FAILED}" -gt 0 ]]; then
  echo ""
  echo "Một hoặc nhiều data-lake paths thiếu. Chạy setup trước:"
  echo "  bash infra/hdfs/setup_datalake.sh"
  exit 1
fi

# ------------------------------------------------------------------
# [5] Upload file JSONL mẫu vào HDFS
# ------------------------------------------------------------------
echo ""
echo "--- [5/7] Upload JSONL mẫu vào HDFS ---"

compute_sha256() {
  local target="$1"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "${target}" | awk '{print $1}'
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "${target}" | awk '{print $1}'
  elif command -v python3 >/dev/null 2>&1; then
    python3 -c "import hashlib, sys; print(hashlib.sha256(open(sys.argv[1], 'rb').read()).hexdigest())" "${target}"
  elif command -v python >/dev/null 2>&1; then
    python -c "import hashlib, sys; print(hashlib.sha256(open(sys.argv[1], 'rb').read()).hexdigest())" "${target}"
  else
    echo "ERROR: Không tìm thấy sha256sum, shasum hoặc python để tính checksum!" >&2
    return 1
  fi
}

# Tạo file tạm trên host với nội dung chuẩn
TMPFILE=$(mktemp /tmp/hdfs_smoke_in_XXXXXX.jsonl 2>/dev/null || mktemp -t hdfs_smoke_in.XXXXXX.jsonl 2>/dev/null || mktemp)
# printf '%s\n': ghi SAMPLE_JSON + newline, kiểm soát chính xác bytes
printf '%s\n' "${SAMPLE_JSON}" > "${TMPFILE}"

# Tính SHA-256 của file gốc trên host
LOCAL_SHA=$(compute_sha256 "${TMPFILE}")
log_info "SHA-256 local  : ${LOCAL_SHA}"

# Copy file từ host vào NameNode pod
log_info "Copy file vào pod ${NAMENODE_POD}..."
kubectl cp "${TMPFILE}" "fraudstream/${NAMENODE_POD}:/tmp/smoke_test_input.jsonl"

# Upload từ pod vào HDFS (-f: overwrite nếu đã tồn tại — idempotent)
log_info "hdfs dfs -put vào ${HDFS_SMOKE_PATH}..."
kubectl exec -n "${NAMESPACE}" "${NAMENODE_POD}" -- \
    hdfs dfs -put -f /tmp/smoke_test_input.jsonl "${HDFS_SMOKE_PATH}"

# Giữ TMPFILE để so sánh checksum ở bước 7
log_pass "Upload hoàn thành: ${HDFS_SMOKE_PATH}"

# ------------------------------------------------------------------
# [6] List — xác nhận file có trong HDFS
# ------------------------------------------------------------------
echo ""
echo "--- [6/7] List /fraud-data/raw/ ---"
LIST_OUTPUT=$(kubectl exec -n "${NAMESPACE}" "${NAMENODE_POD}" -- \
    hdfs dfs -ls /fraud-data/raw 2>/dev/null || true)
echo "${LIST_OUTPUT}"

if echo "${LIST_OUTPUT}" | grep -q "smoke_test_hdfs.jsonl"; then
  log_pass "File smoke_test_hdfs.jsonl tồn tại trong /fraud-data/raw/"
else
  log_fail "File smoke_test_hdfs.jsonl KHÔNG tìm thấy trong /fraud-data/raw/"
fi

# ------------------------------------------------------------------
# [7] Cat + SHA-256 checksum verification
# ------------------------------------------------------------------
echo ""
echo "--- [7/7] Nội dung và SHA-256 checksum ---"

# Đọc lại từ HDFS vào file tạm thứ hai (tránh shell variable trimming)
TMPFILE_HDFS=$(mktemp /tmp/hdfs_smoke_out_XXXXXX.jsonl 2>/dev/null || mktemp -t hdfs_smoke_out.XXXXXX.jsonl 2>/dev/null || mktemp)
kubectl exec -n "${NAMESPACE}" "${NAMENODE_POD}" -- \
    hdfs dfs -cat "${HDFS_SMOKE_PATH}" 2>/dev/null > "${TMPFILE_HDFS}"

echo "Nội dung từ HDFS:"
cat "${TMPFILE_HDFS}"

# Tính SHA-256 của cả hai file — byte-perfect comparison
HDFS_SHA=$(compute_sha256 "${TMPFILE_HDFS}")
log_info "SHA-256 từ HDFS  : ${HDFS_SHA}"
log_info "SHA-256 local    : ${LOCAL_SHA}"

# Dọn dẹp
rm -f "${TMPFILE}" "${TMPFILE_HDFS}"

if [[ "${LOCAL_SHA}" == "${HDFS_SHA}" ]]; then
  log_pass "Checksum khớp — nội dung toàn vẹn."
else
  log_fail "Checksum KHÔNG khớp — nội dung bị thay đổi!"
fi

# ------------------------------------------------------------------
# [8] Persistence test (tùy chọn: --test-persistence)
# ------------------------------------------------------------------
if [[ "${1:-}" == "--test-persistence" ]]; then
  echo ""
  echo "--- [8/8] Persistence test (restart NameNode pod) ---"

  # Ghi lại UID của pod trước khi xóa
  UID_BEFORE=$(kubectl get pod "${NAMENODE_POD}" -n "${NAMESPACE}" \
      -o jsonpath='{.metadata.uid}' 2>/dev/null || echo "unknown")
  log_info "UID pod trước restart: ${UID_BEFORE}"

  # Xóa pod — StatefulSet sẽ tự tạo lại
  log_info "Xóa pod ${NAMENODE_POD} (StatefulSet sẽ tự recreate)..."
  kubectl delete pod "${NAMENODE_POD}" -n "${NAMESPACE}" --wait=false

  # Chờ pod cũ biến mất
  log_info "Chờ pod cũ terminate..."
  sleep 5

  # Chờ pod mới Ready
  log_info "Chờ pod mới Ready (timeout ${TIMEOUT_READY}s)..."
  kubectl wait --namespace "${NAMESPACE}" \
      --for=condition=Ready pod/"${NAMENODE_POD}" \
      --timeout="${TIMEOUT_READY}s" >/dev/null

  # Xác nhận đây là pod MỚI (UID đã thay đổi)
  UID_AFTER=$(kubectl get pod "${NAMENODE_POD}" -n "${NAMESPACE}" \
      -o jsonpath='{.metadata.uid}' 2>/dev/null || echo "unknown")
  log_info "UID pod sau restart: ${UID_AFTER}"

  if [[ "${UID_BEFORE}" != "${UID_AFTER}" ]]; then
    log_pass "Pod đã được recreate (UID thay đổi — đúng hành vi StatefulSet)."
  else
    log_fail "UID pod không thay đổi — pod có thể chưa bị recreate."
  fi

  # Đọc lại file từ HDFS sau khi pod restart
  log_info "Đọc lại ${HDFS_SMOKE_PATH} sau pod restart..."
  RESTART_OUTPUT=$(kubectl exec -n "${NAMESPACE}" "${NAMENODE_POD}" -- \
      hdfs dfs -cat "${HDFS_SMOKE_PATH}" 2>/dev/null || true)

  if echo "${RESTART_OUTPUT}" | grep -q "tx_hdfs_smoke_20261019_001"; then
    log_pass "Dữ liệu được bảo toàn 100% trên PVC sau pod restart."
    echo "  Nội dung đọc lại:"
    echo "  ${RESTART_OUTPUT}"
  else
    log_fail "Dữ liệu bị mất sau pod restart — PVC không mount đúng!"
    echo "  Output: ${RESTART_OUTPUT}"
  fi
else
  echo ""
  echo "  [Bỏ qua persistence test] Chạy --test-persistence để kiểm tra:"
  echo "  bash infra/hdfs/smoke_test.sh --test-persistence"
fi

# ------------------------------------------------------------------
# Tổng kết
# ------------------------------------------------------------------
echo ""
echo "============================================================"
echo " HDFS Smoke Test — Tổng kết"
echo "============================================================"
if [[ "${FAILED}" -eq 0 ]]; then
  echo -e " STATUS: \033[32mALL CHECKS PASSED\033[0m"
  echo "============================================================"
  exit 0
else
  echo -e " STATUS: \033[31mFAILED (${FAILED} kiểm tra thất bại)\033[0m"
  echo "============================================================"
  exit 1
fi
