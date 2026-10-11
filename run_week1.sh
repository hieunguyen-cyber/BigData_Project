#!/usr/bin/env bash
# Chạy toàn bộ phần việc tự động của Phạm Trung Hiếu - Tuần 1.
# Dùng: bash run_week1.sh [đường_dẫn_creditcard.csv] [đường_dẫn_train_transaction.csv]
set -euo pipefail
cd "$(dirname "$0")"
ULB="${1:-data/raw/creditcard.csv}"
IEEE="${2:-}"

echo "== 1/3 Kiểm thử mã =="
python -m pytest tests -q

echo "== 2/3 Chạy EDA trên dữ liệu thật =="
if [ -n "$IEEE" ]; then
  python scripts/eda_ulb.py --data "$ULB" --ieee-transaction "$IEEE"
else
  python scripts/eda_ulb.py --data "$ULB"
fi

echo "== 3/3 Xong =="
echo "Kiểm tra: docs/eda_report.md, docs/eda_stats.json, docs/figures/*.png"
echo "Tiếp theo: đọc lại nhận xét tự động, bổ sung diễn giải nghiệp vụ nếu cần, rồi mở Pull Request."
