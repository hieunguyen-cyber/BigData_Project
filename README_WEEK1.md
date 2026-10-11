# Tuần 1 — Phạm Trung Hiếu: Data analysis & ML foundation

## Kết quả đã chạy ngày 2026-10-11

EDA ULB trên dữ liệu thật: 284.807 giao dịch, 492 fraud (0,1727%), không missing value, 1.081 dòng trùng lặp hoàn toàn. Có 1.825 giao dịch Amount = 0, gồm 27 fraud. Kiểm thử tích hợp Tuần 1: **27 passed**.

- `docs/eda_report.md`: báo cáo, nhận xét, mapping IEEE-CIS và giới hạn dữ liệu.
- `docs/eda_stats.json`: số liệu, SHA-256 và phiên bản môi trường.
- `docs/figures/`: 6 biểu đồ PNG; giữ nguyên đường dẫn khi nộp.
- `docs/feature_v1.md`: công thức, nguồn dữ liệu, thời điểm tính, metrics, leakage và chia tập.
- `docs/simulator_test.md`: đề xuất data-quality rules và hướng dẫn review simulator.
- `docs/week1_task_checklist.md`: trạng thái hoàn thành và việc còn chờ phối hợp.
- `scripts/`, `tests/`, `notebooks/eda_ulb.ipynb`: mã để chạy lại và kiểm thử.

## Chạy trên Windows PowerShell

Từ gốc repo, dùng môi trường chuẩn duy nhất của dự án. Phiên bản Python packages được quản lý trong `requirements.txt` và tóm tắt ở README mục 9.

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
$env:PYTHONIOENCODING = "utf-8"
.\.venv\Scripts\python.exe -m pytest tests -q
# Tải dữ liệu theo data/README.md
.\.venv\Scripts\python.exe scripts\eda_ulb.py --data data\raw\creditcard.csv
```

`pytest.ini` dùng temp trong project để tránh lỗi quyền Windows. Khi đã kích hoạt môi trường phù hợp, dùng `python` thay đường dẫn đầy đủ. Trên Bash: `bash run_week1.sh data/raw/creditcard.csv`.

Tuỳ chọn profile IEEE-CIS:

```powershell
python scripts/eda_ulb.py --data data/raw/creditcard.csv --ieee-transaction data/raw/ieee/train_transaction.csv
```

## Kết quả review và giới hạn dữ liệu

- Nguyễn Trung Hiếu đã review schema/feature v1 và hoàn thiện simulator; toàn bộ 5 scenarios đều vượt qua plausibility checker.
- DQ-02 được chốt: `amount <= 0` là `invalid` trong transaction schema của FraudStream. Quyết định này không phủ nhận việc ULB có giao dịch Amount = 0; ULB là dataset nghiên cứu ẩn danh, còn simulator mô tả giao dịch thanh toán thành công/thất bại có giá trị dương.
- ULB không có thẻ/thiết bị/merchant thật, nên feature hành vi được kiểm thử bằng simulator. IEEE-CIS giữ vai trò mapping/mở rộng khi có dữ liệu được cấp theo điều khoản Kaggle.

Không commit dữ liệu thô/CSV, môi trường Python hoặc cache.
