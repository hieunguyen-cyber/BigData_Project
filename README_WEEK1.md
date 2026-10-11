# Tuần 1 — Phạm Trung Hiếu: Data analysis & ML foundation

## Kết quả đã chạy ngày 2026-10-11

EDA ULB trên dữ liệu thật: 284.807 giao dịch, 492 fraud (0,1727%), không missing value, 1.081 dòng trùng lặp hoàn toàn. Có 1.825 giao dịch Amount = 0, gồm 27 fraud. Kiểm thử: **18 passed**.

- `docs/eda_report.md`: báo cáo, nhận xét, mapping IEEE-CIS và giới hạn dữ liệu.
- `docs/eda_stats.json`: số liệu, SHA-256 và phiên bản môi trường.
- `docs/figures/`: 6 biểu đồ PNG; giữ nguyên đường dẫn khi nộp.
- `docs/feature_v1.md`: công thức, nguồn dữ liệu, thời điểm tính, metrics, leakage và chia tập.
- `docs/simulator_test.md`: đề xuất data-quality rules và hướng dẫn review simulator.
- `docs/week1_task_checklist.md`: trạng thái hoàn thành và việc còn chờ phối hợp.
- `scripts/`, `tests/`, `notebooks/eda_ulb.ipynb`: mã để chạy lại và kiểm thử.

## Chạy trên Windows PowerShell

Từ gốc repo, dùng môi trường EDA riêng. `requirements.txt` của hệ thống dùng Python 3.12/NumPy 1.26; không cài chồng hai bộ requirements vào cùng môi trường.

```powershell
py -3.13 -m venv .venv-eda
.\.venv-eda\Scripts\python.exe -m pip install -r requirements-eda.txt
$env:PYTHONIOENCODING = "utf-8"
.\.venv-eda\Scripts\python.exe -m pytest tests -q
# Tải dữ liệu theo data/README.md
.\.venv-eda\Scripts\python.exe scripts\eda_ulb.py --data data\raw\creditcard.csv
```

Đã kiểm thử với Python 3.13.1, NumPy 2.5.2, pandas 2.2.3, matplotlib 3.10.0. `pytest.ini` dùng temp trong project để tránh lỗi quyền Windows. Khi đã kích hoạt môi trường phù hợp, dùng `python` thay đường dẫn đầy đủ. Trên Bash: `bash run_week1.sh data/raw/creditcard.csv`.

Tuỳ chọn profile IEEE-CIS:

```powershell
python scripts/eda_ulb.py --data data/raw/creditcard.csv --ieee-transaction data/raw/ieee/train_transaction.csv
```

## Việc còn chờ trước nghiệm thu

- Chưa có dữ liệu IEEE-CIS thật; mục 7 báo cáo là mapping thiết kế, chưa có profile thực nghiệm.
- Nguyễn Trung Hiếu cần review feature v1 và hoàn thiện phần [NTH] trong `docs/simulator_test.md`.
- Phạm Trung Hiếu cần chạy checker trên output simulator thật và ghi bằng chứng review. Test dữ liệu kiểm thử không thay thế bước này.
- Hai bên cần chốt DQ-02 (Amount = 0); ngưỡng rule vẫn là đề xuất chưa hiệu chỉnh.
- ULB không có thẻ/thiết bị/merchant nên chưa kiểm chứng feature hành vi trên dữ liệu thật. Chưa huấn luyện mô hình. Notebook chưa chạy riêng.

Không commit dữ liệu thô/CSV, môi trường Python hoặc cache. Bổ sung link PR/commit sau review; không đánh dấu review hoàn thành trước khi thực hiện.
