# Dữ liệu cho EDA (không commit lên Git)

`data/raw/` nằm trong `.gitignore`. Mỗi thành viên tự tải dữ liệu; kết quả được kiểm chứng bằng SHA-256 ghi trong
`docs/week_1/eda/eda_report.md` mục 10 và `docs/week_1/eda/eda_stats.json` (trường `meta.sha256`).

## ULB Credit Card Fraud Detection (bắt buộc)

Nguồn: https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud

```bash
pip install kaggle                       # cần ~/.kaggle/kaggle.json (API token của tài khoản Kaggle)
kaggle datasets download -d mlg-ulb/creditcardfraud -p data/raw --unzip
# kết quả: data/raw/creditcard.csv
```
Hoặc tải thủ công trên trang Kaggle, giải nén `creditcard.csv` vào `data/raw/`.

Schema mong đợi: `Time`, `V1…V28`, `Amount`, `Class` (31 cột). Script sẽ báo lỗi nếu thiếu cột.

## IEEE-CIS Fraud Detection (tuỳ chọn, để profile và mapping)

Nguồn: https://www.kaggle.com/competitions/ieee-fraud-detection/data (cần chấp nhận luật cuộc thi)

```bash
kaggle competitions download -c ieee-fraud-detection -p data/raw/ieee
unzip data/raw/ieee/ieee-fraud-detection.zip -d data/raw/ieee
# cần: data/raw/ieee/train_transaction.csv (≈ 590 nghìn dòng, ≈ 394 cột; đọc theo chunk nên không tốn nhiều RAM)
```

## Ghi chú giấy phép
Dữ liệu thuộc điều khoản của Kaggle/chủ sở hữu; chỉ dùng cho mục đích học tập, không đưa dữ liệu thô vào repo công khai.
