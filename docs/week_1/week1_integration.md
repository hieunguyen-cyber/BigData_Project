# Tích hợp deliverables Tuần 1

## Trạng thái merge

| Nguồn | Commit nguồn | Trạng thái trên `main` |
|---|---|---|
| `hieupham_w1` | `4fd4ecb` - EDA và feature deliverables của Phạm Trung Hiếu | Đã merge qua `12bd61a` |
| `main` | `69f48bd` - simulator, data contract và kiểm thử của Nguyễn Trung Hiếu | Đã tích hợp |

Không còn remote branch nào có thay đổi chưa nằm trong `main` tại thời điểm cập nhật tài liệu này.

## Deliverables đã tích hợp

| Nhóm công việc | Tài liệu / mã nguồn | Kết quả |
|---|---|---|
| EDA và ML foundation | `docs/week_1/eda/eda_report.md`, `docs/week_1/eda/feature_v1.md`, `scripts/eda_ulb.py` | Phân tích ULB, feature v1, metrics và kiểm tra data leakage |
| Fraud simulator | `data-generator/producer.py`, `data-generator/scenarios.py` | Sinh normal traffic và 4 fraud scenarios với seed tái lập |
| Data contract | `docs/general/data_dictionary.md`, `data-generator/schema.py` | Schema 12 trường, validation và phân loại valid/invalid/duplicate/late |
| System design | `docs/general/problem_and_architecture.md` | Kiến trúc Lambda, topics, data flow và quyết định kỹ thuật W1 |
| Kiểm thử | `tests/`, `scripts/check_simulator_plausibility.py` | 27 tests pass; 5 scenarios vượt qua plausibility checker |
| Môi trường chung | `requirements.txt`, README mục 9 | Một baseline Python dependency dùng chung cho nhóm |

## Cách xác minh lại

```bash
python -m pip install -r requirements.txt
python -m pytest tests -q

python data-generator/producer.py --scenario velocity_fraud --seed 42 --rate 20 --duration 60 \
  --output out/velocity_fraud.jsonl
python scripts/check_simulator_plausibility.py \
  --file out/velocity_fraud.jsonl --expect velocity_fraud
```

Lệnh kiểm thử phải báo toàn bộ tests pass; checker phải trả về `PASS` và exit code `0`.

## Tài liệu liên quan

- [Mục tiêu Tuần 1](week_1_target.md)
- [EDA report](eda_report.md)
- [Feature v1](feature_v1.md)
- [Simulator test và data-quality rules](simulator_test.md)
- [Data dictionary](data_dictionary.md)
- [Bài toán và kiến trúc](problem_and_architecture.md)
