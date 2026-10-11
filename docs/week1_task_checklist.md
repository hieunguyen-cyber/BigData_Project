# Task checklist Tuần 1 - Phạm Trung Hiếu (W1-06)

> Mỗi deliverable có owner, tiêu chí hoàn thành và **bằng chứng review**. Cột "Bằng chứng" điền link PR/commit khi xong.

> Cập nhật 2026-10-11: script EDA đã chạy trên ULB thật (284.807 dòng, 492 fraud), sinh 6 biểu đồ và `eda_stats.json`; kiểm thử đạt 18 passed. Số liệu và SHA-256 ở `eda_report.md` mục 10. Chưa chạy notebook riêng, simulator thật, review chéo hoặc profile IEEE-CIS thật. Hướng dẫn PowerShell: `README_WEEK1.md`.

## 1. Deliverables

| Deliverable | Owner | Tiêu chí hoàn thành | Trạng thái | Bằng chứng (PR/commit) | Reviewer |
|---|---|---|---|---|---|
| `scripts/eda_ulb.py`, `notebooks/eda_ulb.ipynb` | Phạm Trung Hiếu | Chạy lại được từ dữ liệu, cùng SHA-256 ⇒ cùng số liệu | ☑ Script chạy trên dữ liệu thật; notebook chưa chạy riêng | `eda_report.md` mục 10; chờ review | NTH |
| `docs/eda_report.md` + `docs/eda_stats.json` + `docs/figures/` | Phạm Trung Hiếu | ≥ 4 biểu đồ có tiêu đề/nhãn/chú thích/nhận xét; nêu imbalance, missing, duplicate, giới hạn | ☑ Sinh từ dữ liệu thật | `eda_stats.json`, 6 PNG; chờ review | NTH |
| `docs/feature_v1.md` | Phạm Trung Hiếu | Mỗi feature có nguồn, thời điểm tính, lý do; không leakage | ☑ Bản v1 đã soạn · ☐ Đã review | | NTH |
| `docs/simulator_test.md` (phần PTH) | Cả hai | Rule DQ, sample 4 loại event, checklist review | ☑ Phần PTH · ☐ Phần NTH | | NTH |
| `scripts/check_simulator_plausibility.py`, `tests/` | Cả hai | Test phân biệt valid/invalid/duplicate/late và 4 fraud pattern | ☑ Test đạt trên dữ liệu kiểm thử · ☐ Chạy trên output simulator thật | | NTH |

## 2. Việc cần làm sau khi nhận gói

1. [ ] Cài môi trường: `pip install -r requirements-eda.txt`
2. [x] Tải ULB theo `data/README.md`; chưa tải IEEE-CIS
3. [x] Chạy pytest và EDA ULB trên PowerShell theo `README_WEEK1.md`
4. [ ] Đọc lại `docs/eda_report.md`: kiểm tra nhận xét tự động có hợp lý, **bổ sung diễn giải nghiệp vụ** của riêng bạn nếu cần
5. [ ] Ghi các con số chính vào `docs/feature_v1.md` mục 8 (fraud rate thật) và `README` nếu muốn trích dẫn
6. [ ] Cập nhật `requirements.txt` + README mục 9 (matplotlib, pytest nếu chưa có)
7. [ ] Mở Pull Request (dùng `.github/PULL_REQUEST_TEMPLATE.md`), yêu cầu Nguyễn Trung Hiếu review
8. [ ] Review simulator của NTH bằng `check_simulator_plausibility.py`, điền cột "Kết quả" ở `simulator_test.md` mục 4.2

## 3. Kế hoạch theo ngày

| Ngày | Công việc | Mốc kiểm tra | Hoàn thành |
|---|---|---|---|
| 1 | Tải dữ liệu, chạy `eda_ulb.py` lần đầu, đọc các câu hỏi EDA (Q1–Q7) | Dữ liệu tải được, SHA-256 ghi lại; thống nhất fraud scenarios | ☐ |
| 2 | Thống kê schema, missing, duplicate (mục 3 báo cáo); trao đổi schema với NTH | Review schema chung | ☐ |
| 3 | Hình 1–2, phân tích imbalance và amount | Đọc nhận xét tự động, kiểm tra với số trong `eda_stats.json` | ☐ |
| 4 | Hình 3–4, chạy profile IEEE-CIS và rà mapping (mục 7) | Đủ 4 biểu đồ + mapping | ☐ |
| 5 | Hoàn thiện feature candidates, metrics; đề xuất rule DQ gửi NTH | Test cases có sample | ☐ |
| 6 | Kiểm tra leakage (test "truncate" mục 7.4 feature_v1), hoàn thiện báo cáo; review simulator | Review chéo | ☐ |
| 7 | Sửa theo review, chốt feature v1, demo + checklist cuối tuần | Demo: scenario → event → tín hiệu → feature | ☐ |

## 4. Checklist nghiệm thu (phần của Phạm Trung Hiếu)

- [x] EDA có tối thiểu 4 biểu đồ cùng nhận xét (Hình 1–4; Hình 5–6 là bổ sung).
- [x] Báo cáo EDA nêu class imbalance, missing values, duplicate records và giới hạn dữ liệu.
- [ ] Feature v1 có công thức/mô tả và kiểm tra data leakage.
- [ ] Có sample/test cho valid, invalid, duplicate và late event (phối hợp với NTH).
- [ ] Deliverable được review chéo trước khi merge.

## 5. Chuẩn bị demo cuối tuần

Demo theo Definition of Done: chọn một scenario → tạo event JSON hợp schema → chỉ ra tín hiệu fraud event đó thể hiện → liên hệ feature v1.
Bảng liên hệ có sẵn ở `docs/simulator_test.md` mục 5. Ví dụ: `velocity_fraud` → `tx_count_5m ≥ 5` → rule velocity → alert `REVIEW`/`BLOCK`.
