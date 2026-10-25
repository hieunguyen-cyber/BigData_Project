# Review chéo kỹ thuật W2 — FraudStream

> Thực hiện ngày 25/10/2026 trên Kind `fraudstream-cluster`. Đây là review
> mã/manifests/scripts độc lập kèm chạy runtime; không thay thế việc ba owner
> xác nhận review chéo nội bộ của nhóm.

## Phạm vi và cách review

| Lớp | Nội dung review | Cách đối chiếu |
|---|---|---|
| Platform | Kind pin version, namespace, quota, CoreDNS, StorageClass/PVC | `check_preflight.sh` và Kind v1.34.11 |
| Kafka | KRaft, topic config, key/value, persistence | `bootstrap_topics.sh`, `smoke_test.py --test-persistence` |
| Cassandra | PVC, CQL access pattern, readiness/liveness, persistence | manifest, `nodetool`, `smoke_test.sh --test-persistence` |
| HDFS | NameNode/DataNode, PVC, live DataNode, layout, I/O/persistence | setup/smoke test, `dfsadmin -report` |
| Integration | Schema/key giữ nguyên từ simulator qua Kafka tới HDFS raw | `integration_smoke.py`, 100 events + checksum |
| Operability | Tái lập ở máy khác, output lỗi rõ, dọn cluster an toàn | `Makefile`, deployment/evidence docs |

## Kết quả và các điểm đã xử lý

| Mức | Phát hiện | Hành động / kết quả |
|---|---|---|
| P1 | Bootstrap HDFS có thể tạo paths khi DataNode chưa live. | Đã đổi thành hard gate từ `dfsadmin -report`; thiếu DataNode trả lỗi. |
| P1 | Cassandra liveness probe tìm literal `${POD_IP}` vì nằm trong dấu nháy đơn. | Đã sửa để shell mở rộng biến. Sau rollout pod giữ `Ready`, `restartCount=0`. |
| P2 | Full verifier trước đây chỉ đủ mức NameNode/path. | Đã kiểm tra DataNode, 2 PVC và HDFS write/read/checksum ở Bash; PowerShell có HDFS I/O riêng. |
| P2 | Batch fixture khai báo thứ tự classification không khớp thứ tự record. | Đã đồng bộ thứ tự `valid → invalid → duplicate → late`. |
| P2 | Thiếu entry point tái lập toàn bộ W2. | Đã thêm `Makefile` với target deploy, bootstrap, verify, integration, persistence và destroy có xác nhận. |

## Nghiệm thu runtime

- Một node Kind Ready; bốn workload W2 Ready, bốn PVC Bound.
- Kafka gửi/đọc 100/100 event hợp schema và đúng `card_id_hash`; persistence
  pass khi broker/client đổi UID nhưng PVC/PV giữ nguyên.
- Cassandra `UN`; CQL insert/select `APPROVE` và `BLOCK` còn sau recreate pod.
- HDFS có 6 paths, `Live datanodes (1)`, checksum JSONL khớp và file còn sau
  NameNode recreate.
- Simulator → Kafka → HDFS tạo raw JSONL 100 event hợp schema.
- Cả `verify_infra.sh` và `verify_infra.ps1` đã chạy qua các kiểm tra W2 trên
  cluster này.

## Tái lập bằng Makefile

`make reproduce` đã được chạy trên chính cluster Kind này. Target tự kiểm tra
toolchain, tái sử dụng/tạo cluster, chạy preflight, apply manifests, bootstrap
Kafka/HDFS/Cassandra, full Bash verifier và integration smoke. Lần chạy tạo
thêm `/fraud-data/raw/integration_8ff18fcefdae.jsonl`; bốn workload vẫn Ready
sau cleanup client pods. `make persistence` chạy riêng ba kiểm tra restart để
tránh làm luồng tái lập thường ngày kéo dài không cần thiết.

Evidence định lượng, timestamp và checksum nằm trong
[`week_2_test_evidence.md`](week_2_test_evidence.md).

## Giới hạn đã biết

1. Kafka và HDFS dùng một broker/DataNode, replication factor `1`; chỉ phù hợp
   demo local, không chịu mất node hay cluster.
2. Kind local-path volume chỉ bền qua pod restart trong cùng cluster; `make
   destroy CONFIRM=1` xóa cluster và data local.
3. Spark batch/streaming chưa là một phần của W2; raw HDFS là contract đầu vào
   cho giai đoạn tiếp theo.

## Kết luận review

W2 đạt tiêu chí kỹ thuật để chuyển sang batch/streaming: deploy tái lập,
service stateful có PVC, kiểm tra độc lập và luồng integration đã có evidence.
Trước merge cuối cùng, Hoàng, Đạt và Khôi vẫn nên đọc/acknowledge thay đổi
thuộc phần họ sở hữu theo checklist nhóm.
