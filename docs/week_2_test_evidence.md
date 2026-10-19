# Bằng chứng Kiểm thử Hạ tầng Tuần 2 (FraudStream)

Tài liệu ghi lại kết quả smoke test, thông số môi trường, mức sử dụng tài nguyên (resource footprint) và các giới hạn kỹ thuật của hệ thống hạ tầng Tuần 2.

**Phạm vi bằng chứng:** mục 1–3 giữ snapshot Windows/Cassandra đã có của
Khôi. Mục 4 là kết quả Kafka thực tế của Hoàng trên Mac ngày **2026-10-19**
(UTC+7). Mục 5 vẫn là mẫu HDFS chờ Đạt điền. Không gộp các snapshot này
thành một lần kiểm thử end-to-end đã hoàn tất.

---

## 1. Môi trường Thực thi & Phiên bản Công cụ

Bảng phiên bản công cụ thực tế sử dụng trong kiểm thử:

| Thành phần | Phiên bản thực tế | Ghi chú |
|---|---|---|
| **Hệ điều hành** | Windows 11 Pro (WSL 2 backend) | Kernel 5.15+ |
| **Docker Desktop** | 27.5.1 | Allocated 10 GB RAM, 4 CPUs |
| **Kubernetes (Kind)** | v1.29.2 (`kindest/node:v1.29.2`) | Kind v0.22.0 |
| **kubectl** | Client v1.30+ | Tương thích k8s v1.29 |
| **Apache Cassandra** | 4.1 (`cassandra:4.1`) | Single-node local StatefulSet |
| **Apache Kafka** | Định hướng ban đầu: 3.7+ | Kết quả thực tế Kafka 3.9.1 trên Mac xem mục 4 |
| **Apache HDFS** | Mục tiêu: 3.3+ (Hadoop) | Chưa xác minh; mục 5 còn là mẫu |

---

## 2. Bằng chứng Preflight & Nền tảng Kubernetes

### 2.1. Trạng thái Node và Namespace
```text
$ kubectl get nodes -o wide
NAME                                STATUS   ROLES           AGE   VERSION   INTERNAL-IP   OS-IMAGE       KERNEL-VERSION
fraudstream-cluster-control-plane   Ready    control-plane   12m   v1.29.2   172.18.0.2    Ubuntu 22.04   5.15.153.1-microsoft-standard-WSL2

$ kubectl get namespaces
NAME              STATUS   AGE
default           Active   12m
fraudstream       Active   11m
kube-node-lease   Active   12m
kube-public       Active   12m
kube-system       Active   12m
```

### 2.2. Trạng thái StorageClass và PersistentVolumeClaim
```text
$ kubectl get storageclass
NAME                 PROVISIONER             RECLAIMPOLICY   VOLUMEBINDINGMODE      ALLOWVOLUMEEXPANSION   AGE
standard (default)   rancher.io/local-path   Delete          WaitForFirstConsumer   false                  12m

$ kubectl get pvc -n fraudstream
NAME                          STATUS   VOLUME                                     CAPACITY   ACCESS MODES   STORAGECLASS   AGE
cassandra-data-cassandra-0    Bound    pvc-8b7c8449-3129-4592-8084-219ecba342e1   5Gi        RWO            standard       8m
```

---

## 3. Bằng chứng Kiểm thử Apache Cassandra (Lê Xuân Nhật Khôi)

### 3.1. Trạng thái Node Health (`nodetool status`)
Cassandra node ở trạng thái `UN` (Up Normal), sở hữu 100% token ring cục bộ:
```text
$ kubectl exec -n fraudstream cassandra-0 -- nodetool status
Datacenter: datacenter1
=======================
Status=Up/Down
|/ State=Normal/Leaving/Joining/Moving
--  Address      Load        Tokens  Owns (effective)  Host ID                               Rack
UN  10.244.0.5   315.42 KiB  16      100.0%            1c06d8cf-9a3b-486a-8671-89e47264a781  rack1
```

### 3.2. Khởi tạo Keyspace và Schema (`01-bootstrap.cql`)
Thực thi bootstrap hoàn toàn thành công, đảm bảo tính idempotent:
```text
$ kubectl exec -i -n fraudstream cassandra-0 -- cqlsh < infra/cassandra/cql/01-bootstrap.cql
Warnings :
To create keyspaces with simple strategy is not recommended for production use.
```

Kiểm tra định nghĩa bảng qua `cqlsh`:
```sql
cqlsh> DESCRIBE KEYSPACE fraudstream;

CREATE KEYSPACE fraudstream WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;

CREATE TABLE fraudstream.transaction_risk (
    card_id_hash text,
    event_time timestamp,
    transaction_id text,
    amount decimal,
    channel text,
    country text,
    created_at timestamp,
    currency text,
    customer_id text,
    decision text,
    device_id_hash text,
    ip_hash text,
    merchant_id text,
    model_version text,
    risk_score double,
    transaction_status text,
    trigger_reasons list<text>,
    PRIMARY KEY ((card_id_hash), event_time, transaction_id)
) WITH CLUSTERING ORDER BY (event_time DESC, transaction_id ASC);
```

### 3.3. Smoke Test: Insert & Select (Không dùng `ALLOW FILTERING`)
Truy vấn trực tiếp trên partition key `card_id_hash` và điều kiện thời gian `event_time >= ...`:

```text
cqlsh:fraudstream> SELECT card_id_hash, event_time, transaction_id, risk_score, decision, trigger_reasons
                   FROM fraudstream.transaction_risk
                   WHERE card_id_hash = 'sha256:7f83b1657ff1fc53b92dc18148a1d65dfc2d4b1fa3d677284addd200126d9069'
                     AND event_time >= '2026-10-18T14:00:00Z';

 card_id_hash                                                       | event_time                      | transaction_id          | risk_score | decision | trigger_reasons
--------------------------------------------------------------------+---------------------------------+-------------------------+------------+----------+---------------------------------------------------------------------------------
 sha256:7f83b1657ff1fc53b92dc18148a1d65dfc2d4b1fa3d677284addd200126d9069 | 2026-10-18 14:35:00.000000+0000 | tx_smoke_20261018_0002  |       0.89 |    BLOCK | ['high_value_anomaly: amount_vs_avg_7d >= 5', 'velocity_fraud: tx_count_5m >= 5']
 sha256:7f83b1657ff1fc53b92dc18148a1d65dfc2d4b1fa3d677284addd200126d9069 | 2026-10-18 14:30:00.000000+0000 | tx_smoke_20261018_0001  |       0.15 |  APPROVE |                                                              ['normal_pattern']

(2 rows)
```
> [!NOTE]
> Truy vấn hoàn tất trong < 5ms mà **không cần `ALLOW FILTERING`** nhờ thiết kế Partition Key `((card_id_hash), event_time, transaction_id)`.

### 3.4. Kiểm tra Cơ chế Bền vững Dữ liệu (Persistence Test)
Quy trình kiểm thử xóa pod:
1. Ghi dữ liệu mẫu `tx_smoke_20261018_0001` vào bảng `transaction_risk`.
2. Xóa pod: `kubectl delete pod cassandra-0 -n fraudstream`.
3. Chờ StatefulSet tự động tạo lại pod mới và mount lại cùng Persistent Volume.
4. Chạy lại truy vấn SELECT:
```text
$ kubectl exec -n fraudstream cassandra-0 -- cqlsh -e "SELECT transaction_id, decision FROM fraudstream.transaction_risk WHERE card_id_hash = 'sha256:7f83b1657ff1fc53b92dc18148a1d65dfc2d4b1fa3d677284addd200126d9069' AND event_time >= '2026-10-18T14:00:00Z';"

 transaction_id          | decision
-------------------------+----------
 tx_smoke_20261018_0002  |    BLOCK
 tx_smoke_20261018_0001  |  APPROVE

(2 rows)
```
**Kết luận**: Dữ liệu được bảo toàn 100% trên PVC, không bị mất sau khi restart pod.

---

## 4. Bằng chứng Kiểm thử Kafka — Nguyễn Minh Hoàng

### 4.1. Môi trường thực tế trên Mac

| Thành phần | Giá trị đã ghi nhận |
|---|---|
| Máy | macOS 27.0, Apple Silicon arm64, RAM 24 GiB |
| Docker Engine | 29.3.1; Docker VM có 4 CPU, 7.751 GiB RAM |
| Kind / Kubernetes / kubectl | 0.33.0 / 1.34.11 / 1.34.1 |
| Kafka | `apache/kafka:3.9.1`, 1 node KRaft broker/controller |
| Context / namespace | `kind-fraudstream-cluster` / `fraudstream` |
| Broker | `kafka-0`: 1/1 Running, Ready |
| PVC | `kafka-data-kafka-0`: Bound, 5 GiB, StorageClass `standard` |
| Endpoint client nội bộ | `kafka.fraudstream.svc.cluster.local:9092` |
| Resource requests / limits | 250m CPU + 512 MiB / 1 CPU + 1536 MiB |
| JVM heap | `-Xms256m -Xmx512m` |

Phiên bản node 1.34.11 được truyền bằng `kind create cluster --image`
theo deployment guide để khớp kubectl trên Mac. File Kind chung vẫn ghi
1.29.2. Chưa đo CPU/RAM peak; requests/limits là cấu hình, không phải số đo.

### 4.2. Topic bootstrap và chạy lại

`bash infra/kafka/bootstrap_topics.sh` chạy hai lần liên tiếp đều exit 0,
kết thúc bằng `PASS: all 4 Kafka topics are ready.` Lần hai giữ nguyên
TopicId và không tạo lại topic.

| Topic | Partitions | Replication factor | retention.ms |
|---|---:|---:|---:|
| `transactions_raw` | 3 | 1 | 604800000 |
| `transactions_invalid` | 1 | 1 | 259200000 |
| `fraud_alerts` | 3 | 1 | 604800000 |
| `fraud_feedback` | 1 | 1 | 2592000000 |

### 4.3. Producer/consumer — 100 events đúng key và JSON

```text
$ python3 infra/kafka/smoke_test.py
PASS: 100/100 events received; JSON, schema and Kafka keys match.
```

Script dùng `scenarios.generate` của simulator Tuần 1, gửi bằng Kafka CLI
từ client pod rồi đọc lại qua Service DNS. Python trên máy kiểm tra đủ
100 transaction ID, không trùng/thiếu, JSON không đổi, validator chuẩn
không có lỗi và Kafka key bằng `card_id_hash`.

Kết quả chính được ghi trực tiếp tại đây. Báo cáo JSON từng lượt nằm
trong `out/` (gitignored), không cần commit riêng file log/JSON.
Run ID `3fd69ae21880`, lúc **01:18:04 ngày 2026-10-19 (UTC+7)**.
Offsets trước/sau trên partitions 0/1/2: `(24,44,32)` → `(48,88,64)`,
tăng tổng cộng 100 messages. Timestamps trong JSON được giữ nguyên UTC.

### 4.4. Persistence — tạo lại cả broker và client

```text
$ python3 infra/kafka/smoke_test.py --test-persistence
PASS: 100/100 events received; JSON, schema and Kafka keys match.
PASS: data preserved after broker and client pod recreation; same PVC/PV reused.
```

Run ID `8559187b416c`, lúc **01:22:19 ngày 2026-10-19 (UTC+7)**.
Gửi 100 events **trước** khi xóa pod; sau khi broker và client được tạo
lại, đọc lại đúng các events đó, không gửi lại để làm đầy dữ liệu.

| Đối tượng | UID trước | UID sau |
|---|---|---|
| Broker pod | `d59c0b5c-4a7e-4c90-8872-bffadc397aee` | `1061e00b-fb51-4676-9391-10d556b3ea29` |
| Client pod | `8a261383-800c-48c3-8fc0-0d24e6fcfc4b` | `a712cd9a-31a6-4bf7-96e0-5a8e4e99613b` |

PVC UID giữ nguyên `c520e683-c9ea-4b9b-a2d2-6b7dfdbb431d` và PV giữ nguyên
`pvc-c520e683-c9ea-4b9b-a2d2-6b7dfdbb431d`. Offsets `(48,88,64)` →
`(72,132,96)`, tăng 100. Các UID/offsets này được trích từ báo cáo của
lượt persistence `8559187b416c` trong `out/`.

### 4.5. Tích hợp xác minh và giới hạn

`bash infra/scripts/verify_infra.sh --kafka-only` kiểm tra pod Ready,
PVC Bound, bốn topics đúng partition/RF/retention rồi gửi–đọc 100 events.
Đã chạy thành công với exit 0:

```text
PASS: Kafka pod kafka-0 is Running and Ready.
PASS: Kafka PVC kafka-data-kafka-0 is Bound.
PASS: Kafka health and all 4 topic configurations verified.
PASS: 100/100 events received; JSON, schema and Kafka keys match.
```

Kiểm tra chỉ đọc: `python3 infra/kafka/smoke_test.py --check-only`.
Toàn bộ health/config, smoke và persistence dùng chung `smoke_test.py`;
không cần verifier riêng. Các file chính là `kafka.yaml`,
`bootstrap_topics.sh`, `smoke_test.py` trong `infra/kafka/`.

Kiểm tra lại sau khi thu gọn ngày 2026-10-19:

| Kiểm tra | Kết quả |
|---|---|
| Script chung `--kafka-only`, run `277c7abb6118` | Exit 0; health/config PASS; 100/100 events đúng JSON/schema/key |
| Persistence, run `13315aaf05b4` | Exit 0; 100/100 events còn nguyên sau tạo lại client/broker; cùng PVC/PV |
| Cú pháp Bash/Python và `git diff --check` | PASS |

Đã kiểm tra lỗi thiếu broker bằng biến `KAFKA_POD=kafka-intentionally-missing`:
script trả exit 1 và báo `NotFound`, không báo PASS giả.
Các tình huống pod chưa Ready, PVC Pending, thiếu topic và sai retention
được mô phỏng tại hàm CLI và đều trả exit 1. Bộ test Python Tuần 1 chạy
lại đạt **27 passed**.

Script chung ở chế độ đầy đủ vẫn cần Cassandra/HDFS của Khôi/Đạt.
Hai dịch vụ này chưa được triển khai/kiểm chứng trên cluster Mac của lần
chạy Kafka này. Bản PowerShell đã nối vào cùng smoke test Python,
nhưng chưa được chạy trên Windows trong lần kiểm thử này.
Chạy script chung đầy đủ: phần Kafka PASS, toàn script exit 1 vì thiếu
Cassandra; HDFS hiện báo WARN theo phần kiểm tra cũ:

```text
[FAIL] Cassandra pod cassandra-0 not found in namespace fraudstream!
[PASS] Kafka pod/PVC/topics and 100-event producer-consumer test passed.
[WARN] HDFS NameNode not yet deployed
STATUS: FAILED (1 checks failed)
```

Kafka local dùng RF=1, PLAINTEXT nội bộ, chưa có listener cho laptop.
Persistence chỉ chứng minh dữ liệu tồn tại qua tạo lại pod trên cùng
cluster/PVC, không chứng minh chịu mất node/cluster hoặc high availability.
Chưa benchmark throughput, latency hay resource peak.

### 4.6. Trạng thái bàn giao

- [x] Manifests Kafka, Service DNS, PVC, probes và resource settings.
- [x] Bootstrap bốn topics đúng cấu hình, chạy lại an toàn.
- [x] Smoke test 100 events: đúng schema/JSON/key và ID.
- [x] Tạo lại client/broker, giữ dữ liệu trên cùng PVC/PV.
- [x] Tài liệu deploy/verify/teardown/troubleshooting và kết quả kiểm thử tại đây.
- [x] Tích hợp kiểm tra Kafka vào script chung; có chế độ Kafka riêng.
- [ ] Khôi hoặc Đạt review chéo và dựng/chạy lại theo tài liệu trước merge.

---

## 5. Bằng chứng Kiểm thử HDFS (Mẫu cho Lê Trọng Đạt)

- **Pod status**: `hdfs-namenode-0` (Running), `hdfs-datanode-0` (Running).
- **Data Lake layout**:
  ```text
  $ hdfs dfs -ls /fraud-data
  drwxr-xr-x   - root supergroup          0 2026-10-18 15:00 /fraud-data/checkpoints
  drwxr-xr-x   - root supergroup          0 2026-10-18 15:00 /fraud-data/cleaned
  drwxr-xr-x   - root supergroup          0 2026-10-18 15:00 /fraud-data/curated
  drwxr-xr-x   - root supergroup          0 2026-10-18 15:00 /fraud-data/late_events
  drwxr-xr-x   - root supergroup          0 2026-10-18 15:00 /fraud-data/models
  drwxr-xr-x   - root supergroup          0 2026-10-18 15:00 /fraud-data/raw
  ```
- **I/O Test**: Upload file `sample_event.json` vào `/fraud-data/raw/`, đọc lại bằng `hdfs dfs -cat` và so khớp MD5 checksum khớp 100%.

---

## 6. Mức Sử Dụng Tài Nguyên (Resource Footprint)

Bảng dưới giữ cấu trúc tham khảo ban đầu. Các giá trị peak của Cassandra/HDFS
chưa được đo lại trên Mac của Hoàng; HDFS vẫn chờ deliverable của Đạt.
Không coi bảng này là phép đo tổng tài nguyên end-to-end. Requests/PVC
Kafka khớp cấu hình thực tế tại mục 4; peak Kafka chưa đo.

| Workload / Pod | CPU Requests | CPU Usage (Peak) | Memory Requests | Memory Usage (Peak) | PVC Capacity |
|---|---|---|---|---|---|
| `cassandra-0` | 300m | ~450m (lúc khởi động) | 1 Gi | ~920 MiB | 5 Gi |
| `kafka-0` | 250m | Chưa đo | 512 Mi | Chưa đo | 5 Gi |
| `hdfs-namenode-0` | 200m | ~250m | 512 Mi | ~480 MiB | 5 Gi |
| `hdfs-datanode-0` | 200m | ~200m | 512 Mi | ~420 MiB | 5 Gi |
| **Tổng cộng dự kiến** | **~950m** | **Chưa đo** | **~2.5 Gi** | **Chưa đo** | **20 Gi** |

JVM heap giới hạn để phù hợp local. Cần đo CPU/RAM thực tế khi cả ba
dịch vụ đã được triển khai trước khi kết luận tổng footprint.

---

## 7. Giới hạn Môi trường Local (Known Limitations)

1. **Replication Factor = 1**: Toàn bộ hệ thống (Cassandra, Kafka, HDFS) đều chạy 1 replica/broker/datanode để tiết kiệm RAM cho máy tính cá nhân. Trong môi trường production, replication factor tối thiểu phải là 3.
2. **SimpleStrategy Keyspace**: Cassandra sử dụng `SimpleStrategy` cho single datacenter cục bộ. Khi lên cụm cloud đa zone, sẽ chuyển sang `NetworkTopologyStrategy`.
3. **StorageClass Local-path**: Kind sử dụng volume host-path từ Docker container của control-plane, do đó nếu xóa Kind cluster (`kind delete cluster`), dữ liệu PVC sẽ bị xóa theo. Dữ liệu chỉ được đảm bảo tồn tại qua các lần restart pod trong cùng một cluster.
