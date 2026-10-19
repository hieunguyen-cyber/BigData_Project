# Hướng dẫn Triển khai Hạ tầng Nền tảng Tuần 2 (FraudStream)

Tài liệu hướng dẫn triển khai môi trường Kubernetes local, cài đặt và kiểm thử bộ ba hạ tầng **Cassandra, Kafka, HDFS** trong namespace `fraudstream`.

---

## 1. Kiến trúc Hạ tầng & Quy ước Chung

Hạ tầng nền tảng Tuần 2 cung cấp nền móng cho pipeline Lambda Architecture của FraudStream:
- **Serving Layer**: Apache Cassandra (StatefulSet 1 node, lưu real-time risk scores và fraud alerts).
- **Ingestion / Speed Layer**: Apache Kafka (1 broker KRaft, tiếp nhận giao dịch từ simulator).
- **Batch / Storage Layer**: Apache HDFS (1 NameNode + DataNode, lưu raw, cleaned, curated Parquet data).

```mermaid
flowchart TD
    subgraph Local_Machine["Máy thành viên (Docker Desktop / WSL2)"]
        subgraph K8s["Kind Kubernetes Cluster: fraudstream-cluster"]
            subgraph NS["Namespace: fraudstream"]
                subgraph Serving["Serving Store (Lê Xuân Nhật Khôi)"]
                    C[Cassandra StatefulSet: cassandra-0]
                    CPVC[(Cassandra PVC: 5Gi)]
                    CSVC[Service: cassandra.fraudstream.svc.cluster.local:9042]
                    C --- CPVC
                    C --- CSVC
                end
                subgraph Ingestion["Event Stream (Nguyễn Minh Hoàng)"]
                    K[Kafka Broker: kafka-0]
                    KPVC[(Kafka PVC)]
                    KSVC[Service: kafka.fraudstream.svc.cluster.local:9092]
                    K --- KPVC
                    K --- KSVC
                end
                subgraph Storage["Data Lake (Lê Trọng Đạt)"]
                    HNN[HDFS NameNode]
                    HDN[HDFS DataNode]
                    HPVC[(HDFS PVCs)]
                    HSVC[Service: hdfs.fraudstream.svc.cluster.local:9000]
                    HNN --- HPVC
                    HDN --- HPVC
                    HNN --- HSVC
                end
                CLI[Client Pod / Smoke Test Runner]
                CLI -.-> CSVC
                CLI -.-> KSVC
                CLI -.-> HSVC
            end
        end
    end
```

### Bảng Quy ước Đặt tên & Kết nối Nội bộ

| Dịch vụ | Loại workload | Service DNS nội bộ | Port | Storage Class |
|---|---|---|---|---|
| **Cassandra** | StatefulSet (`cassandra-0`) | `cassandra.fraudstream.svc.cluster.local` | `9042` | `standard` (5Gi) |
| **Kafka** | StatefulSet / Pod | `kafka.fraudstream.svc.cluster.local` | `9092` | `standard` |
| **HDFS NameNode** | StatefulSet / Pod | `hdfs-namenode.fraudstream.svc.cluster.local` | `9000` (hoặc `8020`), Web: `9870` | `standard` |

---

## 2. Yêu cầu Tiên quyết (Prerequisites)

1. **Hệ điều hành**: Windows 10/11 (với WSL2), macOS, hoặc Linux Ubuntu 22.04+.
2. **Docker Desktop**:
   - Engine: WSL 2 based engine.
   - Resource allocation: Tối thiểu **8 GB RAM** (khuyên dùng **10–12 GB RAM**) và **4 CPU cores** để chạy đồng thời cả 3 hệ thống phân tán.
   - *Gợi ý cho Windows*: Tạo file `C:\Users\<user>\.wslconfig` với:
     ```ini
     [wsl2]
     memory=10GB
     processors=4
     ```
3. **Các công cụ dòng lệnh (CLI)**:
   - `kubectl` (v1.28+)
   - `kind` (v0.22+)
   - `helm` (v3.14+)
   
   *Cài đặt nhanh trên Windows qua PowerShell:*
   ```powershell
   winget install Kubernetes.kubectl Kubernetes.kind Helm.Helm
   ```

---

## 3. Khởi tạo Platform Kubernetes (Lê Xuân Nhật Khôi)

### Bước 3.1: Dựng Cluster bằng Kind
Từ thư mục gốc của repository:
```bash
# Trên Linux / macOS / Git Bash:
kind create cluster --config infra/kind/kind-cluster.yaml

# Trên Windows PowerShell:
kind create cluster --config infra/kind/kind-cluster.yaml
```
Xác nhận cluster đã sẵn sàng:
```bash
kubectl cluster-info --context kind-fraudstream-cluster
kubectl get nodes
```

**Môi trường Mac của Hoàng:** Kind 0.33.0, kubectl 1.34.1. Tạo cluster
Kubernetes 1.34.11 bằng cách ghi đè image cho lần chạy này (file cấu hình
chung vẫn giữ 1.29.2):

```bash
kind create cluster --config infra/kind/kind-cluster.yaml \
  --image kindest/node:v1.34.11@sha256:44e222ee2132dab25ff87301682f89eb82c7880ea3a1bf543bfe9708fd08d67d \
  --wait 5m
```

Chọn một lệnh tạo cluster phù hợp môi trường; không chạy cả hai để tạo
cùng tên cluster. kubectl nên chênh tối đa một minor version với Kubernetes.

### Bước 3.2: Khởi tạo Namespace & Resource Quota
```bash
kubectl apply -f infra/platform/00-namespace.yaml
kubectl apply -f infra/platform/01-resource-quota.yaml
```

### Bước 3.3: Chạy Preflight Check
Kiểm tra tính sẵn sàng của Docker, nodes, CoreDNS và StorageClass:
```bash
# Trên Linux / macOS / Bash:
bash infra/scripts/check_preflight.sh

# Trên Windows PowerShell:
.\infra\scripts\check_preflight.ps1
```

---

## 4. Triển khai Apache Cassandra (Lê Xuân Nhật Khôi)

### Bước 4.1: Áp dụng Cấu hình & Secret
```bash
kubectl apply -f infra/cassandra/cassandra-config.yaml
kubectl apply -f infra/cassandra/cassandra-service.yaml
```

### Bước 4.2: Triển khai Cassandra StatefulSet
```bash
kubectl apply -f infra/cassandra/cassandra-statefulset.yaml
```
Theo dõi trạng thái khởi động pod:
```bash
kubectl get pods -n fraudstream -w
```
*(Cassandra cần khoảng 60–90 giây để hoàn tất khởi động Java process và vượt qua Readiness Probe).*

### Bước 4.3: Chạy Cassandra Smoke Test & CQL Bootstrap
Khởi tạo keyspace `fraudstream`, bảng `transaction_risk`, `fraud_alert`, insert dữ liệu mẫu và chạy truy vấn kiểm thử (không dùng `ALLOW FILTERING`):
```bash
# Trên Linux / macOS / Bash:
bash infra/cassandra/smoke_test.sh

# Trên Windows PowerShell:
.\infra\cassandra\smoke_test.ps1
```

Để kiểm tra cơ chế bền vững dữ liệu (Persistence Test) khi pod bị xóa và dựng lại:
```bash
# Bash:
bash infra/cassandra/smoke_test.sh --test-persistence

# PowerShell:
.\infra\cassandra\smoke_test.ps1 -TestPersistence
```

---

## 5. Triển khai Apache Kafka (Nguyễn Minh Hoàng)

Phần Kafka có ba file: `infra/kafka/kafka.yaml` để triển khai,
`bootstrap_topics.sh` để tạo topics và `smoke_test.py` để kiểm tra
health/config, gửi–đọc dữ liệu và persistence. Hướng dẫn tập trung tại mục
này; kết quả chạy thực tế ghi trong `week_2_test_evidence.md`.

Triển khai Kafka 3.9.1, một node KRaft kiêm broker/controller, PVC 5 GiB
và JVM heap tối đa 512 MiB. Requests: 250m CPU / 512 MiB; limits:
1 CPU / 1536 MiB. Có startup/readiness/liveness probes.

```bash
kubectl apply -f infra/kafka/kafka.yaml
kubectl rollout status statefulset/kafka -n fraudstream --timeout=300s
kubectl get pods,pvc,svc -n fraudstream
bash infra/kafka/bootstrap_topics.sh
```

Chạy lại bootstrap để kiểm tra idempotency. Script kiểm tra cấu hình thực
tế và trả lỗi nếu topic có sẵn nhưng không đúng partition/RF/retention.

| Topic | Partitions | Retention | Replication factor |
|---|---:|---:|---:|
| `transactions_raw` | 3 | 168 giờ | 1 |
| `transactions_invalid` | 1 | 72 giờ | 1 |
| `fraud_alerts` | 3 | 168 giờ | 1 |
| `fraud_feedback` | 1 | 720 giờ | 1 |

Raw/alerts có 3 partitions để chuẩn bị consumer song song; invalid/feedback
có 1 partition cho demo lưu lượng thấp. Feedback giữ 30 ngày để phục vụ
training. RF=1 phù hợp một broker local, không có khả năng chịu mất broker.

**Endpoint bàn giao:** `kafka.fraudstream.svc.cluster.local:9092`, giao thức
PLAINTEXT, key UTF-8 là `card_id_hash`, value là JSON UTF-8 theo data contract.
Endpoint này dùng từ pod trong cluster; chưa có listener cho client trên Mac.
Kafka chỉ lưu/phân phối sự kiện; Spark ở tuần sau chịu trách nhiệm tạo
invalid events, alerts và scoring.

Chạy smoke test từ Mac/Linux với Python 3 và kubectl (không cần thêm Python packages):

```bash
python3 infra/kafka/smoke_test.py
python3 infra/kafka/smoke_test.py --test-persistence
```

Script tạo client pod từ `apache/kafka:3.9.1`, dùng simulator tạo 100 events,
gửi bằng Kafka CLI với key, đọc đúng khoảng offsets mới, kiểm tra JSON/schema,
key và từng transaction ID. Client pod được xóa sau khi chạy.
Tùy chọn persistence tạo lại client và broker pod rồi đọc lại chính 100
events đã gửi trước restart, kiểm tra UID pod đổi và PVC/PV giữ nguyên.
Báo cáo mỗi lần chạy ở `out/kafka_smoke_<run_id>.json` (gitignored), không
commit log/output của từng lượt chạy.

Kiểm tra riêng phần Hoàng qua script chung:

```bash
bash infra/scripts/verify_infra.sh --kafka-only
```

Lệnh kiểm tra pod Ready, PVC Bound, bốn topic đúng cấu hình và smoke test
100 events. Thiếu broker/topic/PVC hoặc lỗi đọc–ghi đều trả exit code khác 0.
Kiểm tra chỉ đọc, không gửi event:

```bash
python3 infra/kafka/smoke_test.py --check-only
```

PowerShell: `.\infra\scripts\verify_infra.ps1 -KafkaOnly` (mặc định dùng `python`).
Nếu dùng `python3`, thêm `-PythonCommand python3`. Trên Windows, bootstrap
Bash chạy trong Git Bash; smoke test Python có thể chạy từ PowerShell.

`--namespace` hoặc `KAFKA_NAMESPACE` chọn namespace, `KAFKA_POD` chọn
broker pod và `KAFKA_BOOTSTRAP_SERVER` ghi đè endpoint. `KAFKA_TOPIC` chọn
topic gửi thử (mặc định `transactions_raw`). Các kiểm tra dùng context
kubectl hiện tại và in rõ context/endpoint trước khi chạy. Mỗi lượt có
transaction ID riêng và khoảng offsets mới nên chạy lại không nhầm dữ
liệu cũ. Bootstrap dừng nếu topic có sẵn nhưng sai cấu hình; không xóa topic.

---

## 6. Triển khai Apache HDFS (Lê Trọng Đạt)

1. Triển khai NameNode, DataNode và Service trong namespace `fraudstream`.
2. Khởi tạo cây thư mục Data Lake:
   ```bash
   kubectl exec -n fraudstream hdfs-namenode-0 -- hdfs dfs -mkdir -p \
     /fraud-data/raw \
     /fraud-data/cleaned \
     /fraud-data/curated \
     /fraud-data/late_events \
     /fraud-data/checkpoints \
     /fraud-data/models
   ```
3. Chạy HDFS smoke test: tải file kiểm thử lên `/fraud-data/raw`, `hdfs dfs -ls` và kiểm tra checksum.

---

## 7. Xác minh Toàn diện Hạ tầng (End-to-End Verification)

Sau khi cả ba thành phần đã được deploy, chạy script xác minh toàn bộ hệ thống:

```bash
# Trên Linux / macOS / Bash:
bash infra/scripts/verify_infra.sh

# Trên Windows PowerShell:
.\infra\scripts\verify_infra.ps1
```

Script sẽ tuần tự kiểm tra:
1. Trạng thái Kubernetes Nodes, CoreDNS và PVCs.
2. Trạng thái Cassandra (`UN`), schema keyspace và smoke test query.
3. Kafka pod Ready, PVC Bound, cấu hình 4 topics và producer/consumer 100 events.
4. Trạng thái HDFS NameNode và 6 thư mục Data Lake chuẩn.

---

## 8. Quy trình Dọn dẹp & Hủy Hạ tầng (Teardown)

Khi muốn tạm dừng hoặc dọn dẹp tài nguyên máy:

Chỉ gỡ Kafka, giữ PVC và các dịch vụ khác:

```bash
kubectl delete -f infra/kafka/kafka.yaml
```

PVC do StatefulSet tạo được giữ lại để lần deploy sau dùng lại dữ liệu.
Các lệnh xóa namespace/cluster dưới đây xóa cả dữ liệu Kafka của môi trường local.

### 1. Dọn dẹp workloads trong namespace (giữ cluster):
```bash
kubectl delete namespace fraudstream
```

### 2. Xóa toàn bộ Kind cluster:
```bash
kind delete cluster --name fraudstream-cluster
```

---

## 9. Hướng dẫn Xử lý Sự cố Thường gặp (Troubleshooting)

**Kafka:** nếu pod không Ready, xem `kubectl describe pod kafka-0 -n fraudstream`
và `kubectl logs kafka-0 -n fraudstream --tail=80`. Nếu PVC Pending, kiểm tra
StorageClass `standard` và events của pod. CLI trong image nằm tại
`/opt/kafka/bin/`, không giả định `kafka-topics.sh` có trong PATH.
Lỗi DNS từ laptop: dùng smoke test có client pod; endpoint nội bộ không
được thiết kế cho producer kết nối trực tiếp từ Mac. Nếu sửa ConfigMap,
chạy `kubectl rollout restart statefulset/kafka -n fraudstream` để pod nhận
biến môi trường mới.

### 1. Pod Cassandra ở trạng thái `Pending`
- **Nguyên nhân**: Thiếu tài nguyên CPU/RAM hoặc StorageClass không cấp phát được PVC.
- **Cách xử lý**:
  ```bash
  kubectl describe pod cassandra-0 -n fraudstream
  kubectl get pvc -n fraudstream
  ```
  Nếu PVC ở trạng thái `Pending`, kiểm tra xem StorageClass `standard` của Kind có đang hoạt động không (`kubectl get storageclass`).

### 2. Cassandra pod bị `OOMKilled` (Exit code 137)
- **Nguyên nhân**: Docker Desktop bị giới hạn RAM, hoặc Cassandra JVM vượt quá memory limit.
- **Cách xử lý**:
  Đảm bảo trong `infra/cassandra/cassandra-config.yaml` đã set:
  ```yaml
  MAX_HEAP_SIZE: "512M"
  HEAP_NEWSIZE: "128M"
  ```
  Đồng thời kiểm tra file `.wslconfig` để cấp đủ ít nhất 8GB RAM cho WSL2.

### 3. Lỗi kết nối `cqlsh: Connection error`
- **Nguyên nhân**: Cassandra đang trong quá trình khởi động hoặc đang replay commitlog.
- **Cách xử lý**:
  Kiểm tra log pod:
  ```bash
  kubectl logs -f cassandra-0 -n fraudstream
  ```
  Chờ cho đến khi thấy log: `Starting listening for CQL clients on ... (0.0.0.0:9042)` trước khi chạy lệnh cqlsh.
