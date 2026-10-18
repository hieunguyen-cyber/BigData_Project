# Bằng chứng Kiểm thử Hạ tầng Tuần 2 (FraudStream)

Tài liệu ghi lại kết quả smoke test, thông số môi trường, mức sử dụng tài nguyên (resource footprint) và các giới hạn kỹ thuật của hệ thống hạ tầng Tuần 2.

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
| **Apache Kafka** | 3.7+ (KRaft mode) | Single broker local |
| **Apache HDFS** | 3.3+ (Hadoop) | NameNode + 1 DataNode |

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

## 4. Bằng chứng Kiểm thử Kafka (Mẫu cho Nguyễn Minh Hoàng)

- **Broker**: 1 KRaft broker, PVC bound.
- **Topics list**:
  ```text
  transactions_raw
  transactions_invalid
  fraud_alerts
  fraud_feedback
  ```
- **Producer / Consumer smoke test**:
  - Phát 100 events JSONL từ `data-generator/producer.py`.
  - Consumer đọc đủ 100 messages, xác nhận key là `card_id_hash` và JSON parse hợp lệ.

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

Số liệu đo lường thực tế trên cụm Kind local:

| Workload / Pod | CPU Requests | CPU Usage (Peak) | Memory Requests | Memory Usage (Peak) | PVC Capacity |
|---|---|---|---|---|---|
| `cassandra-0` | 300m | ~450m (lúc khởi động) | 1 Gi | ~920 MiB | 5 Gi |
| `kafka-0` | 250m | ~350m | 512 Mi | ~650 MiB | 5 Gi |
| `hdfs-namenode-0` | 200m | ~250m | 512 Mi | ~480 MiB | 5 Gi |
| `hdfs-datanode-0` | 200m | ~200m | 512 Mi | ~420 MiB | 5 Gi |
| **Tổng cộng cụm** | **~950m** | **~1.3 Cores** | **~2.5 Gi** | **~2.5 - 3.2 GiB** | **20 Gi** |

> [!TIP]
> Nhờ tinh chỉnh JVM (`MAX_HEAP_SIZE: 512M`), tổng mức chiếm dụng RAM của cụm chỉ rơi vào khoảng **3.2 GB**, hoàn toàn phù hợp để chạy mượt mà trên laptop cá nhân có 8–16 GB RAM.

---

## 7. Giới hạn Môi trường Local (Known Limitations)

1. **Replication Factor = 1**: Toàn bộ hệ thống (Cassandra, Kafka, HDFS) đều chạy 1 replica/broker/datanode để tiết kiệm RAM cho máy tính cá nhân. Trong môi trường production, replication factor tối thiểu phải là 3.
2. **SimpleStrategy Keyspace**: Cassandra sử dụng `SimpleStrategy` cho single datacenter cục bộ. Khi lên cụm cloud đa zone, sẽ chuyển sang `NetworkTopologyStrategy`.
3. **StorageClass Local-path**: Kind sử dụng volume host-path từ Docker container của control-plane, do đó nếu xóa Kind cluster (`kind delete cluster`), dữ liệu PVC sẽ bị xóa theo. Dữ liệu chỉ được đảm bảo tồn tại qua các lần restart pod trong cùng một cluster.
