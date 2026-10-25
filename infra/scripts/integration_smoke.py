#!/usr/bin/env python3
"""Prove the Week 2 handoff: simulator -> Kafka -> HDFS raw storage.

The script sends 100 deterministic simulator events to Kafka through a temporary
in-cluster client pod, consumes exactly those event IDs, then writes the same
JSONL source to HDFS and compares SHA-256 checksums. It does not run Spark.
"""
from __future__ import annotations

import hashlib
import json
import subprocess
import sys
import tempfile
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "data-generator"))
from scenarios import generate  # noqa: E402
from schema import json_line, validate_event  # noqa: E402

NAMESPACE = "fraudstream"
TOPIC = "transactions_raw"
KAFKA_POD = "kafka-0"
KAFKA_ENDPOINT = f"kafka.{NAMESPACE}.svc.cluster.local:9092"


def kubectl(*args: str, stdin: str | None = None, timeout: int = 180) -> str:
    result = subprocess.run(
        ["kubectl", f"--request-timeout={timeout}s", *args],
        input=stdin, text=True, encoding="utf-8", capture_output=True, timeout=timeout,
    )
    if result.returncode:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip())
    return result.stdout


def pod_is_ready(name: str) -> bool:
    pod = json.loads(kubectl("get", "pod", name, "-n", NAMESPACE, "-o", "json"))
    return pod["status"].get("phase") == "Running" and any(
        condition["type"] == "Ready" and condition["status"] == "True"
        for condition in pod["status"].get("conditions", [])
    )


def offsets(client: str) -> dict[int, int]:
    output = kubectl(
        "exec", "-n", NAMESPACE, client, "--", "/opt/kafka/bin/kafka-get-offsets.sh",
        "--bootstrap-server", KAFKA_ENDPOINT, "--topic", TOPIC, "--time", "-1",
    )
    result: dict[int, int] = {}
    for line in output.splitlines():
        topic, partition, offset = line.rsplit(":", 2)
        if topic == TOPIC:
            result[int(partition)] = int(offset)
    if not result:
        raise RuntimeError("Kafka topic has no partitions; run bootstrap_topics.sh first")
    return result


def main() -> int:
    run_id = uuid.uuid4().hex[:12]
    client = f"integration-smoke-{run_id}"
    hdfs_pod = "hdfs-namenode-0"
    local_path: Path | None = None
    try:
        if not pod_is_ready(KAFKA_POD):
            raise RuntimeError("Kafka broker is not Ready")
        if not pod_is_ready(hdfs_pod):
            raise RuntimeError("HDFS NameNode is not Ready")
        # kubectl() raises on a non-zero exit code, so this is a real gate
        # rather than a warning that could let a partial data lake pass.
        kubectl("exec", "-n", NAMESPACE, hdfs_pod, "--", "hdfs", "dfs", "-test", "-d", "/fraud-data/raw")

        events = generate(
            "normal", rate=20, duration=5, seed=42,
            start=datetime.now(timezone.utc) - timedelta(seconds=5),
        )[:100]
        if len(events) != 100:
            raise RuntimeError(f"Expected 100 simulator events, got {len(events)}")
        for event in events:
            event["transaction_id"] = f"integration_{run_id}_{event['transaction_id']}"
            issues = validate_event(event)
            if issues:
                raise RuntimeError(f"Simulator generated invalid event: {issues}")
        expected = {event["transaction_id"]: event for event in events}
        jsonl = "".join(f"{json_line(event)}\n" for event in events)
        payload = "".join(f"{event['card_id_hash']}\t{json_line(event)}\n" for event in events)

        with tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=".jsonl", delete=False) as handle:
            handle.write(jsonl)
            local_path = Path(handle.name)
        local_sha = hashlib.sha256(local_path.read_bytes()).hexdigest()

        pod_spec = {
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": client, "namespace": NAMESPACE},
            "spec": {"restartPolicy": "Never", "activeDeadlineSeconds": 600,
                "containers": [{"name": "client", "image": "apache/kafka:3.9.1",
                    "command": ["/bin/bash", "-c", "sleep 600"],
                    "resources": {"requests": {"cpu": "100m", "memory": "256Mi"},
                                  "limits": {"cpu": "500m", "memory": "512Mi"}}}]},
        }
        kubectl("create", "-f", "-", stdin=json.dumps(pod_spec))
        kubectl("wait", "-n", NAMESPACE, "--for=condition=Ready", f"pod/{client}", "--timeout=180s", timeout=200)
        before = offsets(client)
        kubectl(
            "exec", "-i", "-n", NAMESPACE, client, "--", "/opt/kafka/bin/kafka-console-producer.sh",
            "--bootstrap-server", KAFKA_ENDPOINT, "--topic", TOPIC,
            "--property", "parse.key=true", "--property", "key.separator=\t",
            "--producer-property", "acks=all", stdin=payload,
        )
        after = offsets(client)
        received: dict[str, dict] = {}
        for partition, start in before.items():
            count = after.get(partition, start) - start
            if count <= 0:
                continue
            output = kubectl(
                "exec", "-n", NAMESPACE, client, "--", "/opt/kafka/bin/kafka-console-consumer.sh",
                "--bootstrap-server", KAFKA_ENDPOINT, "--topic", TOPIC,
                "--partition", str(partition), "--offset", str(start), "--max-messages", str(count),
                "--timeout-ms", "15000", "--property", "print.key=true", "--property", "key.separator=\t",
            )
            for line in output.splitlines():
                key, value = line.split("\t", 1)
                event = json.loads(value)
                transaction_id = event.get("transaction_id")
                if transaction_id in expected:
                    if key != event["card_id_hash"] or event != expected[transaction_id]:
                        raise RuntimeError(f"Kafka value/key mismatch for {transaction_id}")
                    received[transaction_id] = event
        if set(received) != set(expected):
            raise RuntimeError(f"Kafka expected 100 events, received {len(received)}")
        print("PASS: simulator -> Kafka sent and consumed 100 schema-valid events.")

        hdfs_path = f"/fraud-data/raw/integration_{run_id}.jsonl"
        pod_path = f"/tmp/integration_{run_id}.jsonl"
        kubectl("cp", str(local_path), f"{NAMESPACE}/{hdfs_pod}:{pod_path}")
        kubectl("exec", "-n", NAMESPACE, hdfs_pod, "--", "hdfs", "dfs", "-put", "-f", pod_path, hdfs_path)
        hdfs_content = kubectl("exec", "-n", NAMESPACE, hdfs_pod, "--", "hdfs", "dfs", "-cat", hdfs_path)
        hdfs_sha = hashlib.sha256(hdfs_content.encode("utf-8")).hexdigest()
        if hdfs_sha != local_sha:
            raise RuntimeError("HDFS raw checksum differs from simulator JSONL")
        print(f"PASS: Kafka source persisted unchanged to HDFS: {hdfs_path}")
        print(f"PASS: run_id={run_id}; sha256={local_sha}; events=100")
        return 0
    except (RuntimeError, subprocess.TimeoutExpired, ValueError, json.JSONDecodeError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    finally:
        subprocess.run(["kubectl", "delete", "pod", client, "-n", NAMESPACE, "--ignore-not-found", "--wait=true"], capture_output=True)
        if local_path is not None:
            local_path.unlink(missing_ok=True)


if __name__ == "__main__":
    raise SystemExit(main())
