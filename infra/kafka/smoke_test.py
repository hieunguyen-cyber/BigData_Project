#!/usr/bin/env python3
"""Send 100 simulator events from a client pod and verify Kafka key/JSON values.

Only Python's standard library and kubectl are required on the host.
Offsets and unique transaction IDs isolate each run from previous smoke tests.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "data-generator"))
from scenarios import generate  # noqa: E402
from schema import json_line, validate_event  # noqa: E402

TOPICS = {
    "transactions_raw": (3, 604800000),
    "transactions_invalid": (1, 259200000),
    "fraud_alerts": (3, 604800000),
    "fraud_feedback": (1, 2592000000),
}


def kubectl(*args: str, stdin: str | None = None, timeout: int = 60) -> str:
    result = subprocess.run(
        ["kubectl", f"--request-timeout={timeout}s", *args],
        input=stdin, text=True, encoding="utf-8", capture_output=True, timeout=timeout,
    )
    if result.returncode:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip())
    return result.stdout


def get_resource(kind: str, name: str, namespace: str) -> dict:
    return json.loads(kubectl("get", kind, name, "-n", namespace, "-o", "json"))


def data_claim(pod: dict) -> str:
    claims = [
        volume["persistentVolumeClaim"]["claimName"]
        for volume in pod["spec"]["volumes"]
        if volume["name"] == "kafka-data" and "persistentVolumeClaim" in volume
    ]
    if len(claims) != 1:
        raise ValueError("Kafka data volume must use a PVC")
    return claims[0]


def verify_kafka(namespace: str, pod_name: str, endpoint: str) -> None:
    """Check broker readiness, persistent storage and all four topic settings."""
    pod = get_resource("pod", pod_name, namespace)
    if pod["status"]["phase"] != "Running" or not any(
        condition["type"] == "Ready" and condition["status"] == "True"
        for condition in pod["status"].get("conditions", [])
    ):
        raise ValueError(f"Kafka pod {pod_name} is not Ready")
    print(f"PASS: Kafka pod {pod_name} is Running and Ready.", flush=True)
    claim = data_claim(pod)
    if get_resource("pvc", claim, namespace)["status"]["phase"] != "Bound":
        raise ValueError(f"Kafka PVC {claim} is not Bound")
    print(f"PASS: Kafka PVC {claim} is Bound.", flush=True)
    for topic, (partitions, retention) in TOPICS.items():
        description = kubectl(
            "exec", "-n", namespace, pod_name, "--", "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server", endpoint, "--describe", "--topic", topic,
        )
        if not all(re.search(pattern, description) for pattern in (
            rf"PartitionCount:\s*{partitions}(?:\s|$)",
            r"ReplicationFactor:\s*1(?:\s|$)",
            rf"retention[.]ms={retention}(?:[,\s]|$)",
        )):
            raise ValueError(f"Kafka topic {topic} is missing or has incorrect settings")
        print(f"PASS: {topic}: partitions={partitions}, replication=1, retention.ms={retention}.", flush=True)
    print("PASS: Kafka health and all 4 topic configurations verified.", flush=True)


def verify_records(lines: list[str], expected: dict[str, dict]) -> int:
    seen: set[str] = set()
    for line in lines:
        key, value = line.split("\t", 1)
        event = json.loads(value)
        transaction_id = event["transaction_id"]
        if transaction_id not in expected:
            continue  # Other traffic during the same offset range is not this test.
        issues = validate_event(event)
        if issues:
            raise ValueError(f"Invalid event {transaction_id}: {issues}")
        if key != event["card_id_hash"]:
            raise ValueError(f"Wrong Kafka key for {transaction_id}")
        if event != expected[transaction_id]:
            raise ValueError(f"JSON value changed for {transaction_id}")
        if transaction_id in seen:
            raise ValueError(f"Duplicate event {transaction_id}")
        seen.add(transaction_id)
    if seen != set(expected):
        raise ValueError(f"Expected {len(expected)} events, received {len(seen)}")
    return len(seen)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--namespace", default=os.environ.get("KAFKA_NAMESPACE", "fraudstream"))
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--check-only", action="store_true", help="Only verify health/config; do not send events")
    mode.add_argument(
        "--test-persistence", action="store_true",
        help="Recreate the Kafka broker and client pods before reading the same events",
    )
    args = parser.parse_args()
    namespace = args.namespace
    broker_name = os.environ.get("KAFKA_POD", "kafka-0")
    endpoint = os.environ.get(
        "KAFKA_BOOTSTRAP_SERVER", f"kafka.{namespace}.svc.cluster.local:9092"
    )
    topic = os.environ.get("KAFKA_TOPIC", "transactions_raw")
    run_id = uuid.uuid4().hex[:12]
    pod_name = f"kafka-smoke-{run_id}"
    created = False
    persistence = {"tested": False}

    def cli(tool: str, *args: str, stdin: str | None = None) -> str:
        return kubectl(
            "exec", "-i", "-n", namespace, pod_name, "--",
            f"/opt/kafka/bin/{tool}.sh", "--bootstrap-server", endpoint,
            *args, stdin=stdin, timeout=60,
        )

    def offsets() -> dict[int, int]:
        output = cli("kafka-get-offsets", "--topic", topic, "--time", "-1")
        result = {}
        for line in output.splitlines():
            name, partition, offset = line.rsplit(":", 2)
            if name == topic:
                result[int(partition)] = int(offset)
        if not result:
            raise ValueError(f"No partitions found for topic {topic}; run bootstrap first")
        return result

    try:
        context = kubectl("config", "current-context").strip()
        print(f"Context: {context}; endpoint: {endpoint}", flush=True)
        verify_kafka(namespace, broker_name, endpoint)
        if args.check_only:
            return 0
        print("[1/4] Creating a temporary Kafka client pod...", flush=True)
        pod = {
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {
                "name": pod_name, "namespace": namespace,
                "labels": {"app.kubernetes.io/name": "kafka-smoke", "app.kubernetes.io/part-of": "fraudstream"},
            },
            "spec": {
                "restartPolicy": "Never", "activeDeadlineSeconds": 600,
                "containers": [{
                    "name": "client", "image": "apache/kafka:3.9.1",
                    "imagePullPolicy": "IfNotPresent", "command": ["/bin/bash", "-c", "sleep 600"],
                    "resources": {
                        "requests": {"cpu": "100m", "memory": "256Mi"},
                        "limits": {"cpu": "500m", "memory": "512Mi"},
                    },
                }],
            },
        }
        kubectl("create", "-f", "-", stdin=json.dumps(pod))
        created = True
        kubectl("wait", "-n", namespace, "--for=condition=Ready", f"pod/{pod_name}", "--timeout=180s", timeout=200)
        before = offsets()

        print("[2/4] Sending 100 normal simulator events...", flush=True)
        events = generate(
            "normal", rate=20, duration=5, seed=42,
            start=datetime.now(timezone.utc) - timedelta(seconds=5),
        )
        for event in events:
            event["transaction_id"] = f"smoke_{run_id}_{event['transaction_id']}"
        expected = {event["transaction_id"]: event for event in events}
        payload = "".join(f"{event['card_id_hash']}\t{json_line(event)}\n" for event in events)
        cli(
            "kafka-console-producer", "--topic", topic,
            "--property", "parse.key=true", "--property", "key.separator=\t",
            "--producer-property", "acks=all",
            "--producer-property", "delivery.timeout.ms=30000",
            "--producer-property", "request.timeout.ms=10000", stdin=payload,
        )
        after = offsets()
        if set(before) != set(after):
            raise ValueError("Topic partitions changed during this test; rerun")

        if args.test_persistence:
            broker_before = get_resource("pod", broker_name, namespace)
            client_before = get_resource("pod", pod_name, namespace)
            if not any(
                owner["kind"] == "StatefulSet"
                for owner in broker_before["metadata"].get("ownerReferences", [])
            ):
                raise ValueError("Persistence test requires a StatefulSet broker")
            claim_name = data_claim(broker_before)
            pvc_before = get_resource("pvc", claim_name, namespace)
            if pvc_before["status"]["phase"] != "Bound":
                raise ValueError("Kafka PVC is not Bound")

            print("[persistence] Recreating client and broker pods; waiting for Kafka recovery...", flush=True)
            kubectl("delete", "pod", pod_name, "-n", namespace, "--wait=true", "--timeout=120s", timeout=150)
            kubectl("delete", "pod", broker_name, "-n", namespace, "--wait=true", "--timeout=120s", timeout=150)
            kubectl("wait", "-n", namespace, "--for=create", f"pod/{broker_name}", "--timeout=120s", timeout=150)
            kubectl("wait", "-n", namespace, "--for=condition=Ready", f"pod/{broker_name}", "--timeout=300s", timeout=330)

            broker_after = get_resource("pod", broker_name, namespace)
            pvc_after = get_resource("pvc", claim_name, namespace)
            if broker_after["metadata"]["uid"] == broker_before["metadata"]["uid"]:
                raise ValueError("Broker pod was not recreated")
            if (
                data_claim(broker_after) != claim_name
                or pvc_after["metadata"]["uid"] != pvc_before["metadata"]["uid"]
                or pvc_after["spec"]["volumeName"] != pvc_before["spec"]["volumeName"]
                or pvc_after["status"]["phase"] != "Bound"
            ):
                raise ValueError("Broker did not reuse the same Bound PVC/PV")

            kubectl("create", "-f", "-", stdin=json.dumps(pod))
            kubectl("wait", "-n", namespace, "--for=condition=Ready", f"pod/{pod_name}", "--timeout=180s", timeout=200)
            client_after = get_resource("pod", pod_name, namespace)
            if client_after["metadata"]["uid"] == client_before["metadata"]["uid"]:
                raise ValueError("Client pod was not recreated")
            persistence = {
                "tested": True, "pvc": claim_name,
                "pvc_uid": pvc_after["metadata"]["uid"], "pv": pvc_after["spec"]["volumeName"],
                "broker_uid_before": broker_before["metadata"]["uid"],
                "broker_uid_after": broker_after["metadata"]["uid"],
                "client_uid_before": client_before["metadata"]["uid"],
                "client_uid_after": client_after["metadata"]["uid"],
            }

        print("[3/4] Reading new records and checking JSON, schema and Kafka keys...", flush=True)
        lines = []
        for partition in sorted(before):
            count = after[partition] - before[partition]
            if count < 0:
                raise ValueError("Topic offsets moved backwards during the test")
            if count:
                output = cli(
                    "kafka-console-consumer", "--topic", topic,
                    "--partition", str(partition), "--offset", str(before[partition]),
                    "--max-messages", str(count), "--timeout-ms", "15000",
                    "--property", "print.key=true", "--property", "key.separator=\t",
                )
                lines.extend(output.splitlines())
        received = verify_records(lines, expected)
        if persistence["tested"]:
            persistence["passed"] = True

        evidence = {
            "passed": True, "run_id": run_id, "context": context,
            "tested_at_utc": datetime.now(timezone.utc).isoformat(),
            "endpoint": endpoint, "topic": topic, "client_image": "apache/kafka:3.9.1",
            "sent": len(expected), "received": received,
            "json_values_match": True, "schema_valid": True, "keys_match": True,
            "offsets_before": before, "offsets_after": after,
            "persistence": persistence,
        }
        output_dir = ROOT / "out"
        output_dir.mkdir(exist_ok=True)
        evidence_path = output_dir / f"kafka_smoke_{run_id}.json"
        evidence_path.write_text(json.dumps(evidence, indent=2) + "\n", encoding="utf-8")
        print(f"[4/4] Evidence saved to {evidence_path}", flush=True)
        print(f"PASS: {received}/100 events received; JSON, schema and Kafka keys match.", flush=True)
        if persistence["tested"]:
            print("PASS: data preserved after broker and client pod recreation; same PVC/PV reused.", flush=True)
        return 0
    except (OSError, RuntimeError, ValueError, KeyError, TypeError, subprocess.TimeoutExpired) as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1
    finally:
        if created:
            try:
                kubectl("delete", "pod", pod_name, "-n", namespace, "--ignore-not-found", "--wait=false")
            except (OSError, RuntimeError, subprocess.TimeoutExpired) as exc:
                print(f"Client pod cleanup failed: {exc}", file=sys.stderr)


if __name__ == "__main__":
    raise SystemExit(main())
