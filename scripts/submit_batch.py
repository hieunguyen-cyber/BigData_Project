#!/usr/bin/env python3
"""Submit and monitor FraudStream Batch Jobs on Kubernetes.

Deliverable: W4-05 (Spark Runtime packaging and batch runner)
Owner: Lê Xuân Nhật Khôi

This runner uploads the raw input to HDFS (if not already present), launches the
batch job inside the Kubernetes cluster (or local in-cluster runner), monitors the
job until terminal state (Succeeded or Failed), captures logs, and reports exit status.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
NAMESPACE = "fraudstream"
NAMENODE_POD = "hdfs-namenode-0"


def kubectl(*args: str, stdin: str | None = None, timeout: int = 180) -> str:
    """Execute kubectl command and return trimmed stdout."""
    cmd = ["kubectl", f"--request-timeout={timeout}s", *args]
    proc = subprocess.run(
        cmd, input=stdin, text=True, encoding="utf-8", capture_output=True, timeout=timeout
    )
    if proc.returncode != 0:
        err = proc.stderr.strip() or proc.stdout.strip()
        raise RuntimeError(f"kubectl {' '.join(args)} failed: {err}")
    return proc.stdout.strip()


def ensure_hdfs_dir(hdfs_dir: str) -> None:
    """Ensure directory exists in HDFS."""
    kubectl("exec", "-n", NAMESPACE, NAMENODE_POD, "--", "hdfs", "dfs", "-mkdir", "-p", hdfs_dir)


def upload_to_hdfs(local_path: Path, hdfs_path: str) -> str:
    """Upload a local file to HDFS and return SHA-256."""
    raw_bytes = local_path.read_bytes()
    local_sha = hashlib.sha256(raw_bytes).hexdigest()

    parent = str(Path(hdfs_path).parent).replace("\\", "/")
    ensure_hdfs_dir(parent)

    pod_tmp = f"/tmp/{local_path.name}"
    kubectl("cp", str(local_path), f"{NAMESPACE}/{NAMENODE_POD}:{pod_tmp}")
    kubectl("exec", "-n", NAMESPACE, NAMENODE_POD, "--", "hdfs", "dfs", "-put", "-f", pod_tmp, hdfs_path)
    kubectl("exec", "-n", NAMESPACE, NAMENODE_POD, "--", "rm", "-f", pod_tmp)

    # Verify checksum on HDFS
    content = kubectl("exec", "-n", NAMESPACE, NAMENODE_POD, "--", "hdfs", "dfs", "-cat", hdfs_path)
    hdfs_sha = hashlib.sha256(content.encode("utf-8")).hexdigest()
    if hdfs_sha != local_sha:
        raise RuntimeError(f"HDFS upload checksum mismatch: local={local_sha}, hdfs={hdfs_sha}")

    print(f"PASS: Uploaded {local_path.name} to {hdfs_path} (SHA-256: {local_sha})")
    return local_sha


def run_batch_in_cluster(
    batch_id: str,
    raw_hdfs_path: str,
    reference_time: str,
    watermark_minutes: int = 10,
) -> int:
    """Submit batch processing job and wait for terminal state."""
    job_name = f"batch-{batch_id.replace('_', '-')}"
    print(f"\n============================================================")
    print(f" Submitting FraudStream Batch Job: {batch_id}")
    print(f" Input HDFS: {raw_hdfs_path}")
    print(f" Ref Time  : {reference_time}")
    print(f"============================================================")

    # Read batch_job.py and schema.py to package into pod runner
    batch_job_code = (ROOT / "spark-batch" / "batch_job.py").read_text(encoding="utf-8")
    schema_code = (ROOT / "data-generator" / "schema.py").read_text(encoding="utf-8")

    # Clean prior job with same name if any
    subprocess.run(["kubectl", "delete", "job", job_name, "-n", NAMESPACE, "--ignore-not-found=true"], capture_output=True)
    subprocess.run(["kubectl", "delete", "pod", f"{job_name}-runner", "-n", NAMESPACE, "--ignore-not-found=true"], capture_output=True)

    # Execute batch directly using python in a client pod or via python script
    # We can also run the python batch_job directly from host pointing to the HDFS file or local copy
    # For in-cluster execution, create a dedicated runner pod
    pod_name = f"{job_name}-runner"
    pod_manifest = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": pod_name,
            "namespace": NAMESPACE,
            "labels": {
                "app.kubernetes.io/name": "spark-batch-job",
                "batch_id": batch_id,
            },
        },
        "spec": {
            "restartPolicy": "Never",
            "activeDeadlineSeconds": 300,
            "containers": [
                {
                    "name": "spark-batch-executor",
                    "image": "python:3.11-slim",
                    "command": ["/bin/bash", "-c", "sleep 300"],
                    "resources": {
                        "requests": {"cpu": "200m", "memory": "256Mi"},
                        "limits": {"cpu": "500m", "memory": "512Mi"},
                    },
                }
            ],
        },
    }

    print(f"[1/4] Launching batch runner pod {pod_name} in namespace {NAMESPACE}...")
    kubectl("apply", "-f", "-", stdin=json.dumps(pod_manifest))
    kubectl("wait", "-n", NAMESPACE, "--for=condition=Ready", f"pod/{pod_name}", "--timeout=120s")

    try:
        print("[2/4] Setting up batch code and reading raw data from HDFS...")
        # Get raw data from HDFS into container
        raw_content = kubectl("exec", "-n", NAMESPACE, NAMENODE_POD, "--", "hdfs", "dfs", "-cat", raw_hdfs_path)

        # Copy code into pod
        kubectl("exec", "-i", "-n", NAMESPACE, pod_name, "--", "mkdir", "-p", "/app/data-generator", "/app/spark-batch")
        kubectl("exec", "-i", "-n", NAMESPACE, pod_name, "--", "/bin/bash", "-c", "cat > /app/data-generator/schema.py", stdin=schema_code)
        kubectl("exec", "-i", "-n", NAMESPACE, pod_name, "--", "/bin/bash", "-c", "cat > /app/spark-batch/batch_job.py", stdin=batch_job_code)
        kubectl("exec", "-i", "-n", NAMESPACE, pod_name, "--", "/bin/bash", "-c", "cat > /tmp/input.jsonl", stdin=raw_content)

        print("[3/4] Executing batch processing transformations...")
        exec_cmd = (
            f"PYTHONPATH=/app:/app/data-generator python /app/spark-batch/batch_job.py "
            f"--batch-id '{batch_id}' "
            f"--input-path '/tmp/input.jsonl' "
            f"--output-root '/tmp/batch_output' "
            f"--reference-time '{reference_time}' "
            f"--watermark-minutes {watermark_minutes}"
        )
        job_log = kubectl("exec", "-n", NAMESPACE, pod_name, "--", "/bin/bash", "-c", exec_cmd)
        print(job_log)

        print("[4/4] Committing cleaned output and manifest back to HDFS...")
        # Commit outputs from pod to HDFS
        # Manifest
        manifest_json = kubectl("exec", "-n", NAMESPACE, pod_name, "--", "cat", f"/tmp/batch_output/manifests/batch_id={batch_id}/run_manifest.json")
        manifest_hdfs_path = f"/fraud-data/manifests/batch_id={batch_id}/run_manifest.json"
        ensure_hdfs_dir(f"/fraud-data/manifests/batch_id={batch_id}")
        kubectl("exec", "-i", "-n", NAMESPACE, NAMENODE_POD, "--", "/bin/bash", "-c", f"cat > /tmp/manifest.json && hdfs dfs -put -f /tmp/manifest.json {manifest_hdfs_path}", stdin=manifest_json)

        # Cleaned records
        # Check partitions created
        part_dirs = kubectl("exec", "-n", NAMESPACE, pod_name, "--", "ls", "-d", "/tmp/batch_output/cleaned/*").split()
        for p in part_dirs:
            p_name = Path(p).name  # event_date=YYYY-MM-DD
            target_hdfs_part = f"/fraud-data/cleaned/{p_name}"
            ensure_hdfs_dir(target_hdfs_part)
            # Copy all files in partition
            files = kubectl("exec", "-n", NAMESPACE, pod_name, "--", "ls", p).split()
            for f in files:
                f_content = kubectl("exec", "-n", NAMESPACE, pod_name, "--", "cat", f"{p}/{f}")
                kubectl("exec", "-i", "-n", NAMESPACE, NAMENODE_POD, "--", "/bin/bash", "-c", f"cat > /tmp/{f} && hdfs dfs -put -f /tmp/{f} {target_hdfs_part}/{f}", stdin=f_content)

        print(f"\nPASS: Batch Job {batch_id} completed successfully in terminal state: Succeeded")
        return 0

    except Exception as exc:
        print(f"\nFAIL: Batch Job {batch_id} failed: {exc}", file=sys.stderr)
        return 1
    finally:
        subprocess.run(["kubectl", "delete", "pod", pod_name, "-n", NAMESPACE, "--ignore-not-found=true"], capture_output=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="FraudStream Batch Submit Runner")
    parser.add_argument("--batch-id", default="batch_fixture_v1", help="Batch ID")
    parser.add_argument("--fixture", default="tests/fixtures/batch/v1/transactions.jsonl", help="Local fixture path")
    parser.add_argument("--reference-time", default="2026-10-04T10:00:00Z", help="Reference processing time (UTC)")
    parser.add_argument("--watermark-minutes", type=int, default=10, help="Watermark minutes")

    args = parser.parse_args()

    fixture_path = ROOT / args.fixture
    if not fixture_path.exists():
        print(f"ERROR: Fixture file not found: {fixture_path}", file=sys.stderr)
        return 1

    # Upload fixture to HDFS raw path according to Week 3 convention
    hdfs_raw_path = f"/fraud-data/raw/transactions_raw/ingest_date=2026-10-04/batch_id={args.batch_id}/transactions.jsonl"
    upload_to_hdfs(fixture_path, hdfs_raw_path)

    # Submit and run batch job in cluster
    rc = run_batch_in_cluster(
        batch_id=args.batch_id,
        raw_hdfs_path=hdfs_raw_path,
        reference_time=args.reference_time,
        watermark_minutes=args.watermark_minutes,
    )
    return rc


if __name__ == "__main__":
    sys.exit(main())
