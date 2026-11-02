from __future__ import annotations
import sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/"spark-batch"))
from batch_job import run_batch_job
def test_idempotent_batch_id(tmp_path):
 source=ROOT/"tests/fixtures/batch/v1/transactions.jsonl"; a=run_batch_job(source,tmp_path,"same","2026-10-04T10:00:00Z"); b=run_batch_job(source,tmp_path,"same","2026-10-04T10:00:00Z")
 assert a["run_id"]==b["run_id"] and len(list((tmp_path/"cleaned").rglob("*.parquet")))==1
