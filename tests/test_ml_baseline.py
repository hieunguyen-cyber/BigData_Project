from __future__ import annotations
import sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/"ml"))
from baseline import train
def test_synthetic_baseline_trains_and_writes_versioned_artifact(tmp_path):
 output=tmp_path/"baseline.joblib"; metrics=train(output,seed=7)
 assert output.exists() and metrics["train_rows"]>0 and 0<=metrics["pr_auc"]<=1 and metrics["label_source"].startswith("synthetic")
