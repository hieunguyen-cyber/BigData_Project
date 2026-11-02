"""Train and evaluate a reproducible synthetic fraud baseline.

Synthetic labels are deliberately marked as such: this validates the pipeline,
not real-world fraud performance.
"""
from __future__ import annotations
import argparse, json, sys
from pathlib import Path
import joblib
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, precision_recall_fscore_support, roc_auc_score
from sklearn.model_selection import train_test_split
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/"data-generator"),str(ROOT/"streaming")]
from scenarios import generate
from engine import StreamingEngine
FEATURES=("tx_count_5m","failed_tx_count_10m","unique_cards_per_device_1h")

def build_dataset(seed=42):
 rows=[]; labels=[]
 for i,scenario in enumerate(("normal","card_testing","velocity_fraud","device_sharing","high_value_anomaly")):
  engine=StreamingEngine(); label=int(scenario!="normal")
  for result in (engine.process(e) for e in generate(scenario,rate=8,duration=8,seed=seed+i)):
   if result["route"]=="scored": rows.append([result["features"][x] for x in FEATURES]); labels.append(label)
 return rows,labels
def train(output:Path,seed=42):
 X,y=build_dataset(seed); X_train,X_test,y_train,y_test=train_test_split(X,y,test_size=.3,random_state=seed,stratify=y); model=LogisticRegression(class_weight="balanced",max_iter=1000,random_state=seed).fit(X_train,y_train); p=model.predict_proba(X_test)[:,1]; pred=(p>=.5).astype(int); precision,recall,f1,_=precision_recall_fscore_support(y_test,pred,average="binary",zero_division=0)
 metrics={"pr_auc":average_precision_score(y_test,p),"roc_auc":roc_auc_score(y_test,p),"precision":precision,"recall":recall,"f1":f1,"train_rows":len(X_train),"test_rows":len(X_test),"label_source":"synthetic simulator scenarios"}
 output.parent.mkdir(parents=True,exist_ok=True);joblib.dump({"model":model,"features":FEATURES,"metrics":metrics,"version":"baseline-synthetic-v1"},output);return metrics
if __name__=="__main__":
 p=argparse.ArgumentParser();p.add_argument("--output",default="out/models/baseline-synthetic-v1.joblib");p.add_argument("--seed",type=int,default=42);a=p.parse_args();print(json.dumps(train(Path(a.output),a.seed),indent=2))
