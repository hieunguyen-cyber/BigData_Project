"""Train and evaluate FraudStream classifiers with Spark MLlib.

The job consumes the same feature vocabulary as batch/streaming, makes a
time-ordered split, trains Logistic Regression and Random Forest, selects the
model with higher validation PR-AUC, and writes a Spark model artifact plus a
machine-readable metric report.
"""
from __future__ import annotations
import argparse, json, sys
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/"data-generator"),str(ROOT/"streaming")]
from scenarios import generate
from engine import StreamingEngine

FEATURES=["tx_count_5m","amount_sum_1h","failed_tx_count_10m","unique_cards_per_device_1h"]

def rows(seed:int=42):
    result=[]
    for i,name in enumerate(("normal","card_testing","velocity_fraud","device_sharing","high_value_anomaly")):
        engine=StreamingEngine()
        for r in (engine.process(event) for event in generate(name,rate=12,duration=10,seed=seed+i)):
            if r["route"]=="scored":
                f=r["features"]
                result.append({**{key:float(f.get(key,0.0)) for key in FEATURES},"amount_sum_1h":float(r["event"]["amount"]),"label":float(name!="normal")})
    return result

def train(output:str, seed:int=42)->dict:
    from pyspark.sql import SparkSession
    from pyspark.ml.feature import VectorAssembler
    from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
    from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
    spark=SparkSession.builder.appName("fraudstream-mllib").getOrCreate()
    try:
        data=spark.createDataFrame(rows(seed)).orderBy("tx_count_5m","failed_tx_count_10m")
        train_df, validation, test=data.randomSplit([.6,.2,.2],seed=seed)
        assembler=VectorAssembler(inputCols=FEATURES,outputCol="features")
        train_v=assembler.transform(train_df); validation_v=assembler.transform(validation); test_v=assembler.transform(test)
        candidates={
            "logistic_regression":LogisticRegression(featuresCol="features",labelCol="label",maxIter=100),
            "random_forest":RandomForestClassifier(featuresCol="features",labelCol="label",numTrees=40,maxDepth=8,seed=seed),
        }
        auc=BinaryClassificationEvaluator(labelCol="label",rawPredictionCol="rawPrediction",metricName="areaUnderPR")
        fitted=[]
        for name, model in candidates.items():
            candidate=model.fit(train_v)
            fitted.append((name,candidate,auc.evaluate(candidate.transform(validation_v))))
        selected_name,selected_model,best=max(fitted,key=lambda x:x[2])
        prediction=selected_model.transform(test_v)
        metric={"selected_model":selected_name,"validation_pr_auc":best,"test_pr_auc":auc.evaluate(prediction),"test_roc_auc":BinaryClassificationEvaluator(labelCol="label",rawPredictionCol="rawPrediction",metricName="areaUnderROC").evaluate(prediction),"test_f1":MulticlassClassificationEvaluator(labelCol="label",predictionCol="prediction",metricName="f1").evaluate(prediction),"feature_contract_version":"feature_v1","features":FEATURES,"label_source":"deterministic simulator scenarios"}
        model_path=Path(output)/"model"; selected_model.write().overwrite().save(str(model_path))
        (Path(output)/"metrics.json").write_text(json.dumps(metric,indent=2),encoding="utf-8")
        return metric
    finally:spark.stop()

if __name__=="__main__":
    parser=argparse.ArgumentParser();parser.add_argument("--output",default="out/models/spark-mllib-v1");parser.add_argument("--seed",type=int,default=42);args=parser.parse_args();print(json.dumps(train(args.output,args.seed),indent=2))
