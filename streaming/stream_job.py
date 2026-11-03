"""Kafka consumer/alert producer adapter for the FraudStream online engine."""
from __future__ import annotations
import argparse, json, sys, time
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/"streaming"))
from engine import StreamingEngine
from metrics import KAFKA_LAG

def main():
 p=argparse.ArgumentParser();p.add_argument("--bootstrap-server",required=True);p.add_argument("--input-topic",default="transactions_raw");p.add_argument("--alert-topic",default="fraud_alerts");p.add_argument("--group-id",default="fraudstream-scoring-v1");p.add_argument("--cassandra-host");p.add_argument("--metrics-port",type=int);a=p.parse_args()
 if a.metrics_port:
  from prometheus_client import start_http_server
  start_http_server(a.metrics_port)
 from kafka import KafkaConsumer,KafkaProducer
 c=KafkaConsumer(a.input_topic,bootstrap_servers=a.bootstrap_server,group_id=a.group_id,auto_offset_reset="latest",value_deserializer=lambda b:json.loads(b.decode())); out=KafkaProducer(bootstrap_servers=a.bootstrap_server,value_serializer=lambda x:json.dumps(x,separators=(",",":")).encode()); engine=StreamingEngine(); session=None
 if a.cassandra_host:
  from cassandra.cluster import Cluster
  from cassandra_sink import write_decision
  last_error=None
  for _ in range(30):
   try:
    session=Cluster([a.cassandra_host],connect_timeout=3).connect();break
   except Exception as exc:
    last_error=exc;time.sleep(2)
  else: raise RuntimeError(f"Cassandra unavailable after 60 seconds: {last_error}")
 try:
  for m in c:
   r=engine.process(m.value)
   if r["route"]=="scored":
    if session: write_decision(session,r)
    if r["alert"]:out.send(a.alert_topic,key=m.value["card_id_hash"].encode(),value=r);out.flush()
   try:
    assigned=c.assignment()
    if assigned:
     ends=c.end_offsets(assigned);KAFKA_LAG.set(sum(max(0,ends[p]-c.position(p)) for p in assigned))
   except Exception: pass
   print(json.dumps(r,ensure_ascii=False))
 finally:c.close();out.close()
if __name__=="__main__":main()
