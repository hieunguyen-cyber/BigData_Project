"""Kafka consumer/alert producer adapter for the FraudStream online engine."""
from __future__ import annotations
import argparse, json, sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/"streaming"))
from engine import StreamingEngine

def main():
 p=argparse.ArgumentParser();p.add_argument("--bootstrap-server",required=True);p.add_argument("--input-topic",default="transactions_raw");p.add_argument("--alert-topic",default="fraud_alerts");p.add_argument("--group-id",default="fraudstream-scoring-v1");p.add_argument("--cassandra-host");a=p.parse_args()
 from kafka import KafkaConsumer,KafkaProducer
 c=KafkaConsumer(a.input_topic,bootstrap_servers=a.bootstrap_server,group_id=a.group_id,auto_offset_reset="latest",value_deserializer=lambda b:json.loads(b.decode())); out=KafkaProducer(bootstrap_servers=a.bootstrap_server,value_serializer=lambda x:json.dumps(x,separators=(",",":")).encode()); engine=StreamingEngine(); session=None
 if a.cassandra_host:
  from cassandra.cluster import Cluster
  from cassandra_sink import write_decision
  session=Cluster([a.cassandra_host]).connect()
 try:
  for m in c:
   r=engine.process(m.value)
   if r["route"]=="scored":
    if session: write_decision(session,r)
    if r["alert"]:out.send(a.alert_topic,key=m.value["card_id_hash"].encode(),value=r);out.flush()
   print(json.dumps(r,ensure_ascii=False))
 finally:c.close();out.close()
if __name__=="__main__":main()
