"""Deterministic throughput/latency benchmark for the scoring engine."""
from __future__ import annotations
import argparse, json, statistics, sys, time
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/"data-generator"),str(ROOT/"streaming")]
from scenarios import generate
from engine import StreamingEngine
def run(events:int,output:str):
 stream=generate("normal",rate=max(1,events),duration=1,seed=2026)[:events];engine=StreamingEngine();samples=[];start=time.perf_counter()
 for event in stream:
  at=time.perf_counter();engine.process(event);samples.append((time.perf_counter()-at)*1000)
 elapsed=time.perf_counter()-start
 report={"events":len(stream),"elapsed_seconds":elapsed,"throughput_events_per_second":len(stream)/elapsed,"latency_ms_p50":statistics.median(samples),"latency_ms_p95":sorted(samples)[max(0,int(len(samples)*.95)-1)]}
 Path(output).parent.mkdir(parents=True,exist_ok=True);Path(output).write_text(json.dumps(report,indent=2),encoding="utf-8");return report
if __name__=="__main__":
 p=argparse.ArgumentParser();p.add_argument("--events",type=int,default=1000);p.add_argument("--output",default="out/benchmarks/streaming.json");a=p.parse_args();print(json.dumps(run(a.events,a.output),indent=2))
