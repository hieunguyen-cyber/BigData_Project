"""Stateful, deterministic online fraud scoring engine."""
from __future__ import annotations
from collections import defaultdict, deque
from datetime import timedelta
from pathlib import Path
from typing import Any
import sys
ROOT=Path(__file__).resolve().parents[1]; sys.path.insert(0,str(ROOT/"data-generator"))
from schema import parse_event_time, validate_event
from metrics import EVENTS, DECISIONS, LATENCY

class StreamingEngine:
    def __init__(self, watermark_minutes=10):
        self.watermark=timedelta(minutes=watermark_minutes); self.seen=set(); self.max_time=None
        self.cards=defaultdict(deque); self.failures=defaultdict(deque); self.devices=defaultdict(deque)
    @staticmethod
    def _trim(q,now,delta):
        while q and q[0][0] < now-delta: q.popleft()
    def process(self,event:dict[str,Any])->dict[str,Any]:
        with LATENCY.time():
         return self._process(event)
    def _process(self,event:dict[str,Any])->dict[str,Any]:
        issues=validate_event(event)
        if issues: EVENTS.labels("invalid").inc(); return {"route":"invalid","issues":issues,"event":event}
        if event["transaction_id"] in self.seen: EVENTS.labels("duplicate").inc(); return {"route":"duplicate","issues":["DQ-08"],"event":event}
        self.seen.add(event["transaction_id"]); ts=parse_event_time(event["event_time"])
        if self.max_time and ts < self.max_time-self.watermark: EVENTS.labels("late").inc(); return {"route":"late","issues":["DQ-09"],"event":event}
        self.max_time=max(self.max_time,ts) if self.max_time else ts
        card=self.cards[event["card_id_hash"]]; card.append((ts,event["amount"])); self._trim(card,ts,timedelta(minutes=5))
        failed=self.failures[event["card_id_hash"]]
        if event["transaction_status"]=="failed":failed.append((ts,1))
        self._trim(failed,ts,timedelta(minutes=10)); device=self.devices[event["device_id_hash"]]; device.append((ts,event["card_id_hash"])); self._trim(device,ts,timedelta(hours=1))
        features={"tx_count_5m":len(card),"failed_tx_count_10m":len(failed),"unique_cards_per_device_1h":len({x[1] for x in device})}
        reasons=[]
        if features["tx_count_5m"]>=5:reasons.append("velocity: tx_count_5m >= 5")
        if features["failed_tx_count_10m"]>=3:reasons.append("card_testing: failed_tx_count_10m >= 3")
        if features["unique_cards_per_device_1h"]>=3:reasons.append("device_sharing: unique_cards_per_device_1h >= 3")
        decision="BLOCK" if len(reasons)>=2 else "REVIEW" if reasons else "APPROVE"; EVENTS.labels("scored").inc(); DECISIONS.labels(decision).inc()
        return {"route":"scored","event":event,"features":features,"risk_score":min(.99,.45+.15*len(reasons)) if reasons else .02,"decision":decision,"trigger_reasons":reasons,"alert":bool(reasons)}
