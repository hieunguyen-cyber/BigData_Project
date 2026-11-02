"""Cassandra serving sink for scored streaming decisions."""
from __future__ import annotations
from datetime import datetime
from decimal import Decimal

def write_decision(session, result: dict, model_version: str = "rules-v1") -> None:
    e=result["event"]
    session.execute("""INSERT INTO fraudstream.transaction_risk
    (card_id_hash,event_time,transaction_id,customer_id,merchant_id,amount,currency,channel,device_id_hash,ip_hash,country,transaction_status,risk_score,decision,trigger_reasons,model_version,created_at)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,toTimestamp(now()))""",
    (e["card_id_hash"],datetime.fromisoformat(e["event_time"].replace("Z","+00:00")),e["transaction_id"],e["customer_id"],e["merchant_id"],Decimal(str(e["amount"])),e["currency"],e["channel"],e["device_id_hash"],e["ip_hash"],e["country"],e["transaction_status"],result["risk_score"],result["decision"],result["trigger_reasons"],model_version))
    if result["alert"]:
        session.execute("""INSERT INTO fraudstream.fraud_alert (card_id_hash,event_time,transaction_id,customer_id,merchant_id,amount,currency,risk_score,decision,alert_status,trigger_reasons,created_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'PENDING',%s,toTimestamp(now()))""",
        (e["card_id_hash"],datetime.fromisoformat(e["event_time"].replace("Z","+00:00")),e["transaction_id"],e["customer_id"],e["merchant_id"],Decimal(str(e["amount"])),e["currency"],result["risk_score"],result["decision"],result["trigger_reasons"]))
