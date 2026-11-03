"""FraudStream dashboard backed by either injected test records or Cassandra."""
import os
from flask import Flask, jsonify, render_template_string, Response
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
T='''<!doctype html><title>FraudStream</title><style>body{font:16px system-ui;margin:3rem;background:#f4f7fb;color:#102a43}.card{display:inline-block;background:#fff;padding:1rem;margin:.5rem;border-radius:8px}table{background:#fff;border-collapse:collapse;width:100%}td,th{padding:.6rem;border:1px solid #dde}</style><h1>FraudStream — Risk Operations</h1><div class=card>Events: {{s.total}}</div><div class=card>Alerts: {{s.alerts}}</div><div class=card>Blocked: {{s.blocked}}</div><table><tr><th>Transaction</th><th>Decision</th><th>Risk</th><th>Reasons</th></tr>{% for r in rows %}<tr><td>{{r.event.transaction_id}}</td><td>{{r.decision}}</td><td>{{'%.2f'|format(r.risk_score)}}</td><td>{{r.trigger_reasons|join(', ')}}</td></tr>{% endfor %}</table>'''
def _cassandra_records():
    host=os.getenv("CASSANDRA_HOST")
    if not host:return []
    try:
        from cassandra.cluster import Cluster
        session=Cluster([host],connect_timeout=2).connect("fraudstream")
        rows=session.execute("SELECT event_time,transaction_id,decision,risk_score,trigger_reasons,card_id_hash FROM dashboard_recent_decision WHERE feed='recent' LIMIT 50")
        return [{"event":{"transaction_id":r.transaction_id,"card_id_hash":r.card_id_hash},"decision":r.decision,"risk_score":r.risk_score,"trigger_reasons":list(r.trigger_reasons or []),"alert":r.decision in ("REVIEW","BLOCK")} for r in rows]
    except Exception:
        # A read-only UI must remain available while a local Cassandra pod is
        # restarting; the health/readiness endpoint still exposes an empty view.
        return []

def create_app(records=None):
 app=Flask(__name__)
 def current():return list(records) if records is not None else _cassandra_records()
 def s(rows):return {"total":len(rows),"alerts":sum(x.get("alert",False) for x in rows),"blocked":sum(x.get("decision")=="BLOCK" for x in rows)}
 @app.get("/")
 def index():
  rows=current();return render_template_string(T,s=s(rows),rows=rows)
 @app.get("/api/summary")
 def summary():
  rows=current();return jsonify(s(rows))
 @app.get("/api/decisions")
 def decisions():return jsonify(current())
 @app.get("/metrics")
 def metrics():return Response(generate_latest(),mimetype=CONTENT_TYPE_LATEST)
 return app
