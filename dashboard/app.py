"""Small local FraudStream dashboard and JSON API."""
from flask import Flask, jsonify, render_template_string
T='''<!doctype html><title>FraudStream</title><style>body{font:16px system-ui;margin:3rem;background:#f4f7fb;color:#102a43}.card{display:inline-block;background:#fff;padding:1rem;margin:.5rem;border-radius:8px}table{background:#fff;border-collapse:collapse;width:100%}td,th{padding:.6rem;border:1px solid #dde}</style><h1>FraudStream — Risk Operations</h1><div class=card>Events: {{s.total}}</div><div class=card>Alerts: {{s.alerts}}</div><div class=card>Blocked: {{s.blocked}}</div><table><tr><th>Transaction</th><th>Decision</th><th>Risk</th><th>Reasons</th></tr>{% for r in rows %}<tr><td>{{r.event.transaction_id}}</td><td>{{r.decision}}</td><td>{{'%.2f'|format(r.risk_score)}}</td><td>{{r.trigger_reasons|join(', ')}}</td></tr>{% endfor %}</table>'''
def create_app(records=None):
 app=Flask(__name__); records=records or []
 def s():return {"total":len(records),"alerts":sum(x.get("alert",False) for x in records),"blocked":sum(x.get("decision")=="BLOCK" for x in records)}
 @app.get("/")
 def index():return render_template_string(T,s=s(),rows=list(reversed(records[-50:])))
 @app.get("/api/summary")
 def summary():return jsonify(s())
 @app.get("/api/decisions")
 def decisions():return jsonify(records)
 return app
