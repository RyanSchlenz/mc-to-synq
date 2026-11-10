"""In-process fake Monte Carlo GraphQL + SYNQ REST server for CLI tests.

No real network access and no real credentials: the server only accepts
the fake values set by the test fixtures.
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

MC_ID, MC_TOKEN = "fake-mc-id", "fake-mc-token"
SYNQ_ID, SYNQ_SECRET, SYNQ_TOKEN = "fake-client", "fake-secret", "fake-access-token"

E = "piqdw:bizviews."

GET_MONITORS = [
    {"uuid": "m-where", "name": "orders_null_dates", "monitorType": "CUSTOM_SQL",
     "description": "Orders with null dates", "consolidatedMonitorStatus": "ACTIVE"},
    {"uuid": "m-count", "name": "negative_totals", "monitorType": "CUSTOM_SQL",
     "description": "Negative totals", "consolidatedMonitorStatus": "ACTIVE"},
    {"uuid": "m-disabled", "name": "disabled_rule", "monitorType": "CUSTOM_SQL",
     "description": "Disabled", "consolidatedMonitorStatus": "DISABLED"},
    {"uuid": "m-other", "name": "staging_rule", "monitorType": "CUSTOM_SQL",
     "description": "Staging", "consolidatedMonitorStatus": "ACTIVE"},
    {"uuid": "m-schema", "name": "schema_changes", "monitorType": "SCHEMA",
     "description": "Schema", "consolidatedMonitorStatus": "ACTIVE"},
]

CUSTOM_RULES = [
    ("m-where", None, "SELECT * FROM piqdw.bizviews.orders WHERE order_date IS NULL;", "orders",
     {"intervalMinutes": 60}),
    ("m-count", None, "SELECT COUNT(*) FROM piqdw.bizviews.orders WHERE total < 0", "orders", None),
    ("m-disabled", None, "SELECT * FROM piqdw.bizviews.orders WHERE 1=0", "orders", None),
    ("m-other", None, "SELECT * FROM piqdw.staging.raw WHERE x IS NULL", None, None),
    ("cr-jinja", "Templated", "SELECT * FROM {{ ref('o') }} WHERE x IS NULL", "orders", None),
    ("cr-noname-1", None, "SELECT * FROM piqdw.bizviews.payments WHERE a IS NULL", "payments",
     None),
    ("cr-noname-2", None, "SELECT * FROM piqdw.bizviews.refunds WHERE a < 0", "refunds", None),
    ("cr-dup-a", "Null amount", "SELECT * FROM piqdw.bizviews.invoices WHERE a IS NULL", "invoices",
     None),
    ("cr-dup-b", "Null amount", "SELECT * FROM piqdw.bizviews.credits WHERE a IS NULL", "credits",
     None),
]

USER_DEFINED = [
    {"uuid": "ud-fresh", "entities": [E + "orders"], "monitorType": "FRESHNESS",
     "ruleDescription": "Orders freshness"},
]

SYNQ_TABLES = {
    "ORDERS": ("ENTITY_TYPE_SNOWFLAKE_TABLE", ["ORDER_ID", "DSS_LOAD_DATE"],
               [{"type": "TABLE_CONSTRAINT_TYPE_PRIMARY_KEY", "columns": ["ORDER_ID"]}]),
    "V_SUMMARY": ("ENTITY_TYPE_SNOWFLAKE_VIEW", ["DAY", "LOAD_DATE"], []),
    "V_CUSTOMER": ("ENTITY_TYPE_SNOWFLAKE_VIEW", ["ID", "UPDATED_AT"],
                   [{"rawType": "PRIMARY KEY", "columns": ["ID"]}]),
    "V_NO_TS": ("ENTITY_TYPE_SNOWFLAKE_VIEW", ["A"], []),
}


class FakeApis:
    def __init__(self) -> None:
        self.sql_tests: dict[str, dict] = {}
        self.monitors: dict[str, dict] = {}
        self.requests: list[dict] = []
        self.fail_sql_upsert = False
        fake = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *a):  # noqa: N802
                pass

            def _do(self, method):
                u = urlparse(self.path)
                qs = parse_qs(u.query)
                n = int(self.headers.get("Content-Length") or 0)
                raw = self.rfile.read(n).decode() if n else ""
                if raw and "json" in self.headers.get("Content-Type", ""):
                    body = json.loads(raw)
                else:
                    body = parse_qs(raw)
                fake.requests.append({"method": method, "path": u.path, "body": body})
                status, resp = fake.route(method, u.path, qs, body, self.headers)
                out = json.dumps(resp).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(out)))
                self.end_headers()
                self.wfile.write(out)

            def do_GET(self):  # noqa: N802
                self._do("GET")

            def do_POST(self):  # noqa: N802
                self._do("POST")

            def do_DELETE(self):  # noqa: N802
                self._do("DELETE")

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def start(self) -> FakeApis:
        self.thread.start()
        return self

    def stop(self) -> None:
        self.server.shutdown()
        self.server.server_close()

    # -- routing ------------------------------------------------------------

    def route(self, method, path, qs, body, headers):
        if path == "/graphql":
            if headers.get("x-mcd-id") != MC_ID or headers.get("x-mcd-token") != MC_TOKEN:
                return 401, {"errors": [{"message": "unauthorized"}]}
            return 200, self.mc(body.get("query", ""))
        if path == "/oauth2/token":
            ok = body.get("client_id") == [SYNQ_ID] and body.get("client_secret") == [SYNQ_SECRET]
            return (200, {"access_token": SYNQ_TOKEN}) if ok else (401, {"error": "bad"})
        if headers.get("Authorization") != f"Bearer {SYNQ_TOKEN}":
            return 401, {"message": "unauthorized"}
        return self.synq(method, path, qs, body)

    def mc(self, q):
        if '__type(name: "CustomRule")' in q:
            fields = ["uuid", "description", "isDeleted", "entities", "ruleType",
                      "queryText", "scheduleConfig"]
            return {"data": {"__type": {"fields": [{"name": f} for f in fields]}}}
        if '__type(name: "UserDefinedMonitorV2")' in q:
            fields = ["uuid", "entities", "monitorType", "ruleDescription"]
            return {"data": {"__type": {"kind": "OBJECT", "possibleTypes": None,
                                        "fields": [{"name": f} for f in fields]}}}
        if "getMonitors" in q:
            return {"data": {"getMonitors": GET_MONITORS}}
        if "getCustomRules" in q:
            if "customSql" in q or "alertCondition" in q:
                return {"errors": [{"message": "Cannot query field"}], "data": None}
            edges = []
            for uuid, desc, sql, table, sched in CUSTOM_RULES:
                ent = [E + table] if table else ["piqdw:staging.raw"]
                edges.append({"node": {"uuid": uuid, "description": desc, "isDeleted": False,
                                       "entities": ent, "ruleType": "CUSTOM_SQL",
                                       "queryText": sql, "scheduleConfig": sched}})
            return {"data": {"getCustomRules": {"edges": edges}}}
        if "getAllUserDefinedMonitorsV2" in q:
            return {"data": {"getAllUserDefinedMonitorsV2": {
                "edges": [{"node": dict(u)} for u in USER_DEFINED]}}}
        if "connections" in q:
            return {"data": {"getUser": {"account": {"connections": [{"uuid": "c1"}]}}}}
        if "getUser" in q:
            return {"data": {"getUser": {"email": "tester@example.com", "firstName": "Test",
                                         "lastName": "User"}}}
        return {"data": None, "errors": [{"message": "unknown query"}]}

    def synq(self, method, path, qs, body):
        if path == "/api/entities/v1/search":
            page = int(qs.get("page", ["0"])[0])
            hits = [{"entityType": t, "name": n, "synqPath": f"sf::acct::PIQDW::BIZVIEWS::{n}"}
                    for n, (t, _, _) in SYNQ_TABLES.items()]
            hits.append({"entityType": "ENTITY_TYPE_SNOWFLAKE_TABLE", "name": "RAW",
                         "synqPath": "sf::acct::PIQDW::STAGING::RAW"})
            pages = [hits[:3], hits[3:]]
            return 200, {"hits": pages[page] if page < len(pages) else []}
        if path in ("/api/schema/v1", "/api/constraints/v1/table-constraints"):
            t = qs.get("id.snowflakeTable.table", [""])[0]
            if t not in SYNQ_TABLES:
                return 404, {}
            _, cols, cons = SYNQ_TABLES[t]
            if path == "/api/schema/v1":
                return 200, {"schema": {"columns": [{"name": c} for c in cols]}}
            return 200, {"constraints": cons}
        if path == "/api/monitors/custom-monitors/v1":
            if method == "GET":
                return 200, {"monitors": list(self.monitors.values())}
            for m in body["monitors"]:
                self.monitors[m["id"]] = m
            return 200, {"created_monitor_ids": [m["id"] for m in body["monitors"]]}
        if path == "/api/monitors/custom-monitors/v1/delete":
            gone = [i for i in body["ids"] if self.monitors.pop(i, None)]
            return 200, {"deleted_ids": gone}
        if path == "/api/datachecks/sqltests/v1":
            if method == "GET":
                return 200, {"sql_tests": list(self.sql_tests.values())}
            if method == "DELETE":
                for i in qs.get("ids", []):
                    self.sql_tests.pop(i, None)
                return 200, {}
            if self.fail_sql_upsert:
                return 500, {"message": "internal error"}
            created, updated = [], []
            for t in body["sql_tests"]:
                (updated if t["id"] in self.sql_tests else created).append(t["id"])
                self.sql_tests[t["id"]] = t
            return 200, {"created_ids": created, "updated_ids": updated, "errors": []}
        return 404, {"message": f"no route {method} {path}"}
