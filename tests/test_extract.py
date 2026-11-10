"""Tests for Monte Carlo extraction: introspection, fallbacks, and merging."""

import re

from mc_to_synq.extract.monitors import extract_monitors


class FakeMC:
    """Minimal stand-in for MonteCarloClient.query with a configurable schema.

    Mirrors real GraphQL behavior: requesting an unknown field returns
    HTTP 200 with errors and data=null rather than raising.
    """

    def __init__(self, cr_fields=None, ud_fields=None, null_types=False, raise_on=None):
        self.cr_fields = cr_fields or [
            "uuid", "description", "isDeleted", "entities", "ruleType",
            "queryText", "scheduleConfig", "alertCondition",
        ]
        self.ud_fields = ud_fields or ["uuid", "entities", "monitorType", "ruleDescription"]
        self.null_types = null_types
        self.raise_on = raise_on or set()
        self.queries: list[str] = []

    def _unknown(self, q, allowed):
        m = re.search(r"node\s*\{(.*)\}\s*\}\s*\}", q, re.DOTALL)
        body = re.sub(r"\{[^{}]*\}", "", m.group(1)) if m else ""
        sub = {"intervalMinutes", "startTime", "operator", "threshold"}
        return [n for n in re.findall(r"[A-Za-z_]\w*", body) if n not in allowed and n not in sub]

    def query(self, q, variables=None):
        self.queries.append(q)
        for key in self.raise_on:
            if key in q:
                raise RuntimeError(f"boom: {key}")
        if '__type(name: "CustomRule")' in q:
            if self.null_types:
                return {"data": {"__type": None}}
            return {"data": {"__type": {"fields": [{"name": f} for f in self.cr_fields]}}}
        if '__type(name: "UserDefinedMonitorV2")' in q:
            if self.null_types:
                return {"data": {"__type": None}}
            return {"data": {"__type": {"kind": "OBJECT",
                                        "fields": [{"name": f} for f in self.ud_fields],
                                        "possibleTypes": None}}}
        if "getMonitors" in q:
            return {"data": {"getMonitors": [
                {"uuid": "m1", "name": "orders_check", "monitorType": "CUSTOM_SQL",
                 "consolidatedMonitorStatus": "ACTIVE"},
                {"uuid": "m2", "name": "freshness", "monitorType": None},
            ]}}
        if "getCustomRules" in q:
            if self._unknown(q, self.cr_fields):
                return {"errors": [{"message": "Cannot query field"}], "data": None}
            sql_key = "queryText" if "queryText" in self.cr_fields else "customSql"
            return {"data": {"getCustomRules": {"edges": [
                {"node": {"uuid": "m1", "description": "Orders check", "isDeleted": False,
                          "entities": ["db:s.orders"], "ruleType": "CUSTOM_SQL",
                          sql_key: "SELECT * FROM s.orders WHERE x IS NULL"}},
                {"node": {"uuid": "cr2", "description": None, "isDeleted": False,
                          "entities": ["db:s.payments"], "ruleType": "CUSTOM_SQL",
                          sql_key: "SELECT * FROM s.payments WHERE y < 0"}},
                {"node": {"uuid": "cr3", "description": "gone", "isDeleted": True,
                          "entities": [], sql_key: "SELECT 1"}},
            ]}}}
        if "getAllUserDefinedMonitorsV2" in q:
            if self._unknown(q, self.ud_fields):
                return {"errors": [{"message": "Cannot query field"}], "data": None}
            return {"data": {"getAllUserDefinedMonitorsV2": {"edges": [
                {"node": {"uuid": "m2", "entities": ["db:s.orders"],
                          "monitorType": "FRESHNESS", "ruleDescription": "Orders freshness"}},
                {"node": {"uuid": "ud3", "entities": ["db:s.customers"],
                          "monitorType": "VOLUME", "ruleDescription": "Customer volume"}},
            ]}}}
        if "connections" in q:
            return {"data": {"getUser": {"account": {"connections": [{"uuid": "c1"}]}}}}
        return {"data": None, "errors": [{"message": "unknown"}]}


def _by_uuid(monitors):
    return {m["uuid"]: m for m in monitors}


def test_introspection_discovers_renamed_sql_field():
    client = FakeMC()
    monitors, connections = extract_monitors(client)
    m = _by_uuid(monitors)
    assert m["m1"]["sql"] == "SELECT * FROM s.orders WHERE x IS NULL"
    assert connections == {"c1": {"uuid": "c1"}}
    # The dynamic query must request the introspected field name, not customSql
    cr_queries = [q for q in client.queries if "getCustomRules" in q]
    assert "queryText" in cr_queries[0] and "customSql" not in cr_queries[0]


def test_three_sources_are_merged():
    monitors, _ = extract_monitors(FakeMC())
    m = _by_uuid(monitors)
    # getMonitors + getCustomRules overlap
    assert m["m1"]["name"] == "orders_check"
    assert m["m1"]["_custom_rule"]["uuid"] == "m1"
    assert m["m1"]["entities"] == ["db:s.orders"]
    # getMonitors + user-defined overlap fills in entities and type
    assert m["m2"]["entities"] == ["db:s.orders"]
    assert m["m2"]["monitorType"] == "FRESHNESS"
    # custom-rule only and user-defined only monitors are added
    assert m["cr2"]["sql"].startswith("SELECT")
    assert m["ud3"]["description"] == "Customer volume"
    # deleted rules are dropped
    assert "cr3" not in m
    assert len(monitors) == 4


def test_null_description_gives_usable_name():
    monitors, _ = extract_monitors(FakeMC())
    m = _by_uuid(monitors)
    assert m["cr2"]["name"] == "custom_rule_cr2"
    assert m["cr2"]["description"] == ""


def test_null_introspection_does_not_crash():
    """__type returns null when a type is renamed or introspection is disabled."""
    client = FakeMC(null_types=True, cr_fields=["uuid", "description", "isDeleted",
                                                "entities", "ruleType", "scheduleConfig"])
    monitors, _ = extract_monitors(client)
    assert "m1" in _by_uuid(monitors)


def test_introspection_exception_does_not_crash():
    client = FakeMC(raise_on={"__type"})
    monitors, _ = extract_monitors(client)
    assert len(monitors) >= 2


def test_stale_field_falls_back_to_minimal_query():
    """If the dynamic query is rejected (errors + data=null), fall back."""

    class StaleAlert(FakeMC):
        def query(self, q, variables=None):
            if "getCustomRules" in q and "alertCondition" in q:
                self.queries.append(q)
                return {"errors": [{"message": "Cannot query field 'alertCondition'"}],
                        "data": None}
            return super().query(q, variables)

    client = StaleAlert()
    monitors, _ = extract_monitors(client)
    m = _by_uuid(monitors)
    assert m["m1"]["sql"] == "SELECT * FROM s.orders WHERE x IS NULL"
    assert sum("getCustomRules" in q for q in client.queries) == 2


def test_failed_source_does_not_abort_extraction():
    client = FakeMC(raise_on={"getMonitors", "connections"})
    monitors, connections = extract_monitors(client)
    assert connections == {}
    assert "cr2" in _by_uuid(monitors)
