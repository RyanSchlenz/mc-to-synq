"""Tests for MC -> SYNQ SQL test conversion logic."""

import sqlite3

import pytest

from mc_to_synq.config import AppConfig
from mc_to_synq.migrate.sql_tests import convert_monitors_to_sql_tests


def _make_config(**overrides) -> AppConfig:
    defaults = {
        "filters": {"text_patterns": ["bizview"]},
        "sql_tests": {
            "id_prefix": "mc_migrated_",
            "severity": "SEVERITY_ERROR",
            "skip_disabled": True,
            "skip_jinja_templates": True,
        },
        "synq": {"integration_id": "test-uuid"},
    }
    defaults.update(overrides)
    return AppConfig(**defaults)


def _monitor(sql, name="test_rule", desc="Test monitor", status="ACTIVE", entities=None):
    return {
        "uuid": "abc-123",
        "name": name,
        "description": desc,
        "monitorType": "CUSTOM_SQL",
        "sql": sql,
        "consolidatedMonitorStatus": status,
        "entities": entities or ["db:bizview.table1"],
    }


class TestSqlConversion:
    """Tests for the core SQL analysis and conversion."""

    def test_simple_where_clause_passes_through(self):
        sql = "SELECT * FROM PIQDW_PROD.BIZVIEWS.V_ORDERS WHERE order_date IS NULL"
        cfg = _make_config()
        tests, meta = convert_monitors_to_sql_tests([_monitor(sql)], cfg)
        assert len(tests) == 1
        assert tests[0]["sql_expression"] == sql
        assert not meta[0].get("needs_wrapping")

    def test_having_clause_not_wrapped(self):
        sql = (
            "SELECT customer_id, COUNT(*) "
            "FROM PIQDW_PROD.BIZVIEWS.V_ORDERS "
            "GROUP BY customer_id HAVING COUNT(*) > 1"
        )
        cfg = _make_config()
        tests, _ = convert_monitors_to_sql_tests([_monitor(sql)], cfg)
        assert len(tests) == 1
        assert "HAVING" in tests[0]["sql_expression"]

    def test_count_gets_wrapped(self):
        sql = "SELECT COUNT(*) FROM PIQDW_PROD.BIZVIEWS.V_ORDERS"
        cfg = _make_config()
        tests, meta = convert_monitors_to_sql_tests([_monitor(sql)], cfg)
        assert len(tests) == 1
        assert "_mc_check" in tests[0]["sql_expression"]
        assert meta[0]["needs_wrapping"] is True

    def test_jinja_template_skipped(self):
        sql = "SELECT * FROM {{ ref('bizview_orders') }}"
        cfg = _make_config()
        tests, meta = convert_monitors_to_sql_tests([_monitor(sql)], cfg)
        assert len(tests) == 0
        assert any(m.get("skipped") for m in meta)

    def test_disabled_monitor_skipped(self):
        sql = "SELECT * FROM PIQDW_PROD.BIZVIEWS.V_ORDERS WHERE x IS NULL"
        cfg = _make_config()
        tests, meta = convert_monitors_to_sql_tests(
            [_monitor(sql, status="DISABLED")], cfg
        )
        assert len(tests) == 0
        assert any(m.get("skip_reason", "").startswith("Disabled") for m in meta)

    def test_stable_id_generation(self):
        sql = "SELECT * FROM PIQDW_PROD.BIZVIEWS.V_ORDERS WHERE x IS NULL"
        cfg = _make_config()
        tests1, _ = convert_monitors_to_sql_tests([_monitor(sql)], cfg)
        tests2, _ = convert_monitors_to_sql_tests([_monitor(sql)], cfg)
        assert tests1[0]["id"] == tests2[0]["id"]

    def test_id_prefix_from_config(self):
        sql = "SELECT * FROM PIQDW_PROD.BIZVIEWS.V_ORDERS WHERE x IS NULL"
        cfg = _make_config(sql_tests={"id_prefix": "custom_"})
        tests, _ = convert_monitors_to_sql_tests([_monitor(sql)], cfg)
        assert tests[0]["id"].startswith("custom_")

    def test_integration_id_set(self):
        sql = "SELECT * FROM PIQDW_PROD.BIZVIEWS.V_ORDERS WHERE x IS NULL"
        cfg = _make_config()
        tests, _ = convert_monitors_to_sql_tests([_monitor(sql)], cfg)
        assert tests[0]["platform"]["synq_integration_id"] == "test-uuid"

    def test_changeme_integration_id_excluded(self):
        sql = "SELECT * FROM PIQDW_PROD.BIZVIEWS.V_ORDERS WHERE x IS NULL"
        cfg = _make_config(synq={"integration_id": "CHANGEME"})
        tests, _ = convert_monitors_to_sql_tests([_monitor(sql)], cfg)
        assert "platform" not in tests[0]

    def test_monitor_without_sql_ignored(self):
        m = _monitor("")
        cfg = _make_config()
        tests, meta = convert_monitors_to_sql_tests([m], cfg)
        assert len(tests) == 0

    def test_text_pattern_filtering(self):
        """Monitors that don't match text patterns are excluded."""
        sql = "SELECT * FROM PRODUCTION.SALES.ORDERS WHERE x IS NULL"
        m = _monitor(sql, entities=["db:sales.orders"])
        cfg = _make_config(filters={"text_patterns": ["bizview"]})
        tests, _ = convert_monitors_to_sql_tests([m], cfg)
        assert len(tests) == 0

    def test_cte_with_where_not_flagged(self):
        sql = (
            "WITH base AS (SELECT * FROM PIQDW_PROD.BIZVIEWS.V_ORDERS) "
            "SELECT * FROM base WHERE order_total < 0"
        )
        cfg = _make_config()
        tests, meta = convert_monitors_to_sql_tests([_monitor(sql)], cfg)
        assert len(tests) == 1
        assert not meta[0].get("manual_review")

    def test_cte_without_where_flagged_for_review(self):
        sql = (
            "WITH base AS (SELECT * FROM PIQDW_PROD.BIZVIEWS.V_ORDERS) "
            "SELECT COUNT(*) FROM base"
        )
        cfg = _make_config()
        _, meta = convert_monitors_to_sql_tests([_monitor(sql)], cfg)
        converted = [m for m in meta if not m.get("skipped")]
        assert any(m.get("manual_review") for m in converted)


# ---------------------------------------------------------------------------
# Semantic checks: execute the generated SQL and verify SYNQ's contract
# (0 rows = pass, any rows = fail) on a clean and a dirty dataset.
# ---------------------------------------------------------------------------



def _db(dirty: bool) -> sqlite3.Connection:
    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE orders (order_id, customer_id, total)")
    con.execute("CREATE TABLE orders_copy (order_id, customer_id, total)")
    con.execute("CREATE TABLE customers (customer_id)")
    con.execute("INSERT INTO customers VALUES (1), (2)")
    con.execute("INSERT INTO orders VALUES (1, 1, 10), (2, 2, 5)")
    con.execute("INSERT INTO orders_copy SELECT * FROM orders")
    if dirty:
        con.execute("INSERT INTO customers VALUES (2)")
        con.execute("INSERT INTO orders VALUES (3, 99, -1)")
    return con


def _verdict(sql: str, dirty: bool) -> str:
    rows = _db(dirty).execute(sql).fetchall()
    return "fail" if rows else "pass"


SEMANTIC_CASES = {
    "plain_count": "SELECT COUNT(*) FROM orders WHERE total < 0",
    "aliased_count": "SELECT COUNT(*) AS bad_rows FROM orders WHERE total < 0",
    "count_distinct": "select count(distinct order_id) from orders where total < 0",
    "count_over_having": (
        "SELECT COUNT(*) FROM (SELECT customer_id FROM customers "
        "GROUP BY customer_id HAVING COUNT(*) > 1)"
    ),
    "count_not_exists": (
        "SELECT COUNT(*) FROM orders o WHERE NOT EXISTS "
        "(SELECT 1 FROM customers c WHERE c.customer_id = o.customer_id)"
    ),
    "reconciliation": (
        "SELECT (SELECT COUNT(*) FROM orders) AS source_count, "
        "(SELECT COUNT(*) FROM orders_copy) AS target_count, "
        "(SELECT COUNT(*) FROM orders) - (SELECT COUNT(*) FROM orders_copy) AS difference"
    ),
    "having_rows": (
        "SELECT customer_id, COUNT(*) FROM customers GROUP BY customer_id HAVING COUNT(*) > 1"
    ),
    "not_exists_rows": (
        "SELECT o.* FROM orders o WHERE NOT EXISTS "
        "(SELECT 1 FROM customers c WHERE c.customer_id = o.customer_id)"
    ),
    "where_rows": "SELECT * FROM orders WHERE total < 0",
    "cte_where": "WITH b AS (SELECT * FROM orders) SELECT * FROM b WHERE total < 0",
}


class TestSqlSemantics:
    """The deployed SQL must actually pass on clean data and fail on dirty data."""

    @pytest.mark.parametrize("case", sorted(SEMANTIC_CASES))
    def test_pass_on_clean_fail_on_dirty(self, case):
        sql = SEMANTIC_CASES[case]
        cfg = _make_config(filters={})
        tests, meta = convert_monitors_to_sql_tests([_monitor(sql, name=case)], cfg)
        assert len(tests) == 1
        assert not meta[0]["manual_review"], meta[0]["wrapping_note"]
        deployed = tests[0]["sql_expression"]
        assert _verdict(deployed, dirty=False) == "pass", deployed
        assert _verdict(deployed, dirty=True) == "fail", deployed

    def test_plain_count_wrap_references_only_real_columns(self):
        cfg = _make_config(filters={})
        sql = "SELECT COUNT(*) FROM orders"
        tests, _ = convert_monitors_to_sql_tests([_monitor(sql)], cfg)
        out = tests[0]["sql_expression"]
        assert "difference" not in out and "source_count" not in out
        _db(False).execute(out).fetchall()  # must not raise

    def test_cte_returning_count_flagged(self):
        sql = "WITH b AS (SELECT * FROM orders) SELECT COUNT(*) FROM b WHERE total < 0"
        cfg = _make_config(filters={})
        _, meta = convert_monitors_to_sql_tests([_monitor(sql)], cfg)
        assert meta[0]["manual_review"] is True

    def test_multi_aggregate_flagged(self):
        sql = "SELECT COUNT(*), MAX(total) FROM orders"
        cfg = _make_config(filters={})
        _, meta = convert_monitors_to_sql_tests([_monitor(sql)], cfg)
        assert meta[0]["manual_review"] is True
        assert meta[0]["needs_wrapping"] is False


class TestIdCollisions:

    def test_same_name_does_not_collide(self):
        cfg = _make_config()
        a = _monitor("SELECT * FROM bizview.a WHERE x IS NULL", name="Null check")
        b = _monitor("SELECT * FROM bizview.b WHERE x IS NULL", name="Null check")
        a["uuid"], b["uuid"] = "aaaa-1111", "bbbb-2222"
        tests, meta = convert_monitors_to_sql_tests([a, b], cfg)
        ids = [t["id"] for t in tests]
        assert len(set(ids)) == 2
        assert ids == ["mc_migrated_null_check_aaaa1111", "mc_migrated_null_check_bbbb2222"]
        assert all(m.get("id_collision") for m in meta)

    def test_empty_name_uses_uuid(self):
        cfg = _make_config()
        m1 = _monitor("SELECT * FROM bizview.a WHERE x IS NULL", name=None, desc=None)
        m2 = _monitor("SELECT * FROM bizview.b WHERE x IS NULL", name="", desc="")
        m1["uuid"], m2["uuid"] = "1111", "2222"
        tests, _ = convert_monitors_to_sql_tests([m1, m2], cfg)
        assert [t["id"] for t in tests] == ["mc_migrated_1111", "mc_migrated_2222"]

    def test_ids_are_stable_with_collisions(self):
        cfg = _make_config()
        ms = [_monitor("SELECT * FROM bizview.a WHERE x IS NULL", name="dup") for _ in range(2)]
        ms[0]["uuid"], ms[1]["uuid"] = "u1", "u2"
        first, _ = convert_monitors_to_sql_tests([dict(m) for m in ms], cfg)
        second, _ = convert_monitors_to_sql_tests([dict(m) for m in ms], cfg)
        assert [t["id"] for t in first] == [t["id"] for t in second]

    def test_internal_uuid_key_not_in_payload(self):
        tests, _ = convert_monitors_to_sql_tests(
            [_monitor("SELECT * FROM bizview.a WHERE x IS NULL")], _make_config()
        )
        assert "_mc_uuid" not in tests[0]


class TestFilterConsistency:

    def test_entity_prefix_match_kept_even_if_text_pattern_misses(self):
        cfg = _make_config(
            filters={"entity_prefixes": ["db:sales."], "text_patterns": ["your_schema"]}
        )
        m = _monitor("SELECT * FROM sales.orders WHERE x IS NULL", entities=["db:sales.orders"])
        tests, _ = convert_monitors_to_sql_tests([m], cfg)
        assert len(tests) == 1

    def test_filtered_out_monitor_is_reported_not_silent(self):
        cfg = _make_config(filters={"text_patterns": ["bizview"]})
        m = _monitor("SELECT * FROM sales.orders WHERE x IS NULL", entities=["db:sales.orders"])
        tests, meta = convert_monitors_to_sql_tests([m], cfg)
        assert tests == []
        assert meta[0]["skipped"] is True
        assert "filters" in meta[0]["skip_reason"]
