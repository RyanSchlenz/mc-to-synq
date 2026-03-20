"""Tests for MC -> SYNQ SQL test conversion logic."""

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
