"""Tests for config-driven monitor filtering."""

from mc_to_synq.config import AppConfig
from mc_to_synq.extract.filters import filter_monitors


def _cfg(**overrides) -> AppConfig:
    defaults = {
        "filters": {
            "entity_prefixes": ["mydb:analytics."],
            "text_patterns": ["analytics"],
        }
    }
    defaults.update(overrides)
    return AppConfig(**defaults)


def _monitor(entities=None, sql="", name="", description=""):
    return {
        "uuid": "test-uuid",
        "entities": entities or [],
        "sql": sql,
        "name": name,
        "description": description,
    }


class TestEntityPrefixFilter:
    """Entity prefix is the primary (strict) filter."""

    def test_exact_prefix_match(self):
        m = _monitor(entities=["mydb:analytics.orders"])
        matched, unmatched = filter_monitors([m], _cfg())
        assert len(matched) == 1

    def test_case_insensitive(self):
        m = _monitor(entities=["MYDB:ANALYTICS.orders"])
        matched, _ = filter_monitors([m], _cfg())
        assert len(matched) == 1

    def test_no_match(self):
        m = _monitor(entities=["otherdb:staging.raw_orders"])
        matched, unmatched = filter_monitors([m], _cfg())
        # Falls through to text pattern check
        assert len(unmatched) == 1

    def test_entities_from_custom_rule(self):
        m = _monitor()
        m["_custom_rule"] = {"entities": ["mydb:analytics.products"]}
        matched, _ = filter_monitors([m], _cfg())
        assert len(matched) == 1

    def test_entities_from_user_defined(self):
        m = _monitor()
        m["_user_defined"] = {"entities": ["mydb:analytics.customers"]}
        matched, _ = filter_monitors([m], _cfg())
        assert len(matched) == 1


class TestTextPatternFallback:
    """Text patterns are checked when entity prefix doesn't match."""

    def test_sql_match(self):
        m = _monitor(
            entities=["otherdb:raw.stuff"],
            sql="SELECT * FROM analytics.orders",
        )
        matched, _ = filter_monitors([m], _cfg())
        assert len(matched) == 1

    def test_name_match(self):
        m = _monitor(
            entities=["otherdb:raw.stuff"],
            name="analytics_order_check",
        )
        matched, _ = filter_monitors([m], _cfg())
        assert len(matched) == 1

    def test_description_match(self):
        m = _monitor(
            entities=["otherdb:raw.stuff"],
            description="Validates analytics layer",
        )
        matched, _ = filter_monitors([m], _cfg())
        assert len(matched) == 1

    def test_no_match_anywhere(self):
        m = _monitor(
            entities=["otherdb:raw.stuff"],
            sql="SELECT 1",
            name="raw_check",
            description="check raw table",
        )
        _, unmatched = filter_monitors([m], _cfg())
        assert len(unmatched) == 1


class TestEdgeCases:

    def test_no_filters_includes_all(self):
        cfg = AppConfig(filters={"entity_prefixes": [], "text_patterns": []})
        monitors = [_monitor(), _monitor()]
        matched, unmatched = filter_monitors(monitors, cfg)
        assert len(matched) == 2
        assert len(unmatched) == 0

    def test_empty_monitor_list(self):
        matched, unmatched = filter_monitors([], _cfg())
        assert matched == []
        assert unmatched == []

    def test_multiple_prefixes(self):
        cfg = AppConfig(
            filters={
                "entity_prefixes": ["db1:schema1.", "db2:schema2."],
                "text_patterns": [],
            }
        )
        m1 = _monitor(entities=["db1:schema1.table_a"])
        m2 = _monitor(entities=["db2:schema2.table_b"])
        m3 = _monitor(entities=["db3:other.table_c"])
        matched, unmatched = filter_monitors([m1, m2, m3], cfg)
        assert len(matched) == 2
        assert len(unmatched) == 1
