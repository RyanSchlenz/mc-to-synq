"""Tests for MC -> SYNQ monitor type classification."""

from mc_to_synq.migrate.yaml_export import classify_monitor


def _monitor(monitor_type, sql=""):
    return {"monitorType": monitor_type, "sql": sql}


class TestClassification:

    def test_custom_sql_maps_to_custom_numeric(self):
        result = classify_monitor(_monitor("CUSTOM_SQL"))
        assert result["synq_type"] == "custom_numeric"
        assert not result["needs_manual_review"]

    def test_custom_rule_maps_to_custom_numeric(self):
        result = classify_monitor(_monitor("CUSTOM_RULE"))
        assert result["synq_type"] == "custom_numeric"

    def test_validation_maps_to_custom_numeric(self):
        result = classify_monitor(_monitor("VALIDATION"))
        assert result["synq_type"] == "custom_numeric"
        assert len(result["conversion_notes"]) > 0

    def test_metric_maps_to_custom_numeric(self):
        result = classify_monitor(_monitor("METRIC"))
        assert result["synq_type"] == "custom_numeric"

    def test_comparison_needs_manual_review(self):
        result = classify_monitor(_monitor("COMPARISON"))
        assert result["synq_type"] == "custom_numeric"
        assert result["needs_manual_review"] is True

    def test_stats_maps_to_field_stats(self):
        result = classify_monitor(_monitor("STATS"))
        assert result["synq_type"] == "field_stats"

    def test_freshness_passes_through(self):
        result = classify_monitor(_monitor("FRESHNESS"))
        assert result["synq_type"] == "freshness"

    def test_volume_passes_through(self):
        result = classify_monitor(_monitor("VOLUME"))
        assert result["synq_type"] == "volume"

    def test_schema_excluded(self):
        result = classify_monitor(_monitor("SCHEMA"))
        assert result["synq_type"] is None
        assert not result["needs_manual_review"]

    def test_unknown_type_defaults_with_review(self):
        result = classify_monitor(_monitor("SOMETHING_NEW"))
        assert result["synq_type"] == "custom_numeric"
        assert result["needs_manual_review"] is True

    def test_reconciliation_sql_noted(self):
        result = classify_monitor(
            _monitor("CUSTOM_SQL", sql="SELECT * FROM a EXCEPT SELECT * FROM b")
        )
        assert any("set operations" in n for n in result["conversion_notes"])

    def test_join_sql_noted(self):
        result = classify_monitor(
            _monitor("CUSTOM_SQL", sql="SELECT * FROM a JOIN b ON a.id = b.id")
        )
        assert any("join" in n.lower() for n in result["conversion_notes"])

    def test_case_insensitive_type(self):
        result = classify_monitor(_monitor("custom_sql"))
        assert result["synq_type"] == "custom_numeric"
