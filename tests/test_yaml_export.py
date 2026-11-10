"""Tests for monitors-as-code YAML export."""

import yaml

from mc_to_synq.config import AppConfig
from mc_to_synq.migrate.yaml_export import classify_monitor, generate_yaml_export, write_yaml


def _cfg():
    return AppConfig(snowflake={"schema": "MARTS"})


def test_null_schedule_config_does_not_crash(tmp_path):
    monitors = [
        {"uuid": "a", "name": "a", "monitorType": "CUSTOM_SQL",
         "sql": "SELECT COUNT(*) FROM db.s.t", "_custom_rule": {"scheduleConfig": None}},
        {"uuid": "b", "name": "b", "monitorType": "FRESHNESS", "_custom_rule": None},
        {"uuid": "c", "name": "c", "monitorType": "VOLUME", "_user_defined": {"entities": None}},
    ]
    classified = [(m, classify_monitor(m)) for m in monitors]
    data = generate_yaml_export(classified, _cfg())
    assert [m["schedule"] for m in data["monitors"]] == [{"daily": 0}] * 3
    out = tmp_path / "x.yml"
    write_yaml(data, str(out))
    assert yaml.safe_load(out.read_text())["namespace"] == "marts-monitors"


def test_schedule_mapping():
    m = {"name": "h", "monitorType": "CUSTOM_SQL", "sql": "SELECT 1",
         "_custom_rule": {"scheduleConfig": {"intervalMinutes": 60}}}
    data = generate_yaml_export([(m, classify_monitor(m))], _cfg())
    assert data["monitors"][0]["schedule"] == {"hourly": 0}
