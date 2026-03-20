"""Generate SYNQ monitors-as-code YAML from classified MC monitors.

Handles type mapping, SQL-to-metric-aggregation conversion,
schedule mapping, and annotated YAML output with migration metadata.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Optional

import yaml
from rich.console import Console

from mc_to_synq.config import AppConfig

console = Console()


# ---------------------------------------------------------------------------
# MC -> SYNQ type classification
# ---------------------------------------------------------------------------

def classify_monitor(monitor: dict) -> dict[str, Any]:
    """Classify an MC monitor and determine its SYNQ target type.

    Returns dict with mc_type, synq_type, conversion_notes, needs_manual_review.
    """
    monitor_type = (monitor.get("monitorType") or "").upper()
    sql = monitor.get("sql", "") or ""

    result: dict[str, Any] = {
        "mc_type": monitor_type,
        "synq_type": None,
        "conversion_notes": [],
        "needs_manual_review": False,
    }

    type_map: dict[str, dict[str, Any]] = {
        "CUSTOM_SQL": {"synq_type": "custom_numeric"},
        "CUSTOM_RULE": {"synq_type": "custom_numeric"},
        "VALIDATION": {
            "synq_type": "custom_numeric",
            "note": "MC Validation -> SYNQ custom_numeric. Verify SQL.",
        },
        "METRIC": {
            "synq_type": "custom_numeric",
            "note": "MC Metric -> SYNQ custom_numeric. Verify metric_aggregation.",
        },
        "COMPARISON": {
            "synq_type": "custom_numeric",
            "manual": True,
            "note": "MC Comparison has no native SYNQ equivalent. Rewrite as custom SQL.",
        },
        "STATS": {
            "synq_type": "field_stats",
            "note": "MC Stats -> SYNQ field_stats. Verify field list and sensitivity.",
        },
        "FRESHNESS": {"synq_type": "freshness"},
        "VOLUME": {"synq_type": "volume"},
        "SCHEMA": {
            "synq_type": None,
            "note": "Schema monitor excluded from migration.",
        },
    }

    mapping = type_map.get(monitor_type)
    if mapping:
        result["synq_type"] = mapping.get("synq_type")
        result["needs_manual_review"] = mapping.get("manual", False)
        if mapping.get("note"):
            result["conversion_notes"].append(mapping["note"])
    else:
        result["synq_type"] = "custom_numeric"
        result["needs_manual_review"] = True
        result["conversion_notes"].append(
            f"Unknown MC type '{monitor_type}'. Defaulting to custom_numeric."
        )

    # SQL-level warnings for custom types
    if result["synq_type"] == "custom_numeric" and sql:
        if re.search(r"(EXCEPT|MINUS|NOT\s+IN|NOT\s+EXISTS)", sql, re.IGNORECASE):
            result["conversion_notes"].append(
                "Reconciliation query uses set operations. Wrap in COUNT(*) if needed."
            )
        if re.search(r"(JOIN|CROSS\s+JOIN|UNION)", sql, re.IGNORECASE):
            result["conversion_notes"].append(
                "Multi-table join detected. Verify monitored_id target."
            )

    return result


# ---------------------------------------------------------------------------
# SQL -> metric_aggregation
# ---------------------------------------------------------------------------

def _sql_to_metric_agg(sql: str) -> tuple[Optional[str], Optional[str]]:
    """Try to extract a metric_aggregation expression from MC SQL.

    Returns (expression, warning) -- expression is None if conversion fails.
    """
    if not sql:
        return None, "No SQL found"

    sql_clean = sql.strip().rstrip(";")

    simple_agg = re.match(
        r"^\s*SELECT\s+(COUNT|SUM|AVG|MIN|MAX)\s*\((.+?)\)\s+"
        r"(?:AS\s+\w+\s+)?FROM\s+",
        sql_clean,
        re.IGNORECASE | re.DOTALL,
    )
    if simple_agg:
        func = simple_agg.group(1).upper()
        expr = simple_agg.group(2).strip()
        return f"{func}({expr})", None

    return None, (
        "Complex SQL cannot be auto-converted to metric_aggregation. "
        "Options: rewrite as aggregate, use SYNQ API directly, or create as SQL Test."
    )


# ---------------------------------------------------------------------------
# Table reference extraction
# ---------------------------------------------------------------------------

def _extract_table_refs(monitor: dict) -> list[str]:
    """Extract table references from SQL and entity metadata."""
    tables: set[str] = set()
    sql = monitor.get("sql", "") or ""

    fq_pattern = (
        r"(?:FROM|JOIN|INTO|UPDATE|TABLE)\s+"
        r"([A-Za-z_]\w*\.[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)?)"
    )
    tables.update(re.findall(fq_pattern, sql, re.IGNORECASE))

    for key in ("_user_defined", "_custom_rule"):
        for e in monitor.get(key, {}).get("entities", []) or []:
            tables.add(str(e))

    return list(tables)


# ---------------------------------------------------------------------------
# Schedule mapping
# ---------------------------------------------------------------------------

def _map_schedule(monitor: dict) -> dict:
    """Map MC schedule to SYNQ schedule format."""
    custom_rule = monitor.get("_custom_rule", {})
    interval = custom_rule.get("scheduleConfig", {}).get("intervalMinutes")

    if interval:
        if interval <= 60:
            return {"hourly": 0}
        if interval <= 360:
            return {"every_6_hours": 0}
        if interval <= 720:
            return {"every_12_hours": 0}

    return {"daily": 0}


# ---------------------------------------------------------------------------
# YAML generation
# ---------------------------------------------------------------------------

def generate_yaml_export(
    classified_monitors: list[tuple[dict, dict]],
    config: AppConfig,
) -> dict[str, Any]:
    """Generate the full SYNQ monitors-as-code YAML structure.

    Args:
        classified_monitors: list of (monitor, classification) tuples
        config: app config

    Returns:
        Dict ready for YAML serialization.
    """
    yaml_output: dict[str, Any] = {
        "namespace": f"{config.snowflake.schema_name.lower()}-monitors",
        "defaults": {"severity": "ERROR"},
        "monitors": [],
    }

    auto_count = 0
    manual_count = 0

    for i, (monitor, classification) in enumerate(classified_monitors):
        if classification["synq_type"] is None:
            continue

        mon_def = _build_monitor_def(monitor, classification, i, config)
        clean = {k: v for k, v in mon_def.items() if not k.startswith("_")}

        if mon_def.get("_MANUAL_REVIEW") or classification.get("needs_manual_review"):
            clean["_NEEDS_MANUAL_REVIEW"] = True
            manual_count += 1
        else:
            auto_count += 1

        yaml_output["monitors"].append(clean)

    console.print(f"  Auto-converted: {auto_count}")
    console.print(f"  Manual review:  {manual_count}")

    return yaml_output


def write_yaml(data: dict, filepath: str) -> None:
    """Write YAML with a migration header."""

    class _Dumper(yaml.SafeDumper):
        pass

    def _str_repr(dumper: yaml.Dumper, val: str) -> Any:
        if "\n" in val:
            return dumper.represent_scalar("tag:yaml.org,2002:str", val, style="|")
        return dumper.represent_scalar("tag:yaml.org,2002:str", val)

    _Dumper.add_representer(str, _str_repr)

    header = (
        "# ==========================================================================\n"
        "# SYNQ Monitors-as-Code -- Migrated from Monte Carlo\n"
        f"# Generated: {datetime.now().isoformat()}\n"
        "#\n"
        "# NEXT STEPS:\n"
        "#   1. Update monitored_id values (check SYNQ UI for correct IDs)\n"
        "#   2. Review monitors marked _NEEDS_MANUAL_REVIEW\n"
        "#   3. Update timestamp columns per table\n"
        "#   4. Dry run:  synq-monitors plan -f <this_file>\n"
        "#   5. Deploy:   synq-monitors apply -f <this_file>\n"
        "# ==========================================================================\n\n"
    )

    yaml_str = yaml.dump(data, Dumper=_Dumper, default_flow_style=False, sort_keys=False, width=120)

    with open(filepath, "w") as f:
        f.write(header)
        f.write(yaml_str)


def _build_monitor_def(
    monitor: dict,
    classification: dict,
    index: int,
    config: AppConfig,
) -> dict[str, Any]:
    """Build a single SYNQ monitor YAML definition."""
    mc_name = monitor.get("name") or monitor.get("description") or f"monitor_{index}"
    sql = monitor.get("sql", "") or ""

    safe_name = re.sub(r"[^a-zA-Z0-9_]", "_", mc_name).lower().strip("_")
    safe_name = re.sub(r"_+", "_", safe_name)
    if not safe_name:
        safe_name = f"mc_migrated_{index}"

    tables = _extract_table_refs(monitor)
    primary_table = tables[0] if tables else "UNKNOWN_TABLE"

    mon_def: dict[str, Any] = {
        "name": safe_name,
        "type": classification["synq_type"],
        "monitored_id": f"SYNQ_CONNECTION::{primary_table}",
    }

    if classification["synq_type"] == "custom_numeric":
        metric_agg, agg_warning = _sql_to_metric_agg(sql)
        if metric_agg:
            mon_def["metric_aggregation"] = metric_agg
        else:
            mon_def["metric_aggregation"] = "COUNT(*)"
            mon_def["_MANUAL_REVIEW"] = True
            mon_def["_original_sql"] = sql
        mon_def["mode"] = {"fixed_thresholds": {"min": 0}}

    elif classification["synq_type"] == "freshness":
        mon_def["expression"] = "CHANGEME_TIMESTAMP_COLUMN"
        mon_def["_MANUAL_REVIEW"] = True

    elif classification["synq_type"] == "field_stats":
        mon_def["fields"] = ["_UPDATE_FIELD_LIST"]
        mon_def["_MANUAL_REVIEW"] = True
        mon_def["mode"] = {"anomaly_engine": {"sensitivity": "BALANCED"}}

    mon_def["schedule"] = _map_schedule(monitor)

    return mon_def
