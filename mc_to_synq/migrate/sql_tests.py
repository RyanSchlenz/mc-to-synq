"""Convert Monte Carlo SQL monitors to SYNQ SQL Tests.

SYNQ SQL Tests convention: a test PASSES if the query returns 0 rows.
If rows are returned, the test FAILS (same as dbt test conventions).

This module handles:
- SQL analysis to determine if wrapping is needed
- Jinja template detection and skip logic
- Stable ID generation for idempotent upserts
- Batch payload construction
"""

from __future__ import annotations

import re
from typing import Any, Optional

from rich.console import Console

from mc_to_synq.config import AppConfig

console = Console()


# ---------------------------------------------------------------------------
# SQL analysis
# ---------------------------------------------------------------------------

def _contains_jinja(sql: str) -> bool:
    """Check if SQL contains Jinja template variables."""
    return "{{" in sql or "{%" in sql


def _analyze_sql(sql: str) -> dict[str, Any]:
    """Analyze SQL to determine if/how it needs wrapping for SYNQ.

    Returns dict with:
        needs_wrapping: bool
        manual_review: bool
        note: optional explanation
    """
    sql_upper = sql.upper()
    is_cte = bool(re.match(r"^\s*WITH\s+", sql, re.IGNORECASE))

    # Already has HAVING -- likely already filters to violations
    if re.search(r"HAVING\s+", sql_upper):
        return {"needs_wrapping": False, "manual_review": False, "note": None}

    # CTE analysis
    if is_cte:
        final_select = re.search(r"\)\s*SELECT\s+", sql, re.IGNORECASE)
        if final_select:
            final_part = sql[final_select.start():]
            if re.search(r"\bWHERE\b", final_part, re.IGNORECASE):
                return {"needs_wrapping": False, "manual_review": False, "note": None}
            return {
                "needs_wrapping": False,
                "manual_review": True,
                "note": (
                    "CTE with no WHERE/HAVING on final SELECT. "
                    "May return rows even when passing. Add a filter clause."
                ),
            }
        return {
            "needs_wrapping": False,
            "manual_review": True,
            "note": "Complex CTE structure. Manual review required.",
        }

    # NOT IN / NOT EXISTS -- already violation-oriented
    if re.search(r"WHERE\s+.*\bNOT\s+(IN|EXISTS)\b", sql_upper, re.DOTALL):
        return {"needs_wrapping": False, "manual_review": False, "note": None}

    # Simple COUNT -- needs wrapping to return rows on failure
    if re.match(r"^\s*SELECT\s+COUNT\s*\(", sql, re.IGNORECASE):
        return {
            "needs_wrapping": True,
            "manual_review": False,
            "note": "Returns a count. Wrapped to return rows only on failure.",
        }

    return {"needs_wrapping": False, "manual_review": False, "note": None}


def _wrap_count_sql(sql: str) -> str:
    """Wrap a COUNT-based SQL query so it returns rows only on violation."""
    return (
        f"SELECT * FROM (\n{sql}\n) _mc_check\n"
        f"WHERE COALESCE(_mc_check.difference, 0) != 0\n"
        f"   OR COALESCE(_mc_check.source_count, 0) "
        f"!= COALESCE(_mc_check.target_count, 0)"
    )


# ---------------------------------------------------------------------------
# Conversion
# ---------------------------------------------------------------------------

def convert_monitors_to_sql_tests(
    monitors: list[dict],
    config: AppConfig,
) -> tuple[list[dict], list[dict]]:
    """Convert a list of MC monitors to SYNQ SQL Test payloads.

    Only processes monitors that have SQL. Skips disabled monitors
    and Jinja templates based on config.

    Returns:
        (sql_tests, metadata_list) - parallel lists of test payloads
        and conversion metadata.
    """
    sql_tests: list[dict] = []
    metadata_list: list[dict] = []

    id_prefix = config.sql_tests.id_prefix
    schedule = config.sql_tests.default_schedule
    severity = config.sql_tests.severity
    integration_id = config.synq.integration_id
    text_patterns = [p.lower() for p in config.filters.text_patterns]

    for i, monitor in enumerate(monitors):
        sql = (monitor.get("sql") or "").strip().rstrip(";")
        if not sql:
            continue

        # Check text patterns to ensure this monitor is relevant
        name = monitor.get("name", "") or ""
        desc = monitor.get("description", "") or ""
        entities = monitor.get("entities") or []
        all_text = (
            f"{sql} {name} {desc} {' '.join(str(e) for e in entities)}"
        ).lower()

        if text_patterns and not any(p in all_text for p in text_patterns):
            continue

        # Skip disabled monitors
        if config.sql_tests.skip_disabled:
            status = monitor.get("consolidatedMonitorStatus", "")
            if status == "DISABLED":
                metadata_list.append(_skip_metadata(monitor, "Disabled in MC"))
                continue

        # Skip Jinja templates
        if config.sql_tests.skip_jinja_templates and _contains_jinja(sql):
            metadata_list.append(
                _skip_metadata(monitor, "SQL contains Jinja template variables")
            )
            continue

        # Analyze and convert
        test, meta = _convert_single(
            monitor, i, sql, id_prefix, schedule, severity, integration_id
        )
        if test:
            sql_tests.append(test)
        metadata_list.append(meta)

    return sql_tests, metadata_list


def _convert_single(
    monitor: dict,
    index: int,
    sql: str,
    id_prefix: str,
    schedule: str,
    severity: str,
    integration_id: str,
) -> tuple[Optional[dict], dict]:
    """Convert a single MC monitor to a SYNQ SQL Test."""
    mc_uuid = monitor.get("uuid", f"unknown_{index}")
    mc_name = monitor.get("name", "") or ""
    mc_description = monitor.get("description", "") or ""
    mc_type = monitor.get("monitorType", "CUSTOM_SQL")

    test_name = mc_description if mc_description else mc_name
    if not test_name:
        test_name = f"Migrated Test {index}"

    # Generate stable ID
    safe_id = re.sub(r"[^a-zA-Z0-9_-]", "_", mc_name).lower().strip("_")
    safe_id = re.sub(r"_+", "_", safe_id)
    test_id = f"{id_prefix}{safe_id}"
    if len(test_id) > 80:
        test_id = test_id[:70] + "_" + mc_uuid[:8]

    # Analyze SQL
    analysis = _analyze_sql(sql)
    final_sql = _wrap_count_sql(sql) if analysis["needs_wrapping"] else sql

    sql_test: dict[str, Any] = {
        "id": test_id,
        "name": test_name,
        "description": mc_description,
        "sql_expression": final_sql,
        "severity": severity,
        "recurrence_rule": schedule,
        "save_failures": True,
        "annotations": [
            {"name": "source", "values": ["monte_carlo_migration"]},
        ],
    }

    if integration_id and integration_id != "CHANGEME":
        sql_test["platform"] = {"synq_integration_id": integration_id}

    metadata = {
        "mc_uuid": mc_uuid,
        "mc_name": test_name,
        "mc_type": mc_type,
        "entities": monitor.get("entities", []),
        "needs_wrapping": analysis["needs_wrapping"],
        "manual_review": analysis["manual_review"],
        "wrapping_note": analysis["note"],
        "original_sql_length": len(sql),
        "final_sql_length": len(final_sql),
        "skipped": False,
    }

    return sql_test, metadata


def _skip_metadata(monitor: dict, reason: str) -> dict:
    return {
        "mc_uuid": monitor.get("uuid", "unknown"),
        "mc_name": (
            monitor.get("description")
            or monitor.get("name")
            or "unknown"
        ),
        "mc_type": monitor.get("monitorType", ""),
        "skipped": True,
        "skip_reason": reason,
    }
