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
from mc_to_synq.extract.filters import _matches_entity_prefix, _matches_text_patterns

console = Console()


# ---------------------------------------------------------------------------
# SQL analysis
# ---------------------------------------------------------------------------

def _contains_jinja(sql: str) -> bool:
    """Check if SQL contains Jinja template variables."""
    return "{{" in sql or "{%" in sql


def _top_level_select_items(sql: str) -> list[tuple[int, int]] | None:
    """Return (start, end) spans of the outermost SELECT list items.

    Scans the statement while tracking parentheses and quotes so that
    commas and FROM keywords inside subqueries are ignored. Returns None
    if the statement does not start with SELECT.
    """
    m = re.match(r"\s*SELECT\s+(?:DISTINCT\s+)?", sql, re.IGNORECASE)
    if not m:
        return None

    items: list[tuple[int, int]] = []
    depth = 0
    quote: str | None = None
    item_start = m.end()
    i = item_start
    n = len(sql)
    while i < n:
        ch = sql[i]
        if quote:
            if ch == quote:
                quote = None
        elif ch in ("'", '"'):
            quote = ch
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif depth == 0 and ch == ",":
            items.append((item_start, i))
            item_start = i + 1
        elif (
            depth == 0
            and ch.isspace()
            and re.match(r"\s+FROM\b", sql[i:], re.IGNORECASE)
        ):
            break
        i += 1
    items.append((item_start, i))
    return items


_ALIAS_RE = re.compile(r"(?:\s+AS)?\s+([A-Za-z_]\w*)\s*$", re.IGNORECASE)


def _split_alias(item: str) -> tuple[str, str | None]:
    """Split a select item into (expression, alias)."""
    item = item.strip()
    m = _ALIAS_RE.search(item)
    if m and not item[: m.start()].rstrip().upper().endswith(("SELECT", "DISTINCT")):
        expr = item[: m.start()].strip()
        if expr:
            return expr, m.group(1)
    return item, None


def _is_single_call(expr: str, func: str) -> bool:
    """True if expr is exactly FUNC( ... ) with the parens balanced to the end."""
    m = re.match(rf"{func}\s*\(", expr, re.IGNORECASE)
    if not m:
        return False
    depth = 0
    for j in range(m.end() - 1, len(expr)):
        if expr[j] == "(":
            depth += 1
        elif expr[j] == ")":
            depth -= 1
            if depth == 0:
                return expr[j + 1 :].strip() == ""
    return False


_RECON_COLUMNS = ("difference", "source_count", "target_count")


def _plan_count_wrap(sql: str) -> dict[str, Any] | None:
    """Decide how to wrap a query whose outer SELECT returns aggregate values.

    Returns None if the outer SELECT is not an aggregate-only result.
    """
    spans = _top_level_select_items(sql)
    if not spans:
        return None

    items = [(_split_alias(sql[a:b]), (a, b)) for a, b in spans]

    # Single COUNT(...) column: wrap and compare to zero.
    if len(items) == 1:
        (expr, alias), (_, end) = items[0]
        if _is_single_call(expr, "COUNT"):
            if alias:
                inner = sql
            else:
                alias = "_mc_count"
                inner = sql[:end].rstrip() + f" AS {alias}" + sql[end:]
            return {"kind": "count", "inner": inner, "column": alias}

    # Reconciliation style: source_count / target_count / difference columns.
    aliases = {a.lower() for (_, a), _ in items if a}
    if "difference" in aliases or {"source_count", "target_count"} <= aliases:
        cols = [c for c in _RECON_COLUMNS if c in aliases]
        return {"kind": "recon", "inner": sql, "columns": cols}

    # Some other aggregate-only select (several COUNT/SUM columns, etc.).
    if items and all(
        re.match(r"(COUNT|SUM|AVG|MIN|MAX)\s*\(", e, re.IGNORECASE)
        for (e, _), _ in items
    ) and not re.search(r"\bGROUP\s+BY\b", sql, re.IGNORECASE):
        return {"kind": "unknown_aggregate"}

    return None


def _analyze_sql(sql: str) -> dict[str, Any]:
    """Analyze SQL to determine if/how it needs wrapping for SYNQ.

    The outer SELECT is checked first: a query that returns a COUNT
    always returns exactly one row, so it must be wrapped regardless of
    any HAVING or NOT EXISTS inside it.

    Returns dict with:
        needs_wrapping: bool
        manual_review: bool
        note: optional explanation
        wrapped_sql: the SQL to deploy when needs_wrapping is True
    """
    is_cte = bool(re.match(r"^\s*WITH\s+", sql, re.IGNORECASE))

    # CTE analysis
    if is_cte:
        final_select = re.search(r"\)\s*SELECT\s+", sql, re.IGNORECASE)
        if final_select:
            final_part = sql[final_select.start() + 1 :]
            if _plan_count_wrap(final_part):
                return {
                    "needs_wrapping": False,
                    "manual_review": True,
                    "note": (
                        "CTE whose final SELECT returns an aggregate. It returns one "
                        "row even when passing. Rewrite with a HAVING/WHERE filter."
                    ),
                }
            if re.search(r"\b(WHERE|HAVING)\b", final_part, re.IGNORECASE):
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

    plan = _plan_count_wrap(sql)
    if plan and plan["kind"] == "count":
        col = f"_mc_check.{plan['column']}"
        return {
            "needs_wrapping": True,
            "manual_review": False,
            "note": "Returns a count. Wrapped to return rows only when the count is not 0.",
            "wrapped_sql": (
                f"SELECT * FROM (\n{plan['inner']}\n) _mc_check\n"
                f"WHERE COALESCE({col}, 0) != 0"
            ),
        }
    if plan and plan["kind"] == "recon":
        conds = []
        if "difference" in plan["columns"]:
            conds.append("COALESCE(_mc_check.difference, 0) != 0")
        if "source_count" in plan["columns"] and "target_count" in plan["columns"]:
            conds.append(
                "COALESCE(_mc_check.source_count, 0) != COALESCE(_mc_check.target_count, 0)"
            )
        return {
            "needs_wrapping": True,
            "manual_review": False,
            "note": "Reconciliation counts. Wrapped to return rows only on mismatch.",
            "wrapped_sql": (
                f"SELECT * FROM (\n{plan['inner']}\n) _mc_check\nWHERE "
                + "\n   OR ".join(conds)
            ),
        }
    if plan and plan["kind"] == "unknown_aggregate":
        return {
            "needs_wrapping": False,
            "manual_review": True,
            "note": (
                "Returns aggregate values in one row, so it would always fail in SYNQ. "
                "Rewrite to return rows only on violation."
            ),
        }

    # HAVING on the outer query already filters to violations
    if re.search(r"\bHAVING\s+", sql, re.IGNORECASE):
        return {"needs_wrapping": False, "manual_review": False, "note": None}

    # NOT IN / NOT EXISTS -- already violation-oriented
    if re.search(r"WHERE\s+.*\bNOT\s+(IN|EXISTS)\b", sql, re.IGNORECASE | re.DOTALL):
        return {"needs_wrapping": False, "manual_review": False, "note": None}

    return {"needs_wrapping": False, "manual_review": False, "note": None}


def _wrap_count_sql(sql: str) -> str:
    """Wrap an aggregate SQL query so it returns rows only on violation.

    Returns the SQL unchanged if it is not a COUNT or reconciliation query.
    """
    analysis = _analyze_sql(sql)
    return analysis.get("wrapped_sql", sql) if analysis["needs_wrapping"] else sql


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
    prefixes = [p.lower() for p in config.filters.entity_prefixes]
    text_patterns = [p.lower() for p in config.filters.text_patterns]

    for i, monitor in enumerate(monitors):
        sql = (monitor.get("sql") or "").strip().rstrip(";")
        if not sql:
            continue

        # Apply the same entity-prefix-then-text-pattern rule as extraction,
        # so a monitor matched by entity prefix is never dropped here.
        if (prefixes or text_patterns) and not (
            _matches_entity_prefix(monitor, prefixes)
            or _matches_text_patterns(monitor, text_patterns)
        ):
            metadata_list.append(
                _skip_metadata(monitor, "Does not match configured filters")
            )
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

    _dedupe_test_ids(sql_tests, metadata_list)
    return sql_tests, metadata_list


def _dedupe_test_ids(
    sql_tests: list[dict[str, Any]], metadata_list: list[dict[str, Any]]
) -> None:
    """Make test IDs unique so one upsert never overwrites another.

    IDs are derived from the MC name, and MC does not enforce unique names.
    Every test whose ID collides gets the MC uuid appended, which keeps the
    ID stable across runs for the same export.
    """
    counts: dict[str, int] = {}
    for t in sql_tests:
        counts[t["id"]] = counts.get(t["id"], 0) + 1

    seen: set[str] = set()
    meta_by_uuid = {m.get("mc_uuid"): m for m in metadata_list if not m.get("skipped")}
    for t in sql_tests:
        mc_uuid = t.pop("_mc_uuid")
        if counts[t["id"]] > 1:
            suffix = re.sub(r"[^a-zA-Z0-9]", "", mc_uuid).lower()
            base = t["id"][: 80 - len(suffix) - 1].rstrip("_")
            t["id"] = f"{base}_{suffix}"
            if mc_uuid in meta_by_uuid:
                meta_by_uuid[mc_uuid]["id_collision"] = True
        if t["id"] in seen:
            # Should not happen with distinct uuids; fail loudly rather than overwrite.
            raise ValueError(f"Duplicate SQL test id after dedupe: {t['id']}")
        seen.add(t["id"])


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
    if not safe_id:
        safe_id = re.sub(r"[^a-zA-Z0-9]", "", str(mc_uuid)).lower()
    test_id = f"{id_prefix}{safe_id}"
    if len(test_id) > 80:
        test_id = test_id[:70] + "_" + mc_uuid[:8]

    # Analyze SQL
    analysis = _analyze_sql(sql)
    final_sql = analysis["wrapped_sql"] if analysis["needs_wrapping"] else sql

    sql_test: dict[str, Any] = {
        "_mc_uuid": str(mc_uuid),
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
        "mc_uuid": str(mc_uuid),
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
