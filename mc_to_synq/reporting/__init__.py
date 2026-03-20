"""Migration report generation.

Produces JSON reports summarizing extraction, conversion, and deployment
results for audit trails and leadership presentations.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from rich.console import Console

console = Console()


def generate_extraction_report(
    all_monitors: list[dict],
    matched_monitors: list[dict],
    unmatched_monitors: list[dict],
    output_dir: str,
) -> str:
    """Generate a report on the MC extraction and filtering step.

    Returns the path to the written report file.
    """
    report = {
        "generated_at": datetime.now().isoformat(),
        "source": "Monte Carlo",
        "extraction": {
            "total_monitors": len(all_monitors),
            "matched_by_filter": len(matched_monitors),
            "excluded": len(unmatched_monitors),
            "with_sql": sum(1 for m in matched_monitors if m.get("sql")),
            "with_entities": sum(1 for m in matched_monitors if m.get("entities")),
        },
        "type_breakdown": _type_breakdown(matched_monitors),
    }

    filepath = str(Path(output_dir) / "extraction_report.json")
    _write_json(report, filepath)
    return filepath


def generate_sql_test_report(
    sql_tests: list[dict],
    metadata: list[dict],
    deploy_result: dict[str, Any] | None,
    output_dir: str,
) -> str:
    """Generate a report on SQL test migration and deployment."""
    skipped = [m for m in metadata if m.get("skipped")]
    converted = [m for m in metadata if not m.get("skipped")]
    manual_review = [m for m in converted if m.get("manual_review")]
    wrapped = [m for m in converted if m.get("needs_wrapping")]

    report: dict[str, Any] = {
        "generated_at": datetime.now().isoformat(),
        "conversion": {
            "total_processed": len(metadata),
            "converted": len(sql_tests),
            "skipped": len(skipped),
            "needs_manual_review": len(manual_review),
            "wrapped_count": len(wrapped),
        },
        "skipped_detail": [
            {"name": m.get("mc_name"), "reason": m.get("skip_reason")}
            for m in skipped
        ],
    }

    if deploy_result:
        report["deployment"] = {
            "created": len(deploy_result.get("created", [])),
            "updated": len(deploy_result.get("updated", [])),
            "errors": len(deploy_result.get("errors", [])),
            "error_detail": deploy_result.get("errors", []),
        }

    filepath = str(Path(output_dir) / "sql_test_migration_report.json")
    _write_json(report, filepath)
    return filepath


def generate_ootb_report(
    entities_found: int,
    entities_eligible: int,
    definitions: list[dict],
    skipped_entities: list[dict],
    skipped_existing: list[str],
    deploy_result: dict[str, Any] | None,
    output_dir: str,
) -> str:
    """Generate a report on OOTB monitor generation and deployment."""
    freshness = sum(1 for d in definitions if "freshness" in d)
    volume = sum(1 for d in definitions if "volume" in d)
    duplicates = sum(1 for d in definitions if "custom_numeric" in d)

    report: dict[str, Any] = {
        "generated_at": datetime.now().isoformat(),
        "entities": {
            "discovered": entities_found,
            "eligible": entities_eligible,
            "skipped": len(skipped_entities),
        },
        "monitors": {
            "total_new": len(definitions),
            "freshness": freshness,
            "volume": volume,
            "duplicates": duplicates,
            "already_exist": len(skipped_existing),
        },
        "skipped_entities": skipped_entities,
    }

    if deploy_result:
        report["deployment"] = {
            "created": len(deploy_result.get("created_ids", [])),
            "errors": len(deploy_result.get("errors", [])),
            "error_detail": deploy_result.get("errors", []),
        }

    filepath = str(Path(output_dir) / "ootb_monitor_report.json")
    _write_json(report, filepath)
    return filepath


def _type_breakdown(monitors: list[dict]) -> dict[str, int]:
    breakdown: dict[str, int] = {}
    for m in monitors:
        mtype = (m.get("monitorType") or "UNKNOWN").upper()
        breakdown[mtype] = breakdown.get(mtype, 0) + 1
    return breakdown


def _write_json(data: dict, filepath: str) -> None:
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2, default=str)
    console.print(f"  Report saved to: [cyan]{filepath}[/cyan]")
