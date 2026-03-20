"""Prefix-based bulk cleanup for migrated SYNQ resources.

Provides deterministic teardown of SQL tests (mc_migrated_ prefix)
and custom monitors (bv_ prefix) created by the migration tool.
"""

from __future__ import annotations

from rich.console import Console

from mc_to_synq.auth.synq import SynqClient
from mc_to_synq.config import AppConfig
from mc_to_synq.deploy.custom_monitors import list_monitors
from mc_to_synq.deploy.sql_tests import list_sql_tests

console = Console()

BATCH_DELETE_MONITORS_PATH = "/api/monitors/custom-monitors/v1/delete"
SQL_TESTS_PATH = "/api/datachecks/sqltests/v1"


# ---------------------------------------------------------------------------
# SQL test cleanup
# ---------------------------------------------------------------------------

def find_migrated_sql_tests(
    client: SynqClient,
    prefix: str,
) -> list[dict]:
    """List all SQL tests whose ID starts with the given prefix."""
    all_tests = list_sql_tests(client)
    return [t for t in all_tests if t.get("id", "").startswith(prefix)]


def delete_sql_tests(
    client: SynqClient,
    test_ids: list[str],
) -> tuple[int, list[str]]:
    """Delete SQL tests by ID.

    Returns (deleted_count, errors).
    """
    if not test_ids:
        return 0, []

    response = client.delete(SQL_TESTS_PATH, params={"ids": test_ids})

    if response.status_code in (200, 204):
        console.print(f"  Deleted {len(test_ids)} SQL tests.")
        return len(test_ids), []
    else:
        error = f"HTTP {response.status_code}: {response.text[:300]}"
        console.print(f"  [red]Delete failed: {error}[/red]")
        return 0, [error]


# ---------------------------------------------------------------------------
# Custom monitor cleanup
# ---------------------------------------------------------------------------

def find_prefixed_monitors(
    client: SynqClient,
    prefix: str,
) -> list[dict]:
    """List all custom monitors whose name starts with the given prefix."""
    all_monitors = list_monitors(client)
    return [m for m in all_monitors if m.get("name", "").startswith(prefix)]


def delete_monitors(
    client: SynqClient,
    monitor_ids: list[str],
    batch_size: int = 25,
) -> tuple[list[str], list[dict]]:
    """Delete custom monitors by ID in batches.

    Returns (deleted_ids, errors).
    """
    all_deleted: list[str] = []
    all_errors: list[dict] = []

    for batch_start in range(0, len(monitor_ids), batch_size):
        batch = monitor_ids[batch_start : batch_start + batch_size]
        batch_num = (batch_start // batch_size) + 1

        console.print(f"  Deleting batch {batch_num} ({len(batch)} monitors)...")

        response = client.post(
            BATCH_DELETE_MONITORS_PATH,
            data={"ids": batch},
        )

        if response.status_code in (200, 201):
            result = response.json()
            deleted = result.get("deleted_ids", result.get("deletedIds", []))
            all_deleted.extend(deleted)
            console.print(f"    Deleted: {len(deleted)}")
        else:
            all_errors.append({
                "batch": batch_num,
                "status": response.status_code,
                "error": response.text[:300],
            })
            console.print(
                f"    [red]FAILED: HTTP {response.status_code}[/red]"
            )

    return all_deleted, all_errors
