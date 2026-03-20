"""Deploy SQL Tests to SYNQ via the BatchUpsertSqlTests REST endpoint.

Handles batching, error reporting, and idempotent upserts.
"""

from __future__ import annotations

from typing import Any

from rich.console import Console

from mc_to_synq.auth.synq import SynqClient
from mc_to_synq.config import AppConfig

console = Console()

SQL_TESTS_PATH = "/api/datachecks/sqltests/v1"


def deploy_sql_tests(
    client: SynqClient,
    sql_tests: list[dict],
    config: AppConfig,
) -> dict[str, Any]:
    """Deploy SQL tests in batches.

    Returns a summary dict with created, updated, and error counts.
    """
    batch_size = config.output.batch_size
    all_created: list[str] = []
    all_updated: list[str] = []
    all_errors: list[dict] = []

    total_batches = (len(sql_tests) + batch_size - 1) // batch_size

    for batch_start in range(0, len(sql_tests), batch_size):
        batch = sql_tests[batch_start : batch_start + batch_size]
        batch_num = (batch_start // batch_size) + 1

        console.print(
            f"  Batch {batch_num}/{total_batches} ({len(batch)} tests)..."
        )

        response = client.post(SQL_TESTS_PATH, data={"sql_tests": batch})

        if response.status_code in (200, 201):
            result = response.json()
            created = result.get("created_ids", result.get("createdIds", []))
            updated = result.get("updated_ids", result.get("updatedIds", []))
            errors = result.get("errors", [])

            all_created.extend(created)
            all_updated.extend(updated)
            all_errors.extend(errors)

            console.print(
                f"    Created: {len(created)}, Updated: {len(updated)}, "
                f"Errors: {len(errors)}"
            )
            for err in errors:
                console.print(
                    f"    [red]ERROR:[/red] {err.get('id')}: {err.get('reason')}"
                )
        else:
            console.print(f"    [red]FAILED: HTTP {response.status_code}[/red]")
            all_errors.append({
                "id": f"batch_{batch_num}",
                "reason": f"HTTP {response.status_code}: {response.text[:200]}",
            })

    return {
        "created": all_created,
        "updated": all_updated,
        "errors": all_errors,
    }


def list_sql_tests(client: SynqClient) -> list[dict]:
    """List all SQL tests from SYNQ."""
    response = client.get(SQL_TESTS_PATH)
    if response.status_code != 200:
        console.print(f"  [red]List failed: HTTP {response.status_code}[/red]")
        return []

    data = response.json()
    return data.get("sql_tests", data.get("sqlTests", []))
