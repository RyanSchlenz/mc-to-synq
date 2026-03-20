"""Deploy custom monitors to SYNQ via the BatchCreateMonitor REST endpoint.

Handles batching, error collection, and listing existing monitors.
"""

from __future__ import annotations

from typing import Any

from rich.console import Console

from mc_to_synq.auth.synq import SynqClient
from mc_to_synq.config import AppConfig

console = Console()

BATCH_CREATE_PATH = "/api/monitors/custom-monitors/v1"
LIST_MONITORS_PATH = "/api/monitors/custom-monitors/v1"


def deploy_monitors(
    client: SynqClient,
    definitions: list[dict],
    config: AppConfig,
) -> dict[str, Any]:
    """Deploy monitors in batches via BatchCreateMonitor.

    Returns summary with created_ids and errors.
    """
    batch_size = config.output.batch_size
    all_created: list[str] = []
    all_errors: list[dict] = []

    total_batches = (len(definitions) + batch_size - 1) // batch_size

    for batch_start in range(0, len(definitions), batch_size):
        batch = definitions[batch_start : batch_start + batch_size]
        batch_num = (batch_start // batch_size) + 1

        console.print(
            f"  Batch {batch_num}/{total_batches} ({len(batch)} monitors)..."
        )

        response = client.post(BATCH_CREATE_PATH, data={"monitors": batch})

        if response.status_code in (200, 201):
            result = response.json()
            created = result.get(
                "created_monitor_ids", result.get("createdMonitorIds", [])
            )
            all_created.extend(created)
            console.print(f"    Created: {len(created)}")
        else:
            console.print(f"    [red]FAILED: HTTP {response.status_code}[/red]")
            all_errors.append({
                "batch": batch_num,
                "status": response.status_code,
                "error": response.text[:300],
                "monitor_names": [d.get("name", "?") for d in batch],
            })

    return {"created_ids": all_created, "errors": all_errors}


def list_monitors(client: SynqClient) -> list[dict]:
    """List all custom monitors from SYNQ."""
    response = client.get(LIST_MONITORS_PATH)
    if response.status_code != 200:
        console.print(f"  [red]List failed: HTTP {response.status_code}[/red]")
        return []
    return response.json().get("monitors", [])


def get_existing_monitor_names(
    client: SynqClient, prefix: str
) -> set[str]:
    """Get names of existing monitors matching a prefix."""
    all_monitors = list_monitors(client)
    return {
        m.get("name", "")
        for m in all_monitors
        if m.get("name", "").startswith(prefix)
    }
