"""Generate OOTB monitors (freshness, volume, duplicates) for SYNQ.

Uses SYNQ's SchemasService and TableConstraintsService to resolve
timestamp columns and business keys per entity, then builds monitor
definitions for the BatchCreateMonitor API.
"""

from __future__ import annotations

import uuid as uuid_mod
from typing import Any, Optional

from rich.console import Console

from mc_to_synq.auth.synq import SynqClient
from mc_to_synq.config import AppConfig

console = Console()

# ---------------------------------------------------------------------------
# SYNQ API paths
# ---------------------------------------------------------------------------
ENTITY_SEARCH_PATH = "/api/entities/v1/search"
GET_SCHEMA_PATH = "/api/schema/v1"
GET_CONSTRAINTS_PATH = "/api/constraints/v1/table-constraints"


# ---------------------------------------------------------------------------
# Entity discovery
# ---------------------------------------------------------------------------

def discover_entities(
    client: SynqClient,
    config: AppConfig,
) -> list[dict]:
    """Search SYNQ for all entities matching the configured schema.

    Pages through the entity search API and filters to allowed types.
    """
    sf = config.snowflake
    search_query = f"{sf.database} {sf.schema_name}"
    allowed_types = set(config.ootb.allowed_entity_types)

    all_hits: list[dict] = []
    page = 0

    while True:
        r = client.get(
            ENTITY_SEARCH_PATH,
            params={
                "query": search_query,
                "limitPerPage": 50,
                "page": page,
            },
        )

        if r.status_code != 200:
            console.print(f"  [red]Entity search failed: HTTP {r.status_code}[/red]")
            break

        hits = r.json().get("hits", [])
        if not hits or page > 20:
            break

        all_hits.extend(hits)
        page += 1

    # Deduplicate and filter
    schema_upper = sf.schema_name.upper()
    seen: set[str] = set()
    entities: list[dict] = []

    for hit in all_hits:
        entity_type = hit.get("entityType", "")
        synq_path = hit.get("synqPath", "")
        name = hit.get("name", "")

        if schema_upper not in synq_path.upper():
            continue
        if entity_type not in allowed_types:
            continue

        table_name = synq_path.split("::")[-1] if "::" in synq_path else name
        if table_name in seen:
            continue

        seen.add(table_name)
        entities.append({
            "table_name": table_name,
            "entity_type": entity_type,
            "synq_path": synq_path,
        })

    return entities


# ---------------------------------------------------------------------------
# Column / constraint resolution
# ---------------------------------------------------------------------------

def _sf_id_params(config: AppConfig, table_name: str) -> dict[str, str]:
    """Build Snowflake Identifier query params."""
    sf = config.snowflake
    return {
        "id.snowflakeTable.account": sf.account,
        "id.snowflakeTable.database": sf.database,
        "id.snowflakeTable.schema": sf.schema_name,
        "id.snowflakeTable.table": table_name,
    }


def resolve_timestamp_column(
    client: SynqClient,
    config: AppConfig,
    table_name: str,
) -> tuple[Optional[str], list[str]]:
    """Find the best timestamp column for an entity.

    Checks columns against the configured candidates in priority order.
    Returns (column_name, all_columns) or (None, all_columns).
    """
    r = client.get(GET_SCHEMA_PATH, params=_sf_id_params(config, table_name))

    if r.status_code != 200:
        return None, []

    columns = [
        col.get("name", "").upper()
        for col in r.json().get("schema", {}).get("columns", [])
        if col.get("name")
    ]

    for candidate in config.ootb.timestamp_columns:
        if candidate.upper() in columns:
            return candidate, columns

    return None, columns


def resolve_business_key(
    client: SynqClient,
    config: AppConfig,
    table_name: str,
) -> tuple[Optional[list[str]], Optional[str]]:
    """Resolve PK or unique key columns for duplicate detection.

    Returns (key_columns, constraint_type) or (None, None).
    """
    r = client.get(GET_CONSTRAINTS_PATH, params=_sf_id_params(config, table_name))

    if r.status_code != 200:
        return None, None

    constraints = r.json().get("constraints", [])
    target_types = config.ootb.key_constraint_types

    for target_type in target_types:
        for constraint in constraints:
            ctype = constraint.get("type", "")
            raw_type = constraint.get("rawType", "")
            columns = constraint.get("columns", [])

            type_match = False
            if isinstance(ctype, str) and ctype == target_type:
                type_match = True
            elif isinstance(ctype, int):
                if "PRIMARY_KEY" in target_type and ctype == 1:
                    type_match = True
                elif "UNIQUE" in target_type and ctype == 3:
                    type_match = True
            elif "PRIMARY" in raw_type.upper() and "PRIMARY_KEY" in target_type:
                type_match = True
            elif "UNIQUE" in raw_type.upper() and "UNIQUE" in target_type:
                type_match = True

            if type_match and columns:
                return columns, target_type

    return None, None


# ---------------------------------------------------------------------------
# Monitor definition builders
# ---------------------------------------------------------------------------

def _build_sf_identifier(config: AppConfig, table_name: str) -> dict:
    """Build a Snowflake Identifier object for monitor creation."""
    sf = config.snowflake
    return {
        "snowflakeTable": {
            "account": sf.account,
            "database": sf.database,
            "schema": sf.schema_name,
            "table": table_name,
        }
    }


def build_freshness_def(
    config: AppConfig, table_name: str, safe_name: str, ts_col: str
) -> dict:
    return {
        "id": str(uuid_mod.uuid4()),
        "monitored_id": _build_sf_identifier(config, table_name),
        "name": f"{config.ootb.monitor_prefix}freshness_{safe_name}",
        "description": f"OOTB freshness monitor for {config.snowflake.schema_name}.{table_name}",
        "severity": config.ootb.severity,
        "source": "SOURCE_API",
        "time_partitioning": {"expression": ts_col},
        "freshness": {"expression": ts_col},
        "anomaly_engine": {"sensitivity": config.ootb.sensitivity},
        "daily": {"minutes_since_midnight": config.ootb.schedule_minutes_utc},
    }


def build_volume_def(
    config: AppConfig, table_name: str, safe_name: str, ts_col: str
) -> dict:
    return {
        "id": str(uuid_mod.uuid4()),
        "monitored_id": _build_sf_identifier(config, table_name),
        "name": f"{config.ootb.monitor_prefix}volume_{safe_name}",
        "description": f"OOTB volume monitor for {config.snowflake.schema_name}.{table_name}",
        "severity": config.ootb.severity,
        "source": "SOURCE_API",
        "time_partitioning": {"expression": ts_col},
        "volume": {},
        "anomaly_engine": {"sensitivity": config.ootb.sensitivity},
        "daily": {"minutes_since_midnight": config.ootb.schedule_minutes_utc},
    }


def build_duplicates_def(
    config: AppConfig,
    table_name: str,
    safe_name: str,
    ts_col: str,
    key_columns: list[str],
) -> dict:
    sf = config.snowflake
    fqn = f"{sf.database}.{sf.schema_name}.{table_name}"
    key_cols_str = ", ".join(key_columns)
    sql = (
        f"SELECT COUNT(*) FROM ("
        f"SELECT {key_cols_str} "
        f"FROM {fqn} "
        f"GROUP BY {key_cols_str} "
        f"HAVING COUNT(*) > 1"
        f")"
    )
    return {
        "id": str(uuid_mod.uuid4()),
        "monitored_id": _build_sf_identifier(config, table_name),
        "name": f"{config.ootb.monitor_prefix}duplicates_{safe_name}",
        "description": (
            f"Duplicate key detection for {sf.schema_name}.{table_name} "
            f"on ({key_cols_str})"
        ),
        "severity": config.ootb.severity,
        "source": "SOURCE_API",
        "time_partitioning": {"expression": ts_col},
        "custom_numeric": {"metric_aggregation": sql},
        "fixed_thresholds": {"max": {"value": 0}},
        "daily": {"minutes_since_midnight": config.ootb.schedule_minutes_utc},
    }


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def resolve_and_build(
    client: SynqClient,
    config: AppConfig,
    entities: list[dict],
    existing_names: Optional[set[str]] = None,
) -> tuple[list[dict], list[dict], list[dict]]:
    """Resolve metadata per entity and build monitor definitions.

    Returns:
        (definitions, skipped_entities, skipped_existing_names)
    """
    if existing_names is None:
        existing_names = set()

    definitions: list[dict] = []
    skipped: list[dict] = []
    skipped_existing: list[str] = []
    prefix = config.ootb.monitor_prefix

    for i, entity in enumerate(entities):
        table_name = entity["table_name"]
        tag = f"  [{i+1:3d}/{len(entities)}] {table_name:40s}"

        ts_col, all_columns = resolve_timestamp_column(client, config, table_name)

        if not ts_col:
            reason = "no timestamp col" if all_columns else "no schema"
            skipped.append({
                "table_name": table_name,
                "entity_type": entity.get("entity_type", ""),
                "columns_found": len(all_columns),
                "reason": reason,
            })
            console.print(f"{tag} -> [yellow]SKIP ({reason})[/yellow]")
            continue

        key_columns, constraint_type = resolve_business_key(client, config, table_name)
        safe_name = table_name.lower().replace(" ", "_").replace("-", "_")
        monitored_id = _build_sf_identifier(config, table_name)

        # Freshness
        fname = f"{prefix}freshness_{safe_name}"
        if fname in existing_names:
            skipped_existing.append(fname)
        else:
            definitions.append(
                build_freshness_def(config, table_name, safe_name, ts_col)
            )

        # Volume
        vname = f"{prefix}volume_{safe_name}"
        if vname in existing_names:
            skipped_existing.append(vname)
        else:
            definitions.append(
                build_volume_def(config, table_name, safe_name, ts_col)
            )

        # Duplicates (only if PK/unique key exists)
        if key_columns:
            dname = f"{prefix}duplicates_{safe_name}"
            if dname in existing_names:
                skipped_existing.append(dname)
            else:
                definitions.append(
                    build_duplicates_def(config, table_name, safe_name, ts_col, key_columns)
                )

        key_str = f"key=({', '.join(key_columns)})" if key_columns else "key=NONE"
        console.print(f"{tag} -> ts={ts_col}  {key_str}")

    return definitions, skipped, skipped_existing
