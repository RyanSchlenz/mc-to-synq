"""Extract monitors from Monte Carlo via GraphQL API.

Handles schema introspection (MC's schema evolves), multi-source
extraction (getMonitors, getCustomRules, getAllUserDefinedMonitorsV2),
and merging into a unified monitor list.
"""

from __future__ import annotations

import json
from typing import Any

from rich.console import Console

from mc_to_synq.auth.monte_carlo import MonteCarloClient

console = Console()


# ---------------------------------------------------------------------------
# GraphQL queries
# ---------------------------------------------------------------------------

QUERY_ALL_MONITORS = """
{
  getMonitors {
    uuid
    name
    description
    scheduleType
    connectionId
    createdTime
    creatorId
    monitorType
    tags { name value }
    scheduleConfig { intervalMinutes startTime }
    prevExecutionTime
    consolidatedMonitorStatus
  }
}
"""

QUERY_CUSTOM_RULES = """
{
  getCustomRules(first: 500) {
    edges {
      node {
        uuid
        creatorId
        createdTime
        description
        isDeleted
        entities
        ruleType
        customSql
        scheduleConfig { intervalMinutes startTime }
        alertCondition { operator threshold }
      }
    }
  }
}
"""

QUERY_CUSTOM_RULES_MINIMAL = """
{
  getCustomRules(first: 500) {
    edges {
      node {
        uuid
        creatorId
        createdTime
        description
        isDeleted
        entities
        ruleType
        scheduleConfig { intervalMinutes startTime }
      }
    }
  }
}
"""

QUERY_USER_DEFINED = """
{
  getAllUserDefinedMonitorsV2(first: 500) {
    edges {
      node {
        uuid
        description
        monitorType
        entities
        scheduleConfig { intervalMinutes startTime }
        createdTime
        creatorId
      }
    }
  }
}
"""

QUERY_CONNECTIONS = """
{
  getUser {
    account {
      connections {
        uuid
        type
        warehouse { name }
      }
    }
  }
}
"""

# Schema introspection queries
INTROSPECT_CUSTOM_RULE = """
{
  __type(name: "CustomRule") {
    name
    fields { name type { name kind ofType { name kind } } }
  }
}
"""

INTROSPECT_USER_DEFINED_V2 = """
{
  __type(name: "UserDefinedMonitorV2") {
    name
    kind
    fields { name type { name kind ofType { name kind } } }
    possibleTypes { name }
  }
}
"""


# ---------------------------------------------------------------------------
# Schema discovery
# ---------------------------------------------------------------------------

def _discover_sql_field(client: MonteCarloClient) -> str | None:
    """Find the SQL field name on CustomRule (it varies across MC versions)."""
    result = client.query(INTROSPECT_CUSTOM_RULE)
    type_info = result.get("data", {}).get("__type")
    if not type_info:
        return None

    field_names = [f["name"] for f in type_info.get("fields", [])]
    for candidate in ("customSql", "sql", "queryText", "customQuery", "query"):
        if candidate in field_names:
            return candidate
    return None


def _discover_ud_type(client: MonteCarloClient) -> dict[str, Any]:
    """Discover the shape of UserDefinedMonitorV2 (union vs concrete)."""
    result = client.query(INTROSPECT_USER_DEFINED_V2)
    type_info = result.get("data", {}).get("__type", {})
    return {
        "kind": type_info.get("kind"),
        "fields": [f["name"] for f in type_info.get("fields", [])],
        "possible_types": [t["name"] for t in type_info.get("possibleTypes", [])],
    }


# ---------------------------------------------------------------------------
# Extraction
# ---------------------------------------------------------------------------

def extract_monitors(client: MonteCarloClient) -> tuple[list[dict], dict]:
    """Pull all monitors from Monte Carlo.

    Uses schema introspection to discover current field names, then
    builds queries dynamically. Falls back gracefully when queries
    fail due to schema changes.

    Returns:
        (monitors, connections) where monitors is a list of dicts
        and connections maps uuid -> connection info.
    """
    console.print("\n[bold]Discovering MC GraphQL schema...[/bold]")
    sql_field = _discover_sql_field(client)
    if sql_field:
        console.print(f"  SQL field on CustomRule: [cyan]{sql_field}[/cyan]")
    else:
        console.print("  [yellow]No SQL field found on CustomRule[/yellow]")

    ud_info = _discover_ud_type(client)
    if ud_info["kind"]:
        console.print(
            f"  UserDefinedMonitorV2: {ud_info['kind']} "
            f"({len(ud_info['possible_types'])} subtypes)"
        )

    monitors_by_uuid: dict[str, dict] = {}

    # 1. getMonitors (catalog view)
    console.print("\n  Fetching getMonitors...")
    try:
        result = client.query(QUERY_ALL_MONITORS)
        raw = result.get("data", {}).get("getMonitors", []) or []
        for m in raw:
            if m.get("uuid"):
                monitors_by_uuid[m["uuid"]] = m
        console.print(f"  Found {len(raw)} monitors")
    except Exception as e:
        console.print(f"  [yellow]getMonitors failed: {e}[/yellow]")

    # 2. Custom rules (primary source for SQL monitors)
    console.print("  Fetching getCustomRules...")
    custom_rules = _fetch_custom_rules(client, sql_field)
    console.print(
        f"  Found {len(custom_rules)} active custom rules "
        f"({sum(1 for r in custom_rules.values() if r.get('sql'))} with SQL)"
    )

    # 3. User-defined monitors
    console.print("  Fetching getAllUserDefinedMonitorsV2...")
    user_defined = _fetch_user_defined(client, ud_info)
    console.print(f"  Found {len(user_defined)} user-defined monitors")

    # 4. Connections
    console.print("  Fetching connections...")
    connections = _fetch_connections(client)
    console.print(f"  Found {len(connections)} connections")

    # 5. Merge everything
    _merge_custom_rules(monitors_by_uuid, custom_rules)
    _merge_user_defined(monitors_by_uuid, user_defined)

    monitors = list(monitors_by_uuid.values())

    with_sql = sum(1 for m in monitors if m.get("sql"))
    with_entities = sum(1 for m in monitors if m.get("entities"))

    console.print(f"\n  [bold]Extraction summary:[/bold]")
    console.print(f"    Total monitors:  {len(monitors)}")
    console.print(f"    With SQL:        {with_sql}")
    console.print(f"    With entities:   {with_entities}")

    return monitors, connections


# ---------------------------------------------------------------------------
# Fetch helpers
# ---------------------------------------------------------------------------

def _fetch_custom_rules(
    client: MonteCarloClient, sql_field: str | None
) -> dict[str, dict]:
    """Fetch custom rules, building query dynamically based on discovered SQL field."""
    rules: dict[str, dict] = {}

    if sql_field:
        dynamic_query = f"""
        {{
          getCustomRules(first: 500) {{
            edges {{
              node {{
                uuid
                creatorId
                createdTime
                description
                isDeleted
                entities
                ruleType
                {sql_field}
                scheduleConfig {{ intervalMinutes startTime }}
                alertCondition {{ operator threshold }}
              }}
            }}
          }}
        }}
        """
        try:
            result = client.query(dynamic_query)
        except Exception:
            result = client.query(QUERY_CUSTOM_RULES_MINIMAL)
    else:
        try:
            result = client.query(QUERY_CUSTOM_RULES)
        except Exception:
            result = client.query(QUERY_CUSTOM_RULES_MINIMAL)

    edges = (
        result.get("data", {}).get("getCustomRules", {}).get("edges", [])
    )

    for edge in edges:
        node = edge.get("node", {})
        if node.get("uuid") and not node.get("isDeleted"):
            # Normalize SQL field name
            if sql_field and sql_field in node:
                node["sql"] = node[sql_field]
            rules[node["uuid"]] = node

    return rules


def _fetch_user_defined(
    client: MonteCarloClient, ud_info: dict[str, Any]
) -> dict[str, dict]:
    """Fetch user-defined monitors, handling union vs concrete types."""
    ud: dict[str, dict] = {}

    if ud_info["kind"] == "UNION" and ud_info["possible_types"]:
        fragments = ""
        for ptype in ud_info["possible_types"]:
            fragments += f"""
                ... on {ptype} {{
                  uuid description entities createdTime creatorId
                }}
            """
        query = f"""
        {{
          getAllUserDefinedMonitorsV2(first: 500) {{
            edges {{ node {{ {fragments} }} }}
          }}
        }}
        """
    else:
        query = QUERY_USER_DEFINED

    try:
        result = client.query(query)
        edges = (
            result.get("data", {})
            .get("getAllUserDefinedMonitorsV2", {})
            .get("edges", [])
        )
        for edge in edges:
            node = edge.get("node", {})
            if node.get("uuid"):
                ud[node["uuid"]] = node
    except Exception as e:
        console.print(f"  [yellow]User-defined fetch failed: {e}[/yellow]")

    return ud


def _fetch_connections(client: MonteCarloClient) -> dict[str, dict]:
    """Fetch warehouse connections."""
    connections: dict[str, dict] = {}
    try:
        result = client.query(QUERY_CONNECTIONS)
        conns = (
            result.get("data", {})
            .get("getUser", {})
            .get("account", {})
            .get("connections", [])
        )
        for conn in conns:
            connections[conn["uuid"]] = conn
    except Exception as e:
        console.print(f"  [yellow]Connections fetch failed: {e}[/yellow]")
    return connections


# ---------------------------------------------------------------------------
# Merge helpers
# ---------------------------------------------------------------------------

def _merge_custom_rules(
    monitors: dict[str, dict], custom_rules: dict[str, dict]
) -> None:
    """Merge custom rule data into the monitor map (in place)."""
    for uuid, rule in custom_rules.items():
        if uuid in monitors:
            monitors[uuid]["_custom_rule"] = rule
            if rule.get("sql"):
                monitors[uuid]["sql"] = rule["sql"]
            if not monitors[uuid].get("entities"):
                monitors[uuid]["entities"] = rule.get("entities", [])
        else:
            monitors[uuid] = {
                "uuid": uuid,
                "name": rule.get("description", f"custom_rule_{uuid[:8]}"),
                "description": rule.get("description", ""),
                "monitorType": rule.get("ruleType", "CUSTOM_SQL"),
                "sql": rule.get("sql", ""),
                "entities": rule.get("entities", []),
                "creatorId": rule.get("creatorId"),
                "createdTime": rule.get("createdTime"),
                "_custom_rule": rule,
            }


def _merge_user_defined(
    monitors: dict[str, dict], user_defined: dict[str, dict]
) -> None:
    """Merge user-defined monitor data into the monitor map (in place)."""
    for uuid, ud in user_defined.items():
        if uuid in monitors:
            monitors[uuid]["_user_defined"] = ud
            if not monitors[uuid].get("entities"):
                monitors[uuid]["entities"] = ud.get("entities", [])
            if not monitors[uuid].get("monitorType"):
                monitors[uuid]["monitorType"] = ud.get("monitorType", "")
        else:
            monitors[uuid] = {
                "uuid": uuid,
                "name": ud.get("description", f"user_defined_{uuid[:8]}"),
                "description": ud.get("description", ""),
                "monitorType": ud.get("monitorType", ""),
                "entities": ud.get("entities", []),
                "creatorId": ud.get("creatorId"),
                "createdTime": ud.get("createdTime"),
                "_user_defined": ud,
            }
