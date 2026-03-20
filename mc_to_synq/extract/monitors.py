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

def _discover_custom_rule_fields(client: MonteCarloClient) -> dict[str, Any]:
    """Discover available fields on CustomRule type.

    Returns dict with sql_field name and full field list, so we can
    build queries using only fields that actually exist.
    """
    result = client.query(INTROSPECT_CUSTOM_RULE)
    type_info = result.get("data", {}).get("__type")
    if not type_info:
        return {"sql_field": None, "fields": []}

    fields = [f["name"] for f in (type_info.get("fields") or [])]

    sql_field = None
    for candidate in ("customSql", "sql", "queryText", "customQuery", "query"):
        if candidate in fields:
            sql_field = candidate
            break

    return {"sql_field": sql_field, "fields": fields}


def _discover_ud_type(client: MonteCarloClient) -> dict[str, Any]:
    """Discover the shape of UserDefinedMonitorV2 (union vs concrete)."""
    result = client.query(INTROSPECT_USER_DEFINED_V2)
    type_info = result.get("data", {}).get("__type", {})
    return {
        "kind": type_info.get("kind"),
        "fields": [f["name"] for f in (type_info.get("fields") or [])],
        "possible_types": [t["name"] for t in (type_info.get("possibleTypes") or [])],
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
    cr_info = _discover_custom_rule_fields(client)
    sql_field = cr_info["sql_field"]
    cr_fields = cr_info["fields"]
    if sql_field:
        console.print(f"  SQL field on CustomRule: [cyan]{sql_field}[/cyan]")
    else:
        console.print("  [yellow]No SQL field found on CustomRule[/yellow]")
    if cr_fields:
        console.print(f"  CustomRule has {len(cr_fields)} fields")

    ud_info = _discover_ud_type(client)
    if ud_info["kind"]:
        console.print(
            f"  UserDefinedMonitorV2: {ud_info['kind']} "
            f"({len(ud_info['possible_types'])} subtypes, "
            f"{len(ud_info['fields'])} fields)"
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
    custom_rules = _fetch_custom_rules(client, sql_field, cr_fields)
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
    client: MonteCarloClient,
    sql_field: str | None,
    known_fields: list[str],
) -> dict[str, dict]:
    """Fetch custom rules, building query from introspected fields only.

    Only requests fields confirmed to exist on CustomRule, avoiding
    400 errors from stale field names.
    """
    rules: dict[str, dict] = {}

    # Build field list from what introspection confirmed exists
    # Core fields we always want (if they exist)
    wanted_scalars = [
        "uuid", "creatorId", "createdTime", "description",
        "isDeleted", "entities", "ruleType",
    ]
    # Object fields that need sub-selections
    wanted_objects = {
        "scheduleConfig": "{ intervalMinutes startTime }",
        "alertCondition": "{ operator threshold }",
    }

    field_lines = []
    for f in wanted_scalars:
        if f in known_fields:
            field_lines.append(f"                {f}")

    if sql_field and sql_field in known_fields:
        field_lines.append(f"                {sql_field}")

    for f, sub in wanted_objects.items():
        if f in known_fields:
            field_lines.append(f"                {f} {sub}")

    if not field_lines:
        # Introspection returned nothing useful -- use hardcoded minimal
        console.print("  [yellow]No introspected fields, using minimal query[/yellow]")
        try:
            result = client.query(QUERY_CUSTOM_RULES_MINIMAL)
        except Exception:
            return rules
    else:
        fields_str = "\n".join(field_lines)
        dynamic_query = f"""
        {{
          getCustomRules(first: 500) {{
            edges {{
              node {{
{fields_str}
              }}
            }}
          }}
        }}
        """
        try:
            result = client.query(dynamic_query)
        except Exception as e:
            console.print(f"  [yellow]Dynamic query failed ({e}), trying minimal...[/yellow]")
            # Fallback: minimal + just the SQL field
            if sql_field:
                fallback = f"""
                {{
                  getCustomRules(first: 500) {{
                    edges {{
                      node {{
                        uuid description isDeleted entities ruleType {sql_field}
                      }}
                    }}
                  }}
                }}
                """
                try:
                    result = client.query(fallback)
                except Exception:
                    result = client.query(QUERY_CUSTOM_RULES_MINIMAL)
            else:
                result = client.query(QUERY_CUSTOM_RULES_MINIMAL)

    edges = (
        result.get("data", {}).get("getCustomRules", {}).get("edges", [])
    )

    for edge in edges:
        node = edge.get("node", {})
        if node.get("uuid") and not node.get("isDeleted"):
            # Normalize SQL field name
            if sql_field and sql_field in node and node[sql_field]:
                node["sql"] = node[sql_field]
            rules[node["uuid"]] = node

    return rules


def _fetch_user_defined(
    client: MonteCarloClient, ud_info: dict[str, Any]
) -> dict[str, dict]:
    """Fetch user-defined monitors, building query from introspected fields."""
    ud: dict[str, dict] = {}

    # Core fields we want, mapped to possible renames
    # MC renamed description -> ruleDescription in some versions
    field_candidates = {
        "uuid": "uuid",
        "entities": "entities",
        "createdTime": "createdTime",
        "creatorId": "creatorId",
        "monitorType": "monitorType",
    }
    # Description field may have been renamed
    desc_candidates = ["description", "ruleDescription"]
    # Schedule field may have been removed
    schedule_candidates = ["scheduleConfig"]

    discovered = set(ud_info.get("fields") or [])

    if ud_info["kind"] == "UNION" and ud_info["possible_types"]:
        # Union type -- need inline fragments
        scalar_fields = []
        for wanted, name in field_candidates.items():
            if name in discovered:
                scalar_fields.append(name)
        for c in desc_candidates:
            if c in discovered:
                scalar_fields.append(c)
                break

        fields_str = " ".join(scalar_fields)
        fragments = ""
        for ptype in ud_info["possible_types"]:
            fragments += f"""
                ... on {ptype} {{ {fields_str} }}
            """
        query = f"""
        {{
          getAllUserDefinedMonitorsV2(first: 500) {{
            edges {{ node {{ {fragments} }} }}
          }}
        }}
        """
    elif discovered:
        # Concrete type with known fields -- build from introspection
        scalar_fields = []
        for wanted, name in field_candidates.items():
            if name in discovered:
                scalar_fields.append(name)
        for c in desc_candidates:
            if c in discovered:
                scalar_fields.append(c)
                break
        for c in schedule_candidates:
            if c in discovered:
                scalar_fields.append(f"{c} {{ intervalMinutes startTime }}")
                break

        fields_str = "\n                ".join(scalar_fields)
        query = f"""
        {{
          getAllUserDefinedMonitorsV2(first: 500) {{
            edges {{
              node {{
                {fields_str}
              }}
            }}
          }}
        }}
        """
    else:
        # No introspection data -- try hardcoded, expect it may fail
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
                # Normalize description field name
                if "ruleDescription" in node and "description" not in node:
                    node["description"] = node["ruleDescription"]
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