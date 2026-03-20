"""Config-driven monitor filtering.

Replaces the hardcoded BIZVIEW_PATTERNS and BIZVIEW_ENTITY_PREFIXES
with a pluggable filter system driven by the config file.
"""

from __future__ import annotations

from rich.console import Console

from mc_to_synq.config import AppConfig

console = Console()


def filter_monitors(
    monitors: list[dict],
    config: AppConfig,
) -> tuple[list[dict], list[dict]]:
    """Split monitors into matched and unmatched based on config filters.

    A monitor matches if ANY of its entities start with one of the
    configured entity_prefixes (primary, strict filter).

    If entity_prefixes yields no matches for a given monitor, the
    text_patterns are checked against SQL, name, and description
    as a fallback.

    Returns:
        (matched, unmatched) monitor lists.
    """
    prefixes = [p.lower() for p in config.filters.entity_prefixes]
    text_pats = [p.lower() for p in config.filters.text_patterns]

    if not prefixes and not text_pats:
        console.print(
            "[yellow]Warning:[/yellow] No filters configured. "
            "All monitors will be included.\n"
            "Set filters.entity_prefixes in your config to target specific schemas."
        )
        return monitors, []

    matched: list[dict] = []
    unmatched: list[dict] = []

    for monitor in monitors:
        if _matches_entity_prefix(monitor, prefixes):
            matched.append(monitor)
        elif _matches_text_patterns(monitor, text_pats):
            matched.append(monitor)
        else:
            unmatched.append(monitor)

    return matched, unmatched


def _collect_entities(monitor: dict) -> set[str]:
    """Collect all entity references from a monitor and its sub-records."""
    entities: set[str] = set()
    for e in monitor.get("entities") or []:
        entities.add(str(e).lower())
    for key in ("_user_defined", "_custom_rule"):
        detail = monitor.get(key, {})
        for e in detail.get("entities") or []:
            entities.add(str(e).lower())
    return entities


def _matches_entity_prefix(monitor: dict, prefixes: list[str]) -> bool:
    """Check if any monitor entity starts with a configured prefix."""
    if not prefixes:
        return False
    entities = _collect_entities(monitor)
    return any(
        entity.startswith(prefix)
        for entity in entities
        for prefix in prefixes
    )


def _matches_text_patterns(monitor: dict, patterns: list[str]) -> bool:
    """Check if monitor SQL, name, or description contains a text pattern."""
    if not patterns:
        return False
    sql = (monitor.get("sql") or "").lower()
    name = (monitor.get("name") or "").lower()
    desc = (monitor.get("description") or "").lower()
    entities_str = " ".join(str(e) for e in monitor.get("entities") or []).lower()
    combined = f"{sql} {name} {desc} {entities_str}"
    return any(pat in combined for pat in patterns)
