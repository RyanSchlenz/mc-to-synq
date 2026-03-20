"""mc-to-synq CLI.

Single entry point for migrating monitors from Monte Carlo to SYNQ.

Commands:
    mc-to-synq init                 Generate a config file
    mc-to-synq extract              Pull monitors from Monte Carlo
    mc-to-synq migrate sql-tests    Convert MC SQL monitors to SYNQ SQL Tests
    mc-to-synq migrate ootb         Generate OOTB freshness/volume/duplicate monitors
    mc-to-synq migrate yaml         Export monitors-as-code YAML
    mc-to-synq deploy sql-tests     Push SQL Tests to SYNQ
    mc-to-synq deploy ootb          Push OOTB monitors to SYNQ
    mc-to-synq cleanup sql-tests    Delete migrated SQL tests from SYNQ
    mc-to-synq cleanup monitors     Delete prefixed monitors from SYNQ
    mc-to-synq status               Show connection status for MC and SYNQ
"""

from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path
from typing import Optional

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import typer
from rich.console import Console
from rich.table import Table

from mc_to_synq import __version__
from mc_to_synq.config import AppConfig, config_exists, load_config

console = Console()

# ---------------------------------------------------------------------------
# App and subcommand groups
# ---------------------------------------------------------------------------

app = typer.Typer(
    name="mc-to-synq",
    help="Migrate monitors from Monte Carlo to SYNQ (Coalesce Quality).",
    no_args_is_help=True,
    rich_markup_mode="rich",
)

migrate_app = typer.Typer(
    help="Convert MC monitors to SYNQ format.",
    no_args_is_help=True,
)

deploy_app = typer.Typer(
    help="Push converted monitors to SYNQ.",
    no_args_is_help=True,
)

cleanup_app = typer.Typer(
    help="Remove migrated resources from SYNQ.",
    no_args_is_help=True,
)

app.add_typer(migrate_app, name="migrate")
app.add_typer(deploy_app, name="deploy")
app.add_typer(cleanup_app, name="cleanup")


# ---------------------------------------------------------------------------
# Shared options
# ---------------------------------------------------------------------------

ConfigOption = typer.Option(
    None,
    "--config",
    "-c",
    help="Path to config file (default: mc-to-synq.yaml in CWD).",
)


def _load(config_path: Optional[str]) -> AppConfig:
    """Load config with user-friendly error handling."""
    if config_path and not Path(config_path).exists():
        console.print(f"[red]Config file not found:[/red] {config_path}")
        raise typer.Exit(1)

    if not config_path and not config_exists():
        console.print(
            "[yellow]No config file found.[/yellow] "
            "Run [bold]mc-to-synq init[/bold] to create one."
        )
        raise typer.Exit(1)

    cfg = load_config(config_path)

    if cfg.synq.integration_id == "CHANGEME":
        console.print(
            "[yellow]Warning:[/yellow] synq.integration_id is still CHANGEME. "
            "Update your config before deploying."
        )

    return cfg


def _export_path(config: AppConfig, filename: str) -> str:
    """Build a path in the configured output directory."""
    out = Path(config.output.directory)
    out.mkdir(parents=True, exist_ok=True)
    return str(out / filename)


# ---------------------------------------------------------------------------
# init
# ---------------------------------------------------------------------------

@app.command()
def init(
    output: str = typer.Option(
        "mc-to-synq.yaml",
        "--output", "-o",
        help="Where to write the config file.",
    ),
) -> None:
    """Generate an annotated config file to get started."""
    if Path(output).exists():
        overwrite = typer.confirm(f"{output} already exists. Overwrite?")
        if not overwrite:
            raise typer.Exit(0)

    # Copy the bundled example config
    example = Path(__file__).parent.parent / "config" / "example.yaml"
    if example.exists():
        shutil.copy(example, output)
    else:
        # Fallback: generate minimal config inline
        _write_minimal_config(output)

    console.print(f"\n[green]Config written to:[/green] {output}")
    console.print(
        "\nNext steps:\n"
        "  1. Edit the config with your MC and SYNQ credentials\n"
        "  2. Set entity_prefixes to target your schema\n"
        "  3. Run: mc-to-synq status"
    )


def _write_minimal_config(path: str) -> None:
    import yaml

    minimal = {
        "monte_carlo": {"api_url": "https://api.getmontecarlo.com/graphql"},
        "synq": {
            "base_url": "https://api.synq.io",
            "oauth_url": "https://api.synq.io/oauth2/token",
            "integration_id": "CHANGEME",
        },
        "snowflake": {"account": "", "database": "", "schema": ""},
        "filters": {"entity_prefixes": [], "text_patterns": []},
        "network": {"verify_ssl": True, "timeout": 30},
    }
    with open(path, "w") as f:
        yaml.dump(minimal, f, default_flow_style=False, sort_keys=False)


# ---------------------------------------------------------------------------
# version
# ---------------------------------------------------------------------------

def _version_callback(value: bool) -> None:
    if value:
        console.print(f"mc-to-synq {__version__}")
        raise typer.Exit()


@app.callback()
def main(
    version: bool = typer.Option(
        False,
        "--version",
        "-v",
        help="Show version and exit.",
        callback=_version_callback,
        is_eager=True,
    ),
) -> None:
    """Migrate monitors from Monte Carlo to SYNQ."""
    pass


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------

@app.command()
def status(
    config: Optional[str] = ConfigOption,
) -> None:
    """Test connections to Monte Carlo and SYNQ."""
    cfg = _load(config)

    console.print("\n[bold]Monte Carlo[/bold]")
    try:
        from mc_to_synq.auth.monte_carlo import MonteCarloClient

        mc = MonteCarloClient(cfg)
        user = mc.test_connection()
        console.print(f"  [green]Connected[/green] as {user}")
    except Exception as e:
        console.print(f"  [red]Failed:[/red] {e}")

    console.print("\n[bold]SYNQ[/bold]")
    try:
        from mc_to_synq.auth.synq import SynqClient

        synq = SynqClient(cfg)
        if synq.test_connection():
            console.print(f"  [green]Connected[/green] to {cfg.synq.base_url}")
        else:
            console.print("  [red]Auth succeeded but API check failed[/red]")
    except Exception as e:
        console.print(f"  [red]Failed:[/red] {e}")

    console.print()


# ---------------------------------------------------------------------------
# extract
# ---------------------------------------------------------------------------

@app.command()
def extract(
    config: Optional[str] = ConfigOption,
    output: str = typer.Option(
        None, "--output", "-o", help="Output JSON file (default: mc_export.json)."
    ),
) -> None:
    """Pull all monitors from Monte Carlo and save to JSON."""
    cfg = _load(config)

    from mc_to_synq.auth.monte_carlo import MonteCarloClient
    from mc_to_synq.extract.filters import filter_monitors
    from mc_to_synq.extract.monitors import extract_monitors
    from mc_to_synq.reporting import generate_extraction_report

    console.print("\n[bold]Connecting to Monte Carlo...[/bold]")
    mc = MonteCarloClient(cfg)
    user = mc.test_connection()
    console.print(f"  Authenticated as {user}\n")

    monitors, connections = extract_monitors(mc)

    # Filter
    matched, unmatched = filter_monitors(monitors, cfg)
    console.print(f"\n  Matched by filter:  [green]{len(matched)}[/green]")
    console.print(f"  Excluded:           {len(unmatched)}")

    # Save full export
    export_file = output or _export_path(cfg, "mc_export.json")
    with open(export_file, "w") as f:
        json.dump(
            {
                "monitors": monitors,
                "matched": matched,
                "connections": connections,
            },
            f, indent=2, default=str,
        )
    console.print(f"\n  Full export: [cyan]{export_file}[/cyan]")

    # Report
    generate_extraction_report(
        monitors, matched, unmatched, cfg.output.directory
    )
    console.print()


# ---------------------------------------------------------------------------
# migrate sql-tests
# ---------------------------------------------------------------------------

@migrate_app.command("sql-tests")
def migrate_sql_tests(
    config: Optional[str] = ConfigOption,
    export_file: str = typer.Option(
        None,
        "--export",
        "-e",
        help="MC export JSON (default: mc_export.json in output dir).",
    ),
) -> None:
    """Convert MC SQL monitors to SYNQ SQL Test payloads."""
    cfg = _load(config)

    from mc_to_synq.migrate.sql_tests import convert_monitors_to_sql_tests
    from mc_to_synq.reporting import generate_sql_test_report

    # Load export
    export = export_file or _export_path(cfg, "mc_export.json")
    if not Path(export).exists():
        console.print(
            f"[red]Export not found:[/red] {export}\n"
            "Run [bold]mc-to-synq extract[/bold] first."
        )
        raise typer.Exit(1)

    with open(export) as f:
        data = json.load(f)

    monitors = data.get("matched", data.get("monitors", []))
    console.print(f"\n[bold]Converting {len(monitors)} monitors to SQL Tests...[/bold]")

    sql_tests, metadata = convert_monitors_to_sql_tests(monitors, cfg)

    skipped = sum(1 for m in metadata if m.get("skipped"))
    review = sum(1 for m in metadata if m.get("manual_review") and not m.get("skipped"))

    console.print(f"\n  Converted:      [green]{len(sql_tests)}[/green]")
    console.print(f"  Skipped:        {skipped}")
    console.print(f"  Manual review:  {review}")

    # Save payload
    payload_file = _export_path(cfg, "sql_tests_payload.json")
    with open(payload_file, "w") as f:
        json.dump({"sql_tests": sql_tests}, f, indent=2)
    console.print(f"\n  Payload: [cyan]{payload_file}[/cyan]")

    # Report
    generate_sql_test_report(sql_tests, metadata, None, cfg.output.directory)
    console.print()


# ---------------------------------------------------------------------------
# migrate ootb
# ---------------------------------------------------------------------------

@migrate_app.command("ootb")
def migrate_ootb(
    config: Optional[str] = ConfigOption,
    limit: Optional[int] = typer.Option(
        None, "--limit", "-n", help="Limit to N entities."
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Preview without saving."
    ),
) -> None:
    """Generate OOTB freshness/volume/duplicate monitor definitions."""
    cfg = _load(config)

    from mc_to_synq.auth.synq import SynqClient
    from mc_to_synq.deploy.custom_monitors import get_existing_monitor_names
    from mc_to_synq.migrate.ootb_monitors import discover_entities, resolve_and_build
    from mc_to_synq.reporting import generate_ootb_report

    console.print("\n[bold]Connecting to SYNQ...[/bold]")
    synq = SynqClient(cfg)
    synq.authenticate()

    console.print("\n[bold]Discovering entities...[/bold]")
    entities = discover_entities(synq, cfg)

    tables = [e for e in entities if "TABLE" in e.get("entity_type", "") and "VIEW" not in e.get("entity_type", "")]
    views = [e for e in entities if "VIEW" in e.get("entity_type", "")]
    console.print(f"  Found {len(entities)} entities ({len(tables)} tables, {len(views)} views)")

    if limit and limit < len(entities):
        console.print(f"  Limiting to first {limit}")
        entities = entities[:limit]

    console.print("\n[bold]Checking existing monitors...[/bold]")
    existing = get_existing_monitor_names(synq, cfg.ootb.monitor_prefix)
    console.print(f"  {len(existing)} existing monitors with prefix '{cfg.ootb.monitor_prefix}'")

    console.print("\n[bold]Resolving columns and building definitions...[/bold]")
    definitions, skipped, skipped_existing = resolve_and_build(
        synq, cfg, entities, existing
    )

    freshness = sum(1 for d in definitions if "freshness" in d)
    volume = sum(1 for d in definitions if "volume" in d)
    dups = sum(1 for d in definitions if "custom_numeric" in d)

    console.print(f"\n  New monitors:      [green]{len(definitions)}[/green]")
    console.print(f"    Freshness:       {freshness}")
    console.print(f"    Volume:          {volume}")
    console.print(f"    Duplicates:      {dups}")
    console.print(f"  Already exist:     {len(skipped_existing)}")
    console.print(f"  Entities skipped:  {len(skipped)}")

    if not dry_run:
        payload_file = _export_path(cfg, "ootb_monitors_payload.json")
        with open(payload_file, "w") as f:
            json.dump({"monitors": definitions}, f, indent=2)
        console.print(f"\n  Payload: [cyan]{payload_file}[/cyan]")

    generate_ootb_report(
        len(entities) + len(skipped),
        len(entities) - len(skipped),
        definitions,
        skipped,
        skipped_existing,
        None,
        cfg.output.directory,
    )
    console.print()


# ---------------------------------------------------------------------------
# migrate yaml
# ---------------------------------------------------------------------------

@migrate_app.command("yaml")
def migrate_yaml(
    config: Optional[str] = ConfigOption,
    export_file: str = typer.Option(
        None, "--export", "-e", help="MC export JSON."
    ),
    output: str = typer.Option(
        None, "--output", "-o", help="Output YAML file."
    ),
) -> None:
    """Export monitors-as-code YAML for synq-monitors CLI."""
    cfg = _load(config)

    from mc_to_synq.migrate.yaml_export import (
        classify_monitor,
        generate_yaml_export,
        write_yaml,
    )

    export = export_file or _export_path(cfg, "mc_export.json")
    if not Path(export).exists():
        console.print(
            f"[red]Export not found:[/red] {export}\n"
            "Run [bold]mc-to-synq extract[/bold] first."
        )
        raise typer.Exit(1)

    with open(export) as f:
        data = json.load(f)

    monitors = data.get("matched", data.get("monitors", []))
    console.print(f"\n[bold]Classifying {len(monitors)} monitors...[/bold]")

    classified = []
    for m in monitors:
        c = classify_monitor(m)
        classified.append((m, c))
        synq_type = c["synq_type"] or "SKIP"
        status = "MANUAL" if c["needs_manual_review"] else "AUTO"
        if c["synq_type"] is None:
            status = "SKIP"
        name = (m.get("name") or m.get("description") or "unnamed")[:50]
        console.print(f"  [{c['mc_type']:>12}] -> [{synq_type:>15}] {status:6s} {name}")

    console.print(f"\n[bold]Generating YAML...[/bold]")
    yaml_data = generate_yaml_export(classified, cfg)

    yaml_file = output or _export_path(cfg, "synq_monitors.yml")
    write_yaml(yaml_data, yaml_file)
    console.print(f"  Written to: [cyan]{yaml_file}[/cyan]\n")


# ---------------------------------------------------------------------------
# deploy sql-tests
# ---------------------------------------------------------------------------

@deploy_app.command("sql-tests")
def deploy_sql_tests_cmd(
    config: Optional[str] = ConfigOption,
    payload: str = typer.Option(
        None, "--payload", "-p", help="SQL tests JSON payload."
    ),
    limit: Optional[int] = typer.Option(
        None, "--limit", "-n", help="Deploy only first N tests."
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Preview without deploying."
    ),
) -> None:
    """Push SQL Tests to SYNQ."""
    cfg = _load(config)

    from mc_to_synq.auth.synq import SynqClient
    from mc_to_synq.deploy.sql_tests import deploy_sql_tests

    payload_file = payload or _export_path(cfg, "sql_tests_payload.json")
    if not Path(payload_file).exists():
        console.print(
            f"[red]Payload not found:[/red] {payload_file}\n"
            "Run [bold]mc-to-synq migrate sql-tests[/bold] first."
        )
        raise typer.Exit(1)

    with open(payload_file) as f:
        data = json.load(f)

    tests = data.get("sql_tests", [])
    if limit:
        tests = tests[:limit]

    console.print(f"\n[bold]Deploying {len(tests)} SQL Tests to SYNQ...[/bold]")

    if dry_run:
        console.print(f"\n  [yellow]DRY RUN[/yellow] -- would deploy {len(tests)} tests.")
        console.print(f"  Review payload at: {payload_file}\n")
        return

    synq = SynqClient(cfg)
    synq.authenticate()

    result = deploy_sql_tests(synq, tests, cfg)

    console.print(f"\n  Created: [green]{len(result['created'])}[/green]")
    console.print(f"  Updated: {len(result['updated'])}")
    if result["errors"]:
        console.print(f"  Errors:  [red]{len(result['errors'])}[/red]")

    # Save report
    from mc_to_synq.reporting import generate_sql_test_report
    generate_sql_test_report(tests, [], result, cfg.output.directory)
    console.print()


# ---------------------------------------------------------------------------
# deploy ootb
# ---------------------------------------------------------------------------

@deploy_app.command("ootb")
def deploy_ootb_cmd(
    config: Optional[str] = ConfigOption,
    payload: str = typer.Option(
        None, "--payload", "-p", help="OOTB monitors JSON payload."
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Preview without deploying."
    ),
) -> None:
    """Push OOTB monitors to SYNQ."""
    cfg = _load(config)

    from mc_to_synq.auth.synq import SynqClient
    from mc_to_synq.deploy.custom_monitors import deploy_monitors

    payload_file = payload or _export_path(cfg, "ootb_monitors_payload.json")
    if not Path(payload_file).exists():
        console.print(
            f"[red]Payload not found:[/red] {payload_file}\n"
            "Run [bold]mc-to-synq migrate ootb[/bold] first."
        )
        raise typer.Exit(1)

    with open(payload_file) as f:
        data = json.load(f)

    definitions = data.get("monitors", [])

    console.print(
        f"\n[bold]Deploying {len(definitions)} OOTB monitors to SYNQ...[/bold]"
    )

    if dry_run:
        console.print(
            f"\n  [yellow]DRY RUN[/yellow] -- would deploy {len(definitions)} monitors."
        )
        console.print(f"  Review payload at: {payload_file}\n")
        return

    synq = SynqClient(cfg)
    synq.authenticate()

    result = deploy_monitors(synq, definitions, cfg)

    console.print(f"\n  Created: [green]{len(result['created_ids'])}[/green]")
    if result["errors"]:
        console.print(f"  Errors:  [red]{len(result['errors'])}[/red]")

    console.print()


# ---------------------------------------------------------------------------
# cleanup sql-tests
# ---------------------------------------------------------------------------

@cleanup_app.command("sql-tests")
def cleanup_sql_tests_cmd(
    config: Optional[str] = ConfigOption,
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Preview without deleting."
    ),
) -> None:
    """Delete migrated SQL tests from SYNQ (by ID prefix)."""
    cfg = _load(config)

    from mc_to_synq.auth.synq import SynqClient
    from mc_to_synq.deploy.cleanup import delete_sql_tests, find_migrated_sql_tests

    synq = SynqClient(cfg)
    synq.authenticate()

    prefix = cfg.sql_tests.id_prefix
    console.print(f"\n[bold]Finding SQL tests with prefix '{prefix}'...[/bold]")

    tests = find_migrated_sql_tests(synq, prefix)

    if not tests:
        console.print("  No migrated tests found. Nothing to delete.\n")
        return

    console.print(f"  Found {len(tests)} migrated tests:")
    for t in tests:
        console.print(f"    {t.get('id', '?')}: {t.get('name', '?')}")

    if dry_run:
        console.print(f"\n  [yellow]DRY RUN[/yellow] -- would delete {len(tests)} tests.\n")
        return

    if not typer.confirm(f"\nDelete {len(tests)} tests?"):
        raise typer.Exit(0)

    ids = [t["id"] for t in tests]
    deleted, errors = delete_sql_tests(synq, ids)
    console.print(f"\n  Deleted: {deleted}")
    if errors:
        console.print(f"  Errors: {len(errors)}")
    console.print()


# ---------------------------------------------------------------------------
# cleanup monitors
# ---------------------------------------------------------------------------

@cleanup_app.command("monitors")
def cleanup_monitors_cmd(
    config: Optional[str] = ConfigOption,
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Preview without deleting."
    ),
) -> None:
    """Delete prefixed custom monitors from SYNQ."""
    cfg = _load(config)

    from mc_to_synq.auth.synq import SynqClient
    from mc_to_synq.deploy.cleanup import delete_monitors, find_prefixed_monitors

    synq = SynqClient(cfg)
    synq.authenticate()

    prefix = cfg.ootb.monitor_prefix
    console.print(f"\n[bold]Finding monitors with prefix '{prefix}'...[/bold]")

    monitors = find_prefixed_monitors(synq, prefix)

    if not monitors:
        console.print("  No prefixed monitors found. Nothing to delete.\n")
        return

    console.print(f"  Found {len(monitors)} monitors:")
    for m in monitors:
        console.print(f"    {m.get('name', '?')}  (id: {m.get('id', '?')[:12]}...)")

    if dry_run:
        console.print(
            f"\n  [yellow]DRY RUN[/yellow] -- would delete {len(monitors)} monitors.\n"
        )
        return

    if not typer.confirm(f"\nDelete {len(monitors)} monitors?"):
        raise typer.Exit(0)

    ids = [m["id"] for m in monitors if m.get("id")]
    deleted, errors = delete_monitors(synq, ids, cfg.output.batch_size)

    console.print(f"\n  Deleted: {len(deleted)}")
    if errors:
        console.print(f"  Errors: {len(errors)}")
    console.print()


# ---------------------------------------------------------------------------
# migrate-all (convenience orchestrator)
# ---------------------------------------------------------------------------

@app.command("migrate-all")
def migrate_all(
    config: Optional[str] = ConfigOption,
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Extract and convert but do not deploy."
    ),
    limit: Optional[int] = typer.Option(
        None, "--limit", "-n", help="Limit OOTB entity processing."
    ),
) -> None:
    """Run the full migration pipeline: extract, convert, and deploy."""
    cfg = _load(config)

    console.print("\n[bold]=== mc-to-synq: Full Migration ===[/bold]\n")

    # Step 1: Extract
    console.print("[bold]Step 1/4: Extract from Monte Carlo[/bold]")
    from mc_to_synq.auth.monte_carlo import MonteCarloClient
    from mc_to_synq.extract.filters import filter_monitors
    from mc_to_synq.extract.monitors import extract_monitors

    mc = MonteCarloClient(cfg)
    user = mc.test_connection()
    console.print(f"  Authenticated as {user}")

    monitors, connections = extract_monitors(mc)
    matched, unmatched = filter_monitors(monitors, cfg)
    console.print(f"  Matched: {len(matched)}, Excluded: {len(unmatched)}")

    export_file = _export_path(cfg, "mc_export.json")
    with open(export_file, "w") as f:
        json.dump(
            {"monitors": monitors, "matched": matched, "connections": connections},
            f, indent=2, default=str,
        )

    # Step 2: SQL Tests
    console.print(f"\n[bold]Step 2/4: Convert SQL monitors to SQL Tests[/bold]")
    from mc_to_synq.migrate.sql_tests import convert_monitors_to_sql_tests

    sql_tests, sql_meta = convert_monitors_to_sql_tests(matched, cfg)
    console.print(f"  Converted: {len(sql_tests)}")

    payload_file = _export_path(cfg, "sql_tests_payload.json")
    with open(payload_file, "w") as f:
        json.dump({"sql_tests": sql_tests}, f, indent=2)

    # Step 3: OOTB monitors
    console.print(f"\n[bold]Step 3/4: Generate OOTB monitors[/bold]")
    from mc_to_synq.auth.synq import SynqClient
    from mc_to_synq.deploy.custom_monitors import get_existing_monitor_names
    from mc_to_synq.migrate.ootb_monitors import discover_entities, resolve_and_build

    synq = SynqClient(cfg)
    synq.authenticate()

    entities = discover_entities(synq, cfg)
    if limit:
        entities = entities[:limit]

    existing = get_existing_monitor_names(synq, cfg.ootb.monitor_prefix)
    definitions, skipped_ents, skipped_existing = resolve_and_build(
        synq, cfg, entities, existing
    )
    console.print(f"  New OOTB monitors: {len(definitions)}")

    ootb_file = _export_path(cfg, "ootb_monitors_payload.json")
    with open(ootb_file, "w") as f:
        json.dump({"monitors": definitions}, f, indent=2)

    # Step 4: Deploy (unless dry run)
    if dry_run:
        console.print(f"\n[bold]Step 4/4: Deploy[/bold]")
        console.print(
            f"  [yellow]DRY RUN[/yellow] -- skipping deployment.\n"
            f"  SQL Tests payload:    {payload_file}\n"
            f"  OOTB monitors payload: {ootb_file}\n"
            f"\n  To deploy, re-run without --dry-run."
        )
        return

    console.print(f"\n[bold]Step 4/4: Deploy to SYNQ[/bold]")
    from mc_to_synq.deploy.custom_monitors import deploy_monitors
    from mc_to_synq.deploy.sql_tests import deploy_sql_tests

    if sql_tests:
        console.print(f"\n  Deploying {len(sql_tests)} SQL Tests...")
        st_result = deploy_sql_tests(synq, sql_tests, cfg)
        console.print(
            f"    Created: {len(st_result['created'])}, "
            f"Updated: {len(st_result['updated'])}, "
            f"Errors: {len(st_result['errors'])}"
        )

    if definitions:
        console.print(f"\n  Deploying {len(definitions)} OOTB monitors...")
        ootb_result = deploy_monitors(synq, definitions, cfg)
        console.print(
            f"    Created: {len(ootb_result['created_ids'])}, "
            f"Errors: {len(ootb_result['errors'])}"
        )

    console.print("\n[bold green]Migration complete.[/bold green]\n")


if __name__ == "__main__":
    app()