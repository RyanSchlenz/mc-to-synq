# mc-to-synq

Migrate your data observability monitors from [Monte Carlo](https://getmontecarlo.com) to [SYNQ](https://synq.io) (Coalesce Quality) with a single CLI.

Built during a proof of concept that migrated a real Monte Carlo account's monitors to SYNQ on a Snowflake data platform. Handles the full lifecycle: extract monitors from MC's GraphQL API, convert them to SYNQ-compatible formats, deploy via SYNQ's REST API, and clean up when you need to start over.

## What it does

mc-to-synq solves three problems that make MC-to-SYNQ migrations painful:

**1. Monitor extraction is messy.** Monte Carlo stores monitors across three different GraphQL endpoints (`getMonitors`, `getCustomRules`, `getAllUserDefinedMonitorsV2`) with inconsistent schemas that change between versions. mc-to-synq introspects the MC schema at runtime to discover current field names, then merges all sources into a single unified export.

**2. SQL conventions differ.** MC custom SQL monitors return aggregate values and compare against thresholds. SYNQ SQL Tests expect the opposite: a query that returns 0 rows on success and rows on failure (the dbt convention). mc-to-synq analyzes each SQL statement, determines whether wrapping is needed, and handles the conversion automatically. CTE-based queries, HAVING clauses, NOT EXISTS patterns, and COUNT-based checks are all handled with appropriate flags for manual review when the analysis is ambiguous.

**3. SYNQ does not auto-monitor views.** If your data warehouse is view-heavy (which most analytics layers are), SYNQ's deployment rules silently skip them because they rely on `INFORMATION_SCHEMA`, which excludes views. mc-to-synq generates OOTB freshness, volume, and duplicate-detection monitors for every entity -- views included -- using SYNQ's `BatchCreateMonitor` REST API. Timestamp columns and business keys are resolved automatically via SYNQ's schema and constraint APIs.

## Installation

```
pip install mc-to-synq
```

Or install from source:

```
git clone https://github.com/RyanSchlenz/mc-to-synq.git
cd mc-to-synq
pip install -e .
```

Requires Python 3.10+.

## Quick start

### 1. Generate a config file

```
mc-to-synq init
```

This creates `mc-to-synq.yaml` in your current directory with annotated defaults. Open it and fill in:

- Your Snowflake coordinates (account, database, schema)
- Your SYNQ integration UUID (from Settings > Integrations)
- Entity filter prefixes to target the schemas you want to migrate

### 2. Set credentials

Monte Carlo credentials come from `~/.mcd/profiles.ini` (the standard MC CLI config) or environment variables:

```
export MC_API_ID="your_mc_id"
export MC_API_TOKEN="your_mc_token"
```

SYNQ credentials are always environment variables:

```
export SYNQ_CLIENT_ID="your_client_id"
export SYNQ_CLIENT_SECRET="your_client_secret"
```

Create SYNQ API credentials at: Settings > API > Add client. Required scopes: Edit SQL Tests, Edit Automatic Monitors, Edit Custom Monitors.

### 3. Test connections

```
mc-to-synq status
```

### 4. Run the full migration

```
mc-to-synq migrate-all --dry-run
```

This extracts from MC, converts everything, generates payloads, and stops before deploying. Review the output files, then run without `--dry-run` to deploy.

## Commands

### Pipeline commands

| Command | Description |
|---|---|
| `mc-to-synq migrate-all` | Full pipeline: extract, convert, deploy |
| `mc-to-synq migrate-all --dry-run` | Full pipeline without deployment |

### Step-by-step commands

For more control, run each step independently:

| Command | Description |
|---|---|
| `mc-to-synq extract` | Pull all monitors from MC, filter, save to JSON |
| `mc-to-synq migrate sql-tests` | Convert MC SQL monitors to SYNQ SQL Test payloads |
| `mc-to-synq migrate ootb` | Generate OOTB freshness/volume/duplicate monitors |
| `mc-to-synq migrate yaml` | Export monitors-as-code YAML for `synq-monitors` CLI |
| `mc-to-synq deploy sql-tests` | Push SQL Tests to SYNQ |
| `mc-to-synq deploy ootb` | Push OOTB monitors to SYNQ |

### Cleanup commands

| Command | Description |
|---|---|
| `mc-to-synq cleanup sql-tests` | Delete migrated SQL tests (by ID prefix) |
| `mc-to-synq cleanup monitors` | Delete OOTB monitors (by name prefix) |

All cleanup commands support `--dry-run` and require confirmation before deleting.

### Utility commands

| Command | Description |
|---|---|
| `mc-to-synq init` | Generate an annotated config file |
| `mc-to-synq status` | Test MC and SYNQ connections |

## Configuration

All settings live in a single YAML file (`mc-to-synq.yaml`). Key sections:

### Filters

Controls which MC monitors are selected for migration. The entity prefix filter is the primary mechanism -- a monitor is included if any of its registered entities match a prefix. Text patterns are a fallback for monitors with missing entity metadata.

```yaml
filters:
  entity_prefixes:
    - "mydb:analytics."
    - "mydb:marts."
  text_patterns:
    - "analytics"
    - "marts"
```

### OOTB monitor generation

Timestamp column resolution uses a priority list -- the first match per entity wins. Business keys for duplicate detection are resolved from PK and unique constraints.

```yaml
ootb:
  timestamp_columns:
    - "DSS_LOAD_DATE"
    - "LOAD_DATE"
    - "MODIFIED_AT"
    - "UPDATED_AT"
    - "CREATED_AT"
  monitor_prefix: "mc2synq_"
```

The `monitor_prefix` is used for both naming and cleanup. All monitors created by mc-to-synq are named `mc2synq_freshness_*`, `mc2synq_volume_*`, `mc2synq_duplicates_*`, making bulk teardown deterministic.

Version 0.1.0 used `bv_` as the default prefix. If you deployed monitors with 0.1.0, set `monitor_prefix: "bv_"` in your config so `cleanup monitors` and the existing-monitor check still find them.

### Network

If your environment uses an SSL-intercepting proxy, disable certificate verification:

```yaml
network:
  verify_ssl: false
```

If `api.synq.io` does not resolve on your DNS, use the alternative endpoint:

```yaml
synq:
  base_url: "https://developer.synq.io"
  oauth_url: "https://developer.synq.io/oauth2/token"
```

See `mc_to_synq/example.yaml` in the source repository for the full annotated reference, or run `mc-to-synq init` to copy it into your project.

## How it works

### SQL test conversion

MC SQL monitors are converted to SYNQ SQL Tests using these rules:

| MC pattern | SYNQ handling |
|---|---|
| `SELECT * ... WHERE <violation>` | Passed through as-is (0 rows = pass) |
| `SELECT ... HAVING COUNT(*) > 1` | Passed through (HAVING already filters to violations) |
| `SELECT ... WHERE NOT IN/EXISTS` | Passed through (already violation-oriented) |
| `SELECT COUNT(*) FROM ...` | Wrapped in outer query that returns rows only when count != 0, including counts over HAVING or NOT EXISTS subqueries |
| `source_count` / `target_count` / `difference` columns | Wrapped to return rows only on mismatch |
| Other aggregate-only SELECT (for example `SELECT MAX(x) FROM ...`) | Flagged for manual review |
| CTE with WHERE/HAVING on final SELECT | Passed through |
| CTE whose final SELECT returns a COUNT, or has no WHERE | Flagged for manual review |
| Jinja template variables (`{{ }}`) | Skipped (requires manual rewrite) |
| Disabled in MC | Skipped |

The outer SELECT is checked first: a query that returns a single COUNT always returns one row, so it is wrapped even if it contains HAVING or NOT EXISTS. MC alert thresholds are not carried over: a wrapped COUNT fails whenever the count is not 0. Flagged tests are still included in the payload, so review them before deploying.

### Monitor type mapping

| MC type | SYNQ target | Notes |
|---|---|---|
| CUSTOM_SQL, CUSTOM_RULE | custom_numeric | Primary migration path |
| VALIDATION | custom_numeric | No native equivalent |
| METRIC | custom_numeric | Verify metric_aggregation |
| COMPARISON | custom_numeric | Manual review required |
| STATS | field_stats | Verify field list |
| FRESHNESS | freshness | Direct mapping |
| VOLUME | volume | Direct mapping |
| SCHEMA | (excluded) | Not migrated |

### Idempotent operations

SQL tests use stable IDs derived from the MC rule name (prefixed with `mc_migrated_`). Re-running the deployment upserts rather than duplicates. MC does not enforce unique names, so when two monitors would get the same ID, both get their MC uuid appended. Monitors without a name use their uuid. OOTB monitors check for existing monitors by name before creating, so repeated runs skip already-deployed monitors.

## Known limitations

- SYNQ wraps user SQL in an outer `SELECT`, so CTEs and complex queries must use inline subqueries with HAVING-based violation filtering. This is a SYNQ platform constraint, not a tool limitation.
- gRPC-based SYNQ services fail behind corporate proxies that strip HTTP/2 ALPN negotiation. mc-to-synq uses REST-only API paths exclusively.
- MC's GraphQL schema evolves without versioning. mc-to-synq introspects at runtime and falls back gracefully, but new schema changes may require updates.
- Monitors-as-code YAML export generates placeholder `monitored_id` values that must be updated from the SYNQ UI before deploying via `synq-monitors apply`.

## Project structure

```text
mc-to-synq/
  pyproject.toml
  mc_to_synq/
    cli.py                    # Typer CLI entry point
    config.py                 # YAML config loader (Pydantic-validated)
    example.yaml              # Annotated config reference (bundled with the package)
    auth/
      monte_carlo.py          # MC GraphQL client
      synq.py                 # SYNQ OAuth2 client
    extract/
      monitors.py             # MC monitor extraction + schema introspection
      filters.py              # Config-driven entity filtering
    migrate/
      sql_tests.py            # MC SQL -> SYNQ SQL Tests
      ootb_monitors.py        # Freshness/volume/duplicate generation
      yaml_export.py          # Monitors-as-code YAML output
    deploy/
      sql_tests.py            # BatchUpsertSqlTests
      custom_monitors.py      # BatchCreateMonitor
      cleanup.py              # Prefix-based bulk delete
    reporting/
      __init__.py             # JSON report generation
  tests/
    test_config.py
    test_filters.py
    test_sql_tests.py         # includes SQLite execution of generated SQL
    test_classification.py
    test_extract.py           # introspection, fallbacks, three-source merge
    test_yaml_export.py
    test_cli.py               # end-to-end CLI against fake MC/SYNQ APIs
    fake_apis.py
```

## Development

```
git clone https://github.com/RyanSchlenz/mc-to-synq.git
cd mc-to-synq
pip install -e ".[dev]"
pytest
```

## License

MIT
