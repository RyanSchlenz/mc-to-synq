"""Configuration loader for mc-to-synq.

Reads a YAML config file and provides typed access to all settings.
Replaces the hardcoded constants scattered across the original scripts.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

import yaml
from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Config schema
# ---------------------------------------------------------------------------

class MonteCarloConfig(BaseModel):
    api_url: str = "https://api.getmontecarlo.com/graphql"
    credentials_profile: str = "default"


class SynqConfig(BaseModel):
    base_url: str = "https://api.synq.io"
    oauth_url: str = "https://api.synq.io/oauth2/token"
    integration_id: str = "CHANGEME"


class SnowflakeConfig(BaseModel):
    account: str = ""
    database: str = ""
    schema_name: str = Field("", alias="schema")

    model_config = {"populate_by_name": True}


class FiltersConfig(BaseModel):
    entity_prefixes: list[str] = []
    text_patterns: list[str] = []


class OotbConfig(BaseModel):
    timestamp_columns: list[str] = [
        "DSS_LOAD_DATE", "LOAD_DATE", "DW_LOAD_DATE",
        "MODIFIED_AT", "UPDATED_AT", "CREATED_AT",
    ]
    key_constraint_types: list[str] = [
        "TABLE_CONSTRAINT_TYPE_PRIMARY_KEY",
        "TABLE_CONSTRAINT_TYPE_UNIQUE",
    ]
    allowed_entity_types: list[str] = [
        "ENTITY_TYPE_SNOWFLAKE_TABLE",
        "ENTITY_TYPE_SNOWFLAKE_VIEW",
        "ENTITY_TYPE_SNOWFLAKE_DYNAMIC_TABLE",
    ]
    monitor_prefix: str = "bv_"
    severity: str = "SEVERITY_WARNING"
    sensitivity: str = "SENSITIVITY_BALANCED"
    schedule_minutes_utc: int = 360


class SqlTestsConfig(BaseModel):
    id_prefix: str = "mc_migrated_"
    default_schedule: str = (
        "DTSTART:20260310T060000Z\nRRULE:FREQ=DAILY;BYHOUR=6;BYMINUTE=0;BYSECOND=0"
    )
    severity: str = "SEVERITY_ERROR"
    skip_disabled: bool = True
    skip_jinja_templates: bool = True


class NetworkConfig(BaseModel):
    verify_ssl: bool = True
    timeout: int = 30


class OutputConfig(BaseModel):
    directory: str = "."
    batch_size: int = 25


class AppConfig(BaseModel):
    monte_carlo: MonteCarloConfig = MonteCarloConfig()
    synq: SynqConfig = SynqConfig()
    snowflake: SnowflakeConfig = SnowflakeConfig()
    filters: FiltersConfig = FiltersConfig()
    ootb: OotbConfig = OotbConfig()
    sql_tests: SqlTestsConfig = SqlTestsConfig()
    network: NetworkConfig = NetworkConfig()
    output: OutputConfig = OutputConfig()


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------

_CONFIG_FILENAMES = [
    "mc-to-synq.yaml",
    "mc-to-synq.yml",
    "config/mc-to-synq.yaml",
    "config/mc-to-synq.yml",
]


def find_config_file(explicit_path: Optional[str] = None) -> Optional[Path]:
    """Locate the config file, checking explicit path then defaults."""
    if explicit_path:
        p = Path(explicit_path)
        if p.exists():
            return p
        return None

    for name in _CONFIG_FILENAMES:
        p = Path(name)
        if p.exists():
            return p

    return None


def load_config(path: Optional[str] = None) -> AppConfig:
    """Load and validate the configuration.

    Searches for a config file in the following order:
    1. Explicit path (if provided)
    2. mc-to-synq.yaml in CWD
    3. mc-to-synq.yml in CWD
    4. config/mc-to-synq.yaml
    5. config/mc-to-synq.yml

    Returns default config if no file is found (allows CLI to
    guide the user through init).
    """
    config_path = find_config_file(path)

    if config_path is None:
        return AppConfig()

    with open(config_path) as f:
        raw = yaml.safe_load(f) or {}

    return AppConfig(**raw)


def config_exists(path: Optional[str] = None) -> bool:
    """Check if a config file exists."""
    return find_config_file(path) is not None
