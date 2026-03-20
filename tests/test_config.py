"""Tests for config loading and validation."""

import tempfile
from pathlib import Path

import yaml

from mc_to_synq.config import AppConfig, load_config


def test_default_config():
    """Loading without a file returns sensible defaults."""
    cfg = AppConfig()
    assert cfg.monte_carlo.api_url == "https://api.getmontecarlo.com/graphql"
    assert cfg.synq.integration_id == "CHANGEME"
    assert cfg.network.verify_ssl is True
    assert cfg.output.batch_size == 25


def test_load_from_yaml():
    """Config values are read from a YAML file."""
    content = {
        "synq": {
            "base_url": "https://developer.synq.io",
            "integration_id": "test-uuid-1234",
        },
        "snowflake": {
            "account": "acme.us-east-1.aws",
            "database": "WAREHOUSE",
            "schema": "ANALYTICS",
        },
        "network": {"verify_ssl": False},
    }

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".yaml", delete=False
    ) as f:
        yaml.dump(content, f)
        path = f.name

    cfg = load_config(path)
    assert cfg.synq.base_url == "https://developer.synq.io"
    assert cfg.synq.integration_id == "test-uuid-1234"
    assert cfg.snowflake.account == "acme.us-east-1.aws"
    assert cfg.snowflake.schema_name == "ANALYTICS"
    assert cfg.network.verify_ssl is False
    # Unset values get defaults
    assert cfg.monte_carlo.api_url == "https://api.getmontecarlo.com/graphql"

    Path(path).unlink()


def test_missing_file_returns_defaults():
    """Loading a non-existent path returns default config."""
    cfg = load_config("/nonexistent/path.yaml")
    assert cfg.synq.integration_id == "CHANGEME"


def test_filters_defaults_empty():
    """Default filters are empty (user must configure)."""
    cfg = AppConfig()
    assert cfg.filters.entity_prefixes == []
    assert cfg.filters.text_patterns == []


def test_ootb_defaults():
    """OOTB defaults include standard timestamp columns."""
    cfg = AppConfig()
    assert "DSS_LOAD_DATE" in cfg.ootb.timestamp_columns
    assert cfg.ootb.monitor_prefix == "bv_"
