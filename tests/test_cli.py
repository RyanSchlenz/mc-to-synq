"""End-to-end CLI tests against fake Monte Carlo and SYNQ APIs."""

import json

import pytest
import yaml
from typer.testing import CliRunner

from mc_to_synq.cli import app
from tests.fake_apis import MC_ID, MC_TOKEN, SYNQ_ID, SYNQ_SECRET, FakeApis

runner = CliRunner()


@pytest.fixture
def fake():
    apis = FakeApis().start()
    yield apis
    apis.stop()


@pytest.fixture
def workdir(tmp_path, monkeypatch, fake):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.setenv("MC_API_ID", MC_ID)
    monkeypatch.setenv("MC_API_TOKEN", MC_TOKEN)
    monkeypatch.setenv("SYNQ_CLIENT_ID", SYNQ_ID)
    monkeypatch.setenv("SYNQ_CLIENT_SECRET", SYNQ_SECRET)
    monkeypatch.setenv("COLUMNS", "200")

    assert runner.invoke(app, ["init"]).exit_code == 0
    cfg = yaml.safe_load((tmp_path / "mc-to-synq.yaml").read_text())
    cfg["monte_carlo"]["api_url"] = f"{fake.url}/graphql"
    cfg["synq"].update(base_url=fake.url, oauth_url=f"{fake.url}/oauth2/token",
                       integration_id="11111111-2222-3333-4444-555555555555")
    cfg["snowflake"].update(account="acct", database="PIQDW", schema="BIZVIEWS")
    cfg["filters"] = {"entity_prefixes": ["piqdw:bizviews."], "text_patterns": ["bizviews"]}
    cfg["output"]["directory"] = "out"
    (tmp_path / "mc-to-synq.yaml").write_text(yaml.safe_dump(cfg))
    return tmp_path


def run(*args, input=None):
    result = runner.invoke(app, list(args), input=input)
    return result


def test_version():
    result = run("--version")
    assert result.exit_code == 0
    assert "mc-to-synq" in result.output


def test_init_writes_bundled_example(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    assert run("init").exit_code == 0
    cfg = yaml.safe_load((tmp_path / "mc-to-synq.yaml").read_text())
    assert cfg["synq"]["integration_id"] == "CHANGEME"
    # Declining overwrite keeps the file
    (tmp_path / "mc-to-synq.yaml").write_text("keep: me\n")
    assert run("init", input="n\n").exit_code == 0
    assert (tmp_path / "mc-to-synq.yaml").read_text() == "keep: me\n"


def test_commands_without_config_point_to_init(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    result = run("extract")
    assert result.exit_code == 1
    assert "mc-to-synq init" in result.output


def test_status(workdir):
    result = run("status")
    assert result.exit_code == 0
    assert "Connected as Test User (tester@example.com)" in result.output
    assert "Failed" not in result.output


def test_full_step_by_step_flow(workdir, fake):
    out = workdir / "out"

    # extract
    r = run("extract")
    assert r.exit_code == 0, r.output
    assert "SQL field on CustomRule: queryText" in r.output
    export = json.loads((out / "mc_export.json").read_text())
    matched = {m["uuid"] for m in export["matched"]}
    assert "m-other" not in matched
    assert {"m-where", "m-count", "cr-noname-1", "ud-fresh"} <= matched

    # migrate sql-tests
    r = run("migrate", "sql-tests")
    assert r.exit_code == 0, r.output
    tests = json.loads((out / "sql_tests_payload.json").read_text())["sql_tests"]
    ids = [t["id"] for t in tests]
    assert len(ids) == len(set(ids)) == 6  # where, count, 2 unnamed, 2 duplicate names
    count_sql = next(t for t in tests if t["id"] == "mc_migrated_negative_totals")
    assert "COALESCE(_mc_check._mc_count, 0) != 0" in count_sql["sql_expression"]
    assert "difference" not in count_sql["sql_expression"]
    conv_report = json.loads((out / "sql_test_migration_report.json").read_text())
    assert conv_report["conversion"]["skipped"] == 2  # disabled + jinja

    # migrate ootb: views are included
    r = run("migrate", "ootb")
    assert r.exit_code == 0, r.output
    defs = json.loads((out / "ootb_monitors_payload.json").read_text())["monitors"]
    names = {d["name"] for d in defs}
    assert "bv_freshness_v_summary" in names
    assert "bv_duplicates_v_customer" in names
    assert "bv_duplicates_orders" in names
    assert not any("v_no_ts" in n or "raw" in n for n in names)
    ootb_report = json.loads((out / "ootb_monitor_report.json").read_text())
    assert ootb_report["entities"] == {"discovered": 4, "eligible": 3, "skipped": 1}

    # migrate yaml (previously crashed on null scheduleConfig)
    r = run("migrate", "yaml")
    assert r.exit_code == 0, r.output
    assert (out / "synq_monitors.yml").exists()

    # deploy
    assert run("deploy", "sql-tests", "--dry-run").exit_code == 0
    assert fake.sql_tests == {}
    r = run("deploy", "sql-tests")
    assert r.exit_code == 0, r.output
    assert len(fake.sql_tests) == 6
    # conversion report is not clobbered by the deploy report
    assert json.loads((out / "sql_test_migration_report.json").read_text()) == conv_report
    deploy_report = json.loads((out / "sql_test_deploy_report.json").read_text())
    assert deploy_report["deployment"]["created"] == 6

    r = run("deploy", "ootb")
    assert r.exit_code == 0, r.output
    assert len(fake.monitors) == len(defs)

    # idempotency: re-running sql deploy updates, ootb generation skips existing
    r = run("deploy", "sql-tests")
    assert "Updated: 6" in r.output
    r = run("migrate", "ootb")
    assert "Already exist:     8" in r.output

    # cleanup
    r = run("cleanup", "sql-tests", "--dry-run")
    assert r.exit_code == 0 and len(fake.sql_tests) == 6
    r = run("cleanup", "sql-tests", input="n\n")
    assert len(fake.sql_tests) == 6
    r = run("cleanup", "sql-tests", input="y\n")
    assert r.exit_code == 0 and fake.sql_tests == {}
    r = run("cleanup", "monitors", input="y\n")
    assert r.exit_code == 0 and fake.monitors == {}


def test_migrate_all(workdir, fake):
    r = run("migrate-all", "--dry-run")
    assert r.exit_code == 0, r.output
    assert fake.sql_tests == {} and fake.monitors == {}
    r = run("migrate-all")
    assert r.exit_code == 0, r.output
    assert "Migration complete." in r.output
    assert len(fake.sql_tests) == 6
    assert len(fake.monitors) == 8


def test_deploy_errors_exit_nonzero(workdir, fake):
    assert run("extract").exit_code == 0
    assert run("migrate", "sql-tests").exit_code == 0
    fake.fail_sql_upsert = True
    r = run("deploy", "sql-tests")
    assert r.exit_code == 1
    r = run("migrate-all")
    assert r.exit_code == 1
    assert "error" in r.output.lower()


def test_tracebacks_never_print_credentials(workdir, monkeypatch):
    """A network failure must not dump headers with tokens to the terminal."""
    cfg = yaml.safe_load((workdir / "mc-to-synq.yaml").read_text())
    cfg["monte_carlo"]["api_url"] = "http://127.0.0.1:1/graphql"  # nothing listens
    (workdir / "mc-to-synq.yaml").write_text(yaml.safe_dump(cfg))
    assert app.pretty_exceptions_show_locals is False
    r = runner.invoke(app, ["extract"])
    assert r.exit_code != 0
    assert MC_TOKEN not in r.output


def test_bad_synq_credentials_reported_by_status(workdir, monkeypatch):
    monkeypatch.setenv("SYNQ_CLIENT_SECRET", "wrong")
    r = run("status")
    assert "SYNQ auth failed (HTTP 401)" in r.output
