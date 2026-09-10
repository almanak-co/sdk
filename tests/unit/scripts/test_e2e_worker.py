"""Boot identity controls for the separate teardown dispatch lane."""

import sqlite3

import pytest

from qa_lab.e2e_worker import boot_deployment_id, teardown_command


def test_teardown_resolves_boot_state_before_first_transaction(tmp_path):
    with sqlite3.connect(tmp_path / "almanak_state.db") as db:
        db.execute("CREATE TABLE strategy_state (deployment_id TEXT)")
        db.execute("INSERT INTO strategy_state VALUES ('deployment:123456789abc')")
    command = teardown_command(tmp_path)
    assert command[command.index("-s") + 1] == "deployment:123456789abc"
    assert command[:7] == ("uv", "run", "--no-sync", "almanak", "strat", "teardown", "request")
    assert "--wait" in command


def test_missing_boot_database_is_not_created(tmp_path):
    with pytest.raises(ValueError, match="unmeasured"):
        boot_deployment_id(tmp_path)
    assert not (tmp_path / "almanak_state.db").exists()


@pytest.mark.parametrize("other", [None, "lp_dual", "deployment:abcdef123456"])
def test_teardown_rejects_conflicting_or_invalid_boot_identity(tmp_path, other):
    with sqlite3.connect(tmp_path / "almanak_state.db") as db:
        db.execute("CREATE TABLE strategy_state (deployment_id TEXT)")
        db.execute("INSERT INTO strategy_state VALUES ('deployment:123456789abc')")
        db.execute("CREATE TABLE transaction_ledger (deployment_id TEXT)")
        db.execute("INSERT INTO transaction_ledger VALUES (?)", (other,))
    with pytest.raises(ValueError, match="ambiguous"):
        teardown_command(tmp_path)
