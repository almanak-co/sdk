"""`almanak wallet create` without a terminal: a named or a generated password file."""

import json
import os
import stat
from pathlib import Path

import pytest
from eth_account import Account as Signer

from almanak.cli_arguments import build_parser, parse_validated
from almanak.gateway import credentials
from almanak.gateway.credentials import create_wallet, keystore_password, private_text


@pytest.fixture(autouse=True)
def no_prompt(monkeypatch):
    def refuse(prompt):
        raise AssertionError("wallet creation asked for a password")

    monkeypatch.setattr(credentials.getpass, "getpass", refuse)


def unlocked(result, password):
    stores = json.loads(private_text(result_path(result, "keystores")))
    return Signer.from_key(Signer.decrypt(stores[result["account"]], password))


def result_path(result, name):
    return Path(result[name])


def test_generated_password_unlocks_the_keystore_and_stays_private(tmp_path):
    result = create_wallet(tmp_path / "operator", "treasury", generate_password=True)
    password_file = result_path(result, "password_file")
    assert unlocked(result, keystore_password(password_file)).address == result["address"]
    for name in ("keystores.json", "keystore-password", "gateway-auth.token", "wallet.json"):
        mode = os.stat(tmp_path / "operator" / name).st_mode
        assert stat.S_IMODE(mode) == 0o600
    # The printed result and its retained copy name files, never the password or a key.
    retained = json.loads((tmp_path / "operator/wallet.json").read_text())
    assert retained == result
    assert keystore_password(password_file) not in json.dumps(result)


def test_named_password_file_is_used_and_not_copied(tmp_path):
    secret = tmp_path / "secret"
    secret.write_text("correct horse\n")
    secret.chmod(0o600)
    result = create_wallet(tmp_path / "operator", "treasury", password_file=secret)
    assert "password_file" not in result
    assert not (tmp_path / "operator/keystore-password").exists()
    assert unlocked(result, "correct horse").address == result["address"]


def test_a_shared_or_empty_password_file_is_refused_before_anything_is_written(tmp_path):
    shared = tmp_path / "shared"
    shared.write_text("secret\n")
    shared.chmod(0o644)
    with pytest.raises(ValueError, match="private regular file"):
        create_wallet(tmp_path / "a", "treasury", password_file=shared)
    empty = tmp_path / "empty"
    empty.write_text("\n")
    empty.chmod(0o600)
    with pytest.raises(ValueError, match="must not be empty"):
        create_wallet(tmp_path / "b", "treasury", password_file=empty)
    assert not (tmp_path / "a").exists() and not (tmp_path / "b").exists()


def test_the_two_password_sources_are_exclusive(capsys):
    base = ["wallet", "create", "--account", "treasury", "--output", "out"]
    assert parse_validated(build_parser(), [*base, "--generate-password"]).generate_password
    with pytest.raises(SystemExit):
        parse_validated(build_parser(), [*base, "--generate-password", "--password-file", "secret"])
    capsys.readouterr()


def test_a_failed_write_leaves_no_partial_wallet_and_a_retry_succeeds(tmp_path, monkeypatch):
    real = credentials.write_private

    def failing(path, contents):
        if path.name == "keystore-password":
            raise OSError("no space left on device")
        real(path, contents)

    monkeypatch.setattr(credentials, "write_private", failing)
    with pytest.raises(OSError):
        create_wallet(tmp_path / "operator", "treasury", generate_password=True)
    assert not (tmp_path / "operator").exists()
    monkeypatch.setattr(credentials, "write_private", real)
    assert create_wallet(tmp_path / "operator", "treasury", generate_password=True)["address"]


def test_a_fifo_password_file_is_refused_without_waiting_for_a_writer(tmp_path):
    fifo = tmp_path / "fifo"
    os.mkfifo(fifo, 0o600)
    with pytest.raises(ValueError, match="private regular file"):
        create_wallet(tmp_path / "operator", "treasury", password_file=fifo)


def test_a_failed_cleanup_names_the_partial_directory(tmp_path, monkeypatch):
    def failing(path, contents):
        raise OSError("disk went read-only")

    def unremovable(path):
        raise OSError("read-only file system")

    monkeypatch.setattr(credentials, "write_private", failing)
    monkeypatch.setattr(credentials.shutil, "rmtree", unremovable)
    with pytest.raises(OSError) as raised:
        create_wallet(tmp_path / "operator", "treasury", generate_password=True)
    message = str(raised.value)
    assert str(tmp_path / "operator") in message and "could not be removed" in message
    assert "disk went read-only" in message and "read-only file system" in message
