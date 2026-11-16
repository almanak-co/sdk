import json
import os
import stat
import sys
from pathlib import Path

import pytest

from almanak import native
from almanak.cli_arguments import build_parser, product_names

# The release spellings each platform reports, paired with the bundled name.
PLATFORMS = [
    ("Darwin", "arm64", "almanak-code-darwin-arm64"),
    ("Darwin", "x86_64", "almanak-code-darwin-x86_64"),
    ("Linux", "aarch64", "almanak-code-linux-arm64"),
    ("Linux", "x86_64", "almanak-code-linux-x86_64"),
    ("Windows", "AMD64", "almanak-code-windows-x86_64.exe"),
]


class Exec(Exception):
    """Stands in for a process replacement so a test can inspect the argv."""


@pytest.fixture
def execs(monkeypatch):
    calls: list[list[str]] = []

    def record(path, argv, environ=None):
        calls.append(list(argv))
        raise Exec

    monkeypatch.setattr(os, "execve", record)
    monkeypatch.setattr(os, "execv", record)
    monkeypatch.delenv(native.ACTIVE, raising=False)
    return calls


@pytest.fixture
def installed(monkeypatch, tmp_path):
    binary = tmp_path / "almanak-code"
    binary.write_bytes(b"#!/bin/sh\n")
    binary.chmod(0o644)
    monkeypatch.setattr(native, "binary_path", lambda: binary)
    return binary


@pytest.mark.parametrize(("system", "machine", "expected"), PLATFORMS)
def test_binary_name_matches_the_release_asset(system, machine, expected):
    assert native.binary_name(system, machine) == expected


def test_unmapped_architecture_passes_through():
    assert native.binary_name("Linux", "riscv64") == "almanak-code-linux-riscv64"


def test_binary_path_sits_inside_the_installed_package():
    import almanak

    assert native.binary_path().parent == Path(almanak.__file__).resolve().parent / "bin"


def test_bare_invocation_forwards_no_arguments():
    assert native.forwarded_arguments([], frozenset({"run"})) == []


def test_unknown_command_forwards_every_argument():
    argv = ["code", "--model", "x", "-p", "hi"]
    assert native.forwarded_arguments(argv, frozenset({"run"})) == argv


@pytest.mark.parametrize("name", sorted(product_names(build_parser())))
def test_known_commands_never_fall_through(name):
    known = product_names(build_parser())
    assert native.forwarded_arguments([name, "anything"], known) is None


@pytest.mark.parametrize("argv", [["--help"], ["-h"], ["--badoption"], ["--"], ["-x", "code"]])
def test_a_leading_option_stays_with_argparse(argv):
    assert native.forwarded_arguments(argv, frozenset({"run"})) is None


def test_missing_binary_reports_json_and_exits_one(monkeypatch, tmp_path, capsys, execs):
    missing = tmp_path / "absent"
    monkeypatch.setattr(native, "binary_path", lambda: missing)
    with pytest.raises(SystemExit) as failure:
        native.exec_binary(["code"])
    assert failure.value.code == 1
    reported = json.loads(capsys.readouterr().err)
    assert reported["error"] == "almanak_code_unavailable"
    assert reported["path"] == str(missing)
    assert execs == []


def test_a_non_executable_binary_is_made_executable(installed, execs):
    with pytest.raises(Exec):
        native.exec_binary(["code"])
    assert stat.S_IMODE(installed.stat().st_mode) & 0o111
    assert execs == [[str(installed), "code"]]


def test_the_marker_hands_a_v2_command_back_to_argparse(monkeypatch, installed, execs):
    monkeypatch.setenv(native.ACTIVE, "1")
    native.forward_unrecognized(["strat", "new"], frozenset({"run"}))
    assert execs == []


def test_a_v2_command_inside_a_session_lists_the_real_commands(monkeypatch, capsys, execs):
    """The agent still emits `almanak strat new`; argparse names what exists."""
    from almanak import cli

    monkeypatch.setenv(native.ACTIVE, "1")
    monkeypatch.setattr(sys, "argv", ["almanak", "strat", "new"])
    with pytest.raises(SystemExit) as exit_code:
        cli.main()
    assert exit_code.value.code == 2
    reported = capsys.readouterr().err
    assert "invalid choice: 'strat'" in reported
    assert "check" in reported
    assert execs == []


def test_forwarding_marks_the_environment(monkeypatch, installed):
    captured: dict[str, str] = {}

    def record(path, argv, environ):
        captured.update(environ)
        raise Exec

    monkeypatch.setattr(os, "execve", record)
    monkeypatch.delenv(native.ACTIVE, raising=False)
    with pytest.raises(Exec):
        native.exec_binary(["code"])
    assert captured[native.ACTIVE] == "1"


def test_forwarding_selects_the_v3_lane_unless_the_host_chose(monkeypatch):
    local = native.forwarded_environment({"PATH": "/bin"})
    assert local[native.GENERATION] == "v3"
    assert local[native.V3_PYTHON] == sys.executable
    assert local["PATH"] == "/bin"
    hosted = native.forwarded_environment(
        {native.GENERATION: "v3", native.V3_PYTHON: "/opt/almanak-v3/bin/python"}
    )
    assert hosted[native.V3_PYTHON] == "/opt/almanak-v3/bin/python"
    assert native.forwarded_environment({native.ACTIVE: ""})[native.ACTIVE] == "1"


def test_main_forwards_an_unknown_command(monkeypatch, installed, execs):
    from almanak import cli

    monkeypatch.setattr(sys, "argv", ["almanak", "code", "-p", "hi"])
    with pytest.raises(Exec):
        cli.main()
    assert execs[0][1:] == ["code", "-p", "hi"]


def test_main_still_prints_help(monkeypatch, capsys, execs):
    from almanak import cli

    monkeypatch.setattr(sys, "argv", ["almanak", "--help"])
    with pytest.raises(SystemExit) as exit_code:
        cli.main()
    assert exit_code.value.code == 0
    assert capsys.readouterr().out.startswith("usage: almanak ")
    assert execs == []


@pytest.mark.parametrize("argv", [["almanak", "--badoption"], ["almanak", "run"]])
def test_main_still_rejects_bad_argparse_input(monkeypatch, argv, execs):
    from almanak import cli

    monkeypatch.setattr(sys, "argv", argv)
    with pytest.raises(SystemExit) as exit_code:
        cli.main()
    assert exit_code.value.code == 2
    assert execs == []


def test_product_names_reports_a_parser_without_products():
    import argparse

    with pytest.raises(RuntimeError):
        product_names(argparse.ArgumentParser())


def test_obsolete_fork_command_is_not_a_public_product():
    assert "fork" not in product_names(build_parser())
