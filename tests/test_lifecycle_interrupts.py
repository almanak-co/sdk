"""SIGINT stays a CLI interruption even when received inside native schema building."""

import signal
from enum import Enum
from types import SimpleNamespace

import pytest
from pydantic_core import SchemaValidator, core_schema

from almanak.interrupts import deferred_sigint
from almanak.runtime import Runtime


class InterruptLiteral(Enum):
    VALUE = "value"

    def __hash__(self):
        signal.raise_signal(signal.SIGINT)
        return hash(self.value)


def build_interrupted_schema():
    return SchemaValidator(core_schema.literal_schema([InterruptLiteral.VALUE]))


def test_signal_in_native_schema_build_finishes_before_checked_interrupt():
    previous = signal.getsignal(signal.SIGINT)
    with pytest.raises(KeyboardInterrupt), deferred_sigint() as check:
        validator = build_interrupted_schema()
        assert validator is not None
        check()
    assert signal.getsignal(signal.SIGINT) == previous


@pytest.mark.parametrize("last_tick", (False, True))
def test_runtime_defers_signal_until_tick_finishes_and_releases_lease(last_tick):
    events = []
    pending = {"identity": "retained", "outcome": "unknown"}

    def tick():
        events.append("tick")
        build_interrupted_schema()
        events.append("tick_finished")
        return last_tick

    runner = SimpleNamespace(
        tick=tick,
        journal=SimpleNamespace(load=lambda: pending),
        close=lambda: events.append("released"),
    )
    with pytest.raises(KeyboardInterrupt), deferred_sigint() as check:
        Runtime.run(runner, interval=0, interrupt=check)
    assert events == ["tick", "tick_finished", "released"]
    assert pending == {"identity": "retained", "outcome": "unknown"}


def test_signal_before_first_tick_admits_no_work():
    events = []
    runner = SimpleNamespace(
        tick=lambda: events.append("tick"), close=lambda: events.append("released")
    )
    with pytest.raises(KeyboardInterrupt), deferred_sigint() as check:
        signal.raise_signal(signal.SIGINT)
        signal.raise_signal(signal.SIGINT)
        Runtime.run(runner, interval=0, interrupt=check)
    assert events == ["released"]


def test_real_error_is_preserved_and_previous_handler_restored():
    previous = signal.getsignal(signal.SIGINT)
    with pytest.raises(ValueError, match="actual failure"), deferred_sigint():
        signal.raise_signal(signal.SIGINT)
        raise ValueError("actual failure")
    assert signal.getsignal(signal.SIGINT) == previous


def test_cli_reports_deferred_interruption_as_exit_130(monkeypatch, capsys):
    from almanak import cli

    def lifecycle(args):
        with deferred_sigint():
            build_interrupted_schema()

    monkeypatch.setattr(cli, "parse_arguments", lambda: SimpleNamespace())
    monkeypatch.setattr(cli, "dispatch", lifecycle)
    assert cli.main() == 130
    assert "checkpoint is preserved" in capsys.readouterr().err


def test_lifecycle_cli_protects_initialization_and_checks_before_admitting_tick(monkeypatch):
    from almanak import cli, runtime

    events = []

    class Runner:
        run = Runtime.run

        def __init__(self, *args, **kwargs):
            build_interrupted_schema()
            self.journal = SimpleNamespace(load=lambda: SimpleNamespace(deployment_id="retained"))

        def tick(self):
            events.append("tick")
            return True

        def close(self):
            events.append("released")

    monkeypatch.setattr(runtime, "Runtime", Runner)
    args = SimpleNamespace(product="resume", config=None, gateway_url=None, gateway_auth_file=None)
    with pytest.raises(KeyboardInterrupt):
        cli.run_lifecycle(args)
    assert events == ["released"]
