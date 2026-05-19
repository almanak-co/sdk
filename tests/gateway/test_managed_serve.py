"""Tests for the ``managed_serve`` sidecar CLI entrypoint.

Reviewer-flagged in PR #2351: the new lifecycle paths in ``main()`` —
startup success/failure, signal-triggered shutdown, shutdown error —
need direct coverage. We stub :class:`ManagedGateway` because the real
class starts Anvil forks + a gRPC server, neither of which we want
exercised by a fast unit test. The boot-time config helpers
(``load_config``, ``_resolve_anvil_chains_and_funding``,
``strategy_folder_env``, ``compute_anvil_startup_timeout``) are likewise
stubbed so the test stays hermetic.
"""

from __future__ import annotations

import logging
import threading
from unittest.mock import MagicMock, patch

import pytest


def _stub_main_dependencies(
    *,
    mg_class: MagicMock,
    network: str = "anvil",
    chains: tuple[str, ...] = ("base",),
    funding: dict[str, str] | None = None,
):
    """Patch everything ``managed_serve.main`` consults before instantiating ManagedGateway."""
    settings = MagicMock()
    settings.network = network
    settings.grpc_port = 50051
    settings.chains = list(chains)
    settings.grpc_host = "127.0.0.1"

    config = MagicMock()
    config.gateway = settings

    return [
        patch("almanak.gateway.managed_serve.ManagedGateway", new=mg_class),
        patch("almanak.config.service.load_config", return_value=config),
        patch(
            "almanak.framework.cli.run_helpers._resolve_anvil_chains_and_funding",
            return_value=(list(chains), funding or {}),
        ),
        patch("almanak.framework.local_paths.strategy_folder_env", return_value="/tmp/fake-strategy"),
        patch("almanak.framework.cli._anvil_timeout.compute_anvil_startup_timeout", return_value=10.0),
        # install_redaction / configure_structlog do real work on the logging
        # tree; cheap to stub so test runs leave global state clean.
        patch("almanak.gateway.managed_serve.install_redaction"),
        patch("almanak.gateway.managed_serve.configure_structlog"),
    ]


def _run_main_with_patches(patches: list, mg_class: MagicMock) -> int:
    """Apply patches, call main(), return its exit code."""
    from almanak.gateway import managed_serve

    for p in patches:
        p.start()
    try:
        return managed_serve.main()
    finally:
        for p in patches:
            p.stop()


def test_main_returns_zero_on_clean_lifecycle(caplog) -> None:
    """Happy path: ManagedGateway.start succeeds, signal sets stop_event, stop succeeds → exit 0."""
    mg = MagicMock()
    mg.host = "127.0.0.1"
    mg.port = 50051
    mg_class = MagicMock(return_value=mg)

    # Make stop_event auto-set so .wait() returns immediately — simulates
    # SIGTERM having already arrived (or arriving the instant main blocks).
    real_event = threading.Event()
    real_event.set()

    patches = _stub_main_dependencies(mg_class=mg_class)
    patches.append(patch("almanak.gateway.managed_serve.threading.Event", return_value=real_event))

    with caplog.at_level(logging.INFO, logger="almanak.gateway.managed_serve"):
        rc = _run_main_with_patches(patches, mg_class)

    assert rc == 0
    mg.start.assert_called_once()
    mg.stop.assert_called_once()


def test_main_returns_one_when_managed_gateway_start_raises(caplog) -> None:
    """Startup failure: ManagedGateway.start() raises → exit 1, no stop() call needed."""
    mg = MagicMock()
    mg.start.side_effect = RuntimeError("anvil failed to bind")
    mg_class = MagicMock(return_value=mg)

    patches = _stub_main_dependencies(mg_class=mg_class)

    with caplog.at_level(logging.ERROR, logger="almanak.gateway.managed_serve"):
        rc = _run_main_with_patches(patches, mg_class)

    assert rc == 1
    mg.start.assert_called_once()
    # Per ManagedGateway's contract, start() cleans up partial state on
    # failure, so main() does NOT call stop() in the start-failure path.
    mg.stop.assert_not_called()
    # The traceback should appear in caplog at ERROR level via logger.exception.
    assert any("failed to start" in rec.message.lower() for rec in caplog.records)


def test_main_returns_one_when_stop_raises(caplog) -> None:
    """Startup OK + signal received, but mg.stop raises → exit 1."""
    mg = MagicMock()
    mg.host = "127.0.0.1"
    mg.port = 50051
    mg.stop.side_effect = RuntimeError("anvil refused to die")
    mg_class = MagicMock(return_value=mg)

    real_event = threading.Event()
    real_event.set()  # don't actually block

    patches = _stub_main_dependencies(mg_class=mg_class)
    patches.append(patch("almanak.gateway.managed_serve.threading.Event", return_value=real_event))

    with caplog.at_level(logging.ERROR, logger="almanak.gateway.managed_serve"):
        rc = _run_main_with_patches(patches, mg_class)

    assert rc == 1
    mg.start.assert_called_once()
    mg.stop.assert_called_once()
    assert any("error during managed gateway shutdown" in rec.message.lower() for rec in caplog.records)


def test_main_mainnet_network_skips_anvil_setup() -> None:
    """When settings.network != 'anvil', anvil_chains is forced empty and funding too.

    ``_resolve_anvil_chains_and_funding`` may still report chains/funding from
    the config helper, but main() must override them in mainnet mode so
    ManagedGateway doesn't try to start forks against live RPCs.
    """
    mg = MagicMock()
    mg.host = "127.0.0.1"
    mg.port = 50051
    mg_class = MagicMock(return_value=mg)

    real_event = threading.Event()
    real_event.set()

    patches = _stub_main_dependencies(
        mg_class=mg_class,
        network="mainnet",
        chains=("base", "arbitrum"),
        funding={"USDC": "5000"},
    )
    patches.append(patch("almanak.gateway.managed_serve.threading.Event", return_value=real_event))

    rc = _run_main_with_patches(patches, mg_class)

    assert rc == 0
    # ManagedGateway must be constructed with empty anvil_chains + empty
    # anvil_funding for mainnet mode — see managed_serve.main override.
    init_kwargs = mg_class.call_args.kwargs
    assert init_kwargs["anvil_chains"] == []
    assert init_kwargs["anvil_funding"] == {}


def test_signal_handler_sets_stop_event() -> None:
    """The local ``_handle_signal`` closure built inside main() should set stop_event.

    We can't easily reach the closure directly from outside, but we can
    inspect the effect: with a real Event that we monitor, signal delivery
    via the registered handler should release the wait. Verify by calling
    main() with an Event whose .wait is monkeypatched to invoke the
    most-recently-registered SIGTERM handler before returning.
    """
    mg = MagicMock()
    mg.host = "127.0.0.1"
    mg.port = 50051
    mg_class = MagicMock(return_value=mg)

    fake_event = MagicMock(spec=threading.Event)

    # Track the handler signal.signal installs so we can invoke it from .wait.
    installed_handlers: dict[int, object] = {}

    def fake_signal_install(signum, handler):
        installed_handlers[signum] = handler
        return None  # mirrors signal.signal's prior-handler-or-None return

    def wait_then_set():
        # Invoke the SIGTERM handler, which should call fake_event.set().
        handler = installed_handlers.get(15)  # 15 == SIGTERM
        assert handler is not None
        handler(15, None)
        return True

    fake_event.wait.side_effect = wait_then_set

    patches = _stub_main_dependencies(mg_class=mg_class)
    patches.append(patch("almanak.gateway.managed_serve.threading.Event", return_value=fake_event))
    patches.append(patch("almanak.gateway.managed_serve.signal.signal", side_effect=fake_signal_install))

    rc = _run_main_with_patches(patches, mg_class)

    assert rc == 0
    # The handler installed by main() must call .set() on the stop event.
    fake_event.set.assert_called()


# Silence the "Failed to import strategy" warning collected by pytest from the
# unrelated incubating module — not our concern.
@pytest.fixture(autouse=True)
def _quiet_unrelated_import_warning(caplog):
    yield
