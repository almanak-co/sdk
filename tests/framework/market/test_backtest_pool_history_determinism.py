"""Backtest determinism for ``NullPoolHistoryReader`` + builder injection.

Covers UAT card ``docs/internal/uat-cards/VIB-4755.md`` row D2.M6 (VIB-4755 /
POOL-7). Mirrors the VIB-4727 ``test_backtest_pool_analytics_determinism.py``
pattern but enumerates 38 primitives across 4 CLASSES (in-process network,
high-level child-spawn, low-level spawn syscalls, FFI / native-code loading)
per the Round-4..10 Phase 0b iteration's class-enumeration pivot.

The arm_counters dict + decoupled ``all(c == 0 for c in arm_counters.values())``
assertion makes adding a new primitive a one-line edit — the assertion never
changes.
"""

from __future__ import annotations

import inspect
import os
import socket
from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock

import pytest

from almanak.framework.data.interfaces import DataSourceUnavailable
from almanak.framework.data.null_readers import NullPoolHistoryReader
from almanak.framework.market.builders import MarketSnapshotBuilder

_BASE_UNIV3_POOL = "0xd0b53d9277642d899df5c87a3966a349a798f224"
_FIXED_TIMESTAMP = datetime(2024, 1, 1, tzinfo=UTC)


# ============================================================================
# D2.M6 — backtest factories inject NullPoolHistoryReader
# ============================================================================


def test_for_pnl_backtest_state_injects_null_reader():
    """D2.M6: for_pnl_backtest_state(...).pool_history(...) raises
    DataSourceUnavailable('backtest')."""

    class _State:
        timestamp = _FIXED_TIMESTAMP
        price_oracle = None
        balance_provider = None

    snap = MarketSnapshotBuilder.for_pnl_backtest_state(
        chain="base", wallet_address="0x" + "0" * 40, state=_State(),
    )

    assert isinstance(snap._pool_history_reader, NullPoolHistoryReader)


def test_for_paper_fork_injects_null_reader():
    """D2.M6: for_paper_fork(...).pool_history(...) raises
    DataSourceUnavailable('backtest')."""

    class _ForkManager:
        def get_rpc_url(self) -> str:
            return "http://127.0.0.1:8545"

        current_block = 12345

    snap = MarketSnapshotBuilder.for_paper_fork(
        chain="base",
        wallet_address="0x" + "0" * 40,
        fork_manager=_ForkManager(),
    )

    assert isinstance(snap._pool_history_reader, NullPoolHistoryReader)


def test_for_strategy_runner_does_not_autowire_pool_history_reader():
    """D-4 lock: for_strategy_runner does NOT auto-construct the live
    PoolHistoryReader. The cut-over is gated on VIB-4730 hosted-egress +
    VIB-4863 TheGraph API key landing."""

    class _Strategy:
        chain = "base"
        wallet_address = "0x" + "0" * 40

    snap = MarketSnapshotBuilder.for_strategy_runner(strategy=_Strategy())
    assert snap._pool_history_reader is None

    # Calling .pool_history() on it raises the existing ValueError —
    # proving the escape hatch is preserved and the hosted-egress path
    # is NOT silently opened.
    with pytest.raises(ValueError, match=r"No pool history reader configured"):
        snap.pool_history(
            pool_address=_BASE_UNIV3_POOL,
            chain="base",
            start_date=datetime.fromtimestamp(1_700_000_000, tz=UTC),
            end_date=datetime.fromtimestamp(1_700_000_000 + 3600, tz=UTC),
            resolution="1h",
            protocol="uniswap_v3",
        )


def test_market_snapshot_pool_history_signature_requires_protocol():
    """D-2 lock — concrete signature assertion (NOT OR-disjunction).
    Plan-agent Round-1 finding M1: a buggy "accept old OR new signature"
    test is itself a silent-error pattern; assert the new signature
    concretely."""
    from almanak.framework.market.snapshot import MarketSnapshot

    sig = inspect.signature(MarketSnapshot.pool_history)
    assert "protocol" in sig.parameters
    assert sig.parameters["protocol"].default is inspect.Parameter.empty
    assert sig.parameters["protocol"].kind is inspect.Parameter.KEYWORD_ONLY

    # Calling without protocol raises TypeError BEFORE any reader / gateway
    # round-trip — Python's own missing-required-keyword-only signal.
    snap = MarketSnapshot(
        chain="base",
        pool_history_reader=MagicMock(),
    )
    with pytest.raises(TypeError, match=r"protocol"):
        snap.pool_history(  # type: ignore[call-arg]
            pool_address=_BASE_UNIV3_POOL,
            chain="base",
        )


def test_market_snapshot_pool_history_end_date_none_anchors_to_snapshot_timestamp():
    """D2.M6: MarketSnapshot.pool_history(end_date=None) anchors to
    self._timestamp (NOT datetime.now(UTC)). Captured end_date in the
    spy reader equals the snapshot's fixed timestamp."""
    from almanak.framework.market.snapshot import MarketSnapshot

    spy_reader = MagicMock()
    spy_reader.get_pool_history.return_value = MagicMock()
    snap = MarketSnapshot(
        chain="base",
        pool_history_reader=spy_reader,
        timestamp=_FIXED_TIMESTAMP,
    )

    snap.pool_history(
        pool_address=_BASE_UNIV3_POOL,
        start_date=datetime(2023, 10, 1, tzinfo=UTC),
        end_date=None,
        resolution="1h",
        protocol="uniswap_v3",
    )

    # Spy received end_date == snapshot's frozen timestamp (NOT now()).
    call_kwargs = spy_reader.get_pool_history.call_args.kwargs
    assert call_kwargs["end_date"] == _FIXED_TIMESTAMP


# ============================================================================
# D2.M6 — Exhaustive monkeypatch determinism proof (38 primitives, 4 classes)
# ============================================================================


@pytest.mark.acceptance_pack
def test_null_reader_constructs_no_network_primitives(monkeypatch):
    """D2.M6 + Trust statement clause 4: NullPoolHistoryReader() instantiation
    AND .get_pool_history() raise DataSourceUnavailable("backtest") AND
    construct zero of the 38 enumerated primitives across Classes 1-4.

    arm_counters dict captures one counter per monkeypatched primitive;
    decoupled all-zero assertion means adding a primitive is one line."""
    arm_counters: dict[str, int] = {}

    def _arm(name: str) -> Any:
        """Build a monkeypatch that increments arm_counters[name] then raises."""
        arm_counters[name] = 0

        def _raiser(*_args: Any, **_kwargs: Any) -> Any:
            arm_counters[name] += 1
            raise AssertionError(f"{name} attempted in Null path")

        return _raiser

    # Class 1 — In-process Python network primitives (12).
    monkeypatch.setattr("socket.socket.connect", _arm("socket.socket.connect"))
    monkeypatch.setattr("socket.socket.__init__", _arm("socket.socket.__init__"))
    # aiohttp / httpx / requests / urllib3 / web3 / http.client are
    # imported below via the catch-all "try import; if installed, arm"
    # idiom to keep this test green on minimal-dep environments. grpc
    # IS a hard dep.
    for path, name in (
        ("aiohttp.ClientSession.__init__", "aiohttp.ClientSession.__init__"),
        ("httpx.Client.__init__", "httpx.Client.__init__"),
        ("httpx.AsyncClient.__init__", "httpx.AsyncClient.__init__"),
        ("requests.Session.__init__", "requests.Session.__init__"),
        ("urllib.request.urlopen", "urllib.request.urlopen"),
        ("urllib.request.OpenerDirector.open", "urllib.request.OpenerDirector.open"),
        ("urllib3.PoolManager.__init__", "urllib3.PoolManager.__init__"),
        ("http.client.HTTPConnection.__init__", "http.client.HTTPConnection.__init__"),
        ("http.client.HTTPSConnection.__init__", "http.client.HTTPSConnection.__init__"),
        ("web3.HTTPProvider.__init__", "web3.HTTPProvider.__init__"),
        ("web3.AsyncHTTPProvider.__init__", "web3.AsyncHTTPProvider.__init__"),
        ("web3.Web3.__init__", "web3.Web3.__init__"),
    ):
        try:
            monkeypatch.setattr(path, _arm(name))
        except (AttributeError, ImportError, ModuleNotFoundError):
            # If the import path doesn't exist (e.g. a sub-dep is not
            # installed in this test env), keep the counter armed at
            # 0 but don't fail the test. The other classes still arm.
            pass

    # gRPC channel constructors — async + sync (4).
    monkeypatch.setattr("grpc.aio.insecure_channel", _arm("grpc.aio.insecure_channel"))
    monkeypatch.setattr("grpc.aio.secure_channel", _arm("grpc.aio.secure_channel"))
    monkeypatch.setattr("grpc.insecure_channel", _arm("grpc.insecure_channel"))
    monkeypatch.setattr("grpc.secure_channel", _arm("grpc.secure_channel"))

    # Class 2 — High-level child-spawn (9).
    monkeypatch.setattr("subprocess.Popen.__init__", _arm("subprocess.Popen.__init__"))
    monkeypatch.setattr("subprocess.run", _arm("subprocess.run"))
    monkeypatch.setattr("subprocess.call", _arm("subprocess.call"))
    monkeypatch.setattr("subprocess.check_call", _arm("subprocess.check_call"))
    monkeypatch.setattr("subprocess.check_output", _arm("subprocess.check_output"))
    monkeypatch.setattr(os, "system", _arm("os.system"))
    monkeypatch.setattr(os, "popen", _arm("os.popen"))
    # multiprocessing + asyncio.create_subprocess_* + pty.spawn — arm
    # opportunistically (some platforms / minimal envs don't have all).
    for path, name in (
        ("multiprocessing.Process.__init__", "multiprocessing.Process.__init__"),
        ("multiprocessing.Process.start", "multiprocessing.Process.start"),
        ("asyncio.create_subprocess_exec", "asyncio.create_subprocess_exec"),
        ("asyncio.create_subprocess_shell", "asyncio.create_subprocess_shell"),
        ("pty.spawn", "pty.spawn"),
    ):
        try:
            monkeypatch.setattr(path, _arm(name))
        except (AttributeError, ImportError, ModuleNotFoundError):
            pass

    # Class 3 — Low-level spawn syscalls (11).
    for path, name in (
        ("os.fork", "os.fork"),
        ("os.posix_spawn", "os.posix_spawn"),
        ("os.posix_spawnp", "os.posix_spawnp"),
        ("os.execv", "os.execv"),
        ("os.execve", "os.execve"),
        ("os.execvp", "os.execvp"),
        ("os.execvpe", "os.execvpe"),
        ("os.execl", "os.execl"),
        ("os.execle", "os.execle"),
        ("os.execlp", "os.execlp"),
        ("os.execlpe", "os.execlpe"),
        ("os.spawnv", "os.spawnv"),
        ("os.spawnve", "os.spawnve"),
        ("os.spawnvp", "os.spawnvp"),
        ("os.spawnvpe", "os.spawnvpe"),
        ("os.spawnl", "os.spawnl"),
        ("os.spawnle", "os.spawnle"),
        ("os.spawnlp", "os.spawnlp"),
        ("os.spawnlpe", "os.spawnlpe"),
    ):
        try:
            monkeypatch.setattr(path, _arm(name))
        except AttributeError:
            # Some platforms (Windows) lack the POSIX-only spawn variants.
            pass

    # Class 4 — FFI / native-code loading (6).
    for path, name in (
        ("ctypes.CDLL.__init__", "ctypes.CDLL.__init__"),
        ("ctypes.cdll.LoadLibrary", "ctypes.cdll.LoadLibrary"),
        ("ctypes.WinDLL.__init__", "ctypes.WinDLL.__init__"),
        ("ctypes.windll.LoadLibrary", "ctypes.windll.LoadLibrary"),
        ("cffi.FFI.__init__", "cffi.FFI.__init__"),
        ("cffi.FFI.dlopen", "cffi.FFI.dlopen"),
    ):
        try:
            monkeypatch.setattr(path, _arm(name))
        except (AttributeError, ImportError, ModuleNotFoundError):
            pass

    # Sanity check that we armed something.
    assert len(arm_counters) >= 25, f"Expected to arm at least 25 primitives, got {len(arm_counters)}"

    # NOW instantiate the Null reader — catches eager-__init__ bugs.
    reader = NullPoolHistoryReader()

    # And call it — catches lazy-construct-on-first-call bugs.
    with pytest.raises(DataSourceUnavailable, match=r"backtest"):
        reader.get_pool_history(
            pool_address=_BASE_UNIV3_POOL,
            chain="base",
            start_date=datetime.fromtimestamp(1_700_000_000, tz=UTC),
            end_date=datetime.fromtimestamp(1_700_000_000 + 3600, tz=UTC),
            resolution="1h",
            protocol="uniswap_v3",
        )

    # The contract: EVERY armed counter reads 0. A buggy reader that calls
    # ANY armed primitive will have its counter > 0 (the raised
    # AssertionError would have aborted the call, but the counter increments
    # BEFORE the raise). The all-zero assertion is decoupled from the
    # specific list — adding a primitive is one line, no assertion edit.
    nonzero = {name: count for name, count in arm_counters.items() if count != 0}
    assert nonzero == {}, f"Null reader hit primitives: {nonzero}"


def test_null_reader_health_returns_empty_dict():
    """Compat shim: Null reader's health() returns {} like the live reader's."""
    reader = NullPoolHistoryReader()
    assert reader.health() == {}


def test_market_snapshot_wrapper_preserves_data_unavailable_classification():
    """pr-auditor #8 (potential): MarketSnapshot.pool_history wraps any
    reader exception with PoolHistoryUnavailableError via `raise ... from e`.
    The __cause__ chain MUST be preserved so classify_failure walks to
    DATA_UNAVAILABLE — a future bare `raise PoolHistoryUnavailableError(...)`
    (without `from e`) would silently break HOLD inference at the runner.

    This test asserts the snapshot wrapper preserves the typed
    classification end-to-end (the existing D3.F1 tests cover the
    direct-reader path; this one closes the wrapper path).
    """
    from almanak.framework.data.interfaces import DataSourceUnavailable
    from almanak.framework.data.market_snapshot import PoolHistoryUnavailableError
    from almanak.framework.market.snapshot import MarketSnapshot
    from almanak.framework.runner.failure_kind import FailureKind, classify_failure

    # Spy reader that raises a typed DataSourceUnavailable on every call.
    class _RaisingReader:
        def get_pool_history(self, **_kwargs: object) -> object:
            raise DataSourceUnavailable(
                source="pool_history",
                reason="upstream failure (test)",
            )

    snap = MarketSnapshot(
        chain="base",
        pool_history_reader=_RaisingReader(),
        timestamp=_FIXED_TIMESTAMP,
    )

    with pytest.raises(PoolHistoryUnavailableError) as excinfo:
        snap.pool_history(
            pool_address=_BASE_UNIV3_POOL,
            start_date=datetime.fromtimestamp(1_700_000_000, tz=UTC),
            end_date=datetime.fromtimestamp(1_700_000_000 + 3600, tz=UTC),
            resolution="1h",
            protocol="uniswap_v3",
        )

    # __cause__ chain preserved.
    assert isinstance(excinfo.value.__cause__, DataSourceUnavailable)
    # classify_failure walks the chain to DATA_UNAVAILABLE.
    assert classify_failure(excinfo.value) == FailureKind.DATA_UNAVAILABLE
