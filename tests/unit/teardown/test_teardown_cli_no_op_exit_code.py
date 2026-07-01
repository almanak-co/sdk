"""VIB-3705 — `almanak strat teardown execute` no-op exit-code regression guard.

Pins the contract that the CLI distinguishes "nothing to tear down" (exit 0,
canonical no-op log line) from "teardown attempted but failed" (exit 1).

The April 28-29 QA batch reported 5+ false failures on swap-only strategies
(``uniswap_v4_swap_*``, ``fluid_swap_*``, ``edge_yield_*_fluiddex``,
``edge_yield_base_univ4``) that legitimately have nothing to close when the
wallet's balance for the strategy's quote/target token is 0. Treating those
runs as exit 1 led to two misclassified bug tickets (BUG-28 UniV4 + BUG-29
Fluid) that turned out to be exit-code semantics, not real bugs.

Three "nothing to do" branches must exit 0:

1. ``strategy.get_open_positions()`` returns an empty summary (the most common
   case for HOLD-state swap-only strategies — covered by
   ``test_no_open_positions_exits_zero``).
2. ``strategy.generate_teardown_intents()`` returned an empty list, so
   ``TeardownManager.execute()`` returns ``_empty_result(success=True,
   intents_total=0)`` (covered by ``test_empty_intents_exits_zero``).
3. Every queued teardown intent resolved to a no-op (e.g. zero-balance SWAP
   short-circuit) so the manager reports ``success=True`` with zero failures
   (covered by ``test_all_intents_skipped_as_no_op_exits_zero``).

A real teardown failure (orchestrator raises, intent compilation fails, etc.)
must still exit 1 — the regression guard for that lives in
``test_real_teardown_failure_still_exits_one``.
"""

from __future__ import annotations

import importlib
import json
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from click.testing import CliRunner

teardown_cli_module = importlib.import_module("almanak.framework.cli.teardown")


_NO_OP_LOG_PHRASE = "nothing to close. Exiting 0."


@pytest.fixture
def cli_runner() -> CliRunner:
    return CliRunner()


# ---------------------------------------------------------------------------
# Shared fakes — keep these tiny; the goal is to drive the CLI's exit-code
# decision without spinning a real strategy / gateway / orchestrator.
# ---------------------------------------------------------------------------


class _FakeGatewayClient:
    """Minimal gateway client that satisfies the CLI's connect/health probe."""

    def __init__(self, _config) -> None:
        self.connected = False
        self.channel = None

    def connect(self) -> None:
        self.connected = True

    def health_check(self) -> bool:
        return True

    def disconnect(self) -> None:
        self.connected = False

    def eth_call(self, chain=None, to=None, data=None, block=None) -> str:  # noqa: ARG002
        # A real healthy gateway answers the teardown residual-discovery order-count
        # read (VIB-5116) with 0 for a wallet that holds no GMX orders — a MEASURED
        # empty book, so no residual is surfaced and the swap-only no-op path exits
        # 0. Returning a zero word (uint 0 / empty bytes32[]) keeps this fake
        # realistic; without it the discovery would fail-closed-loud on an
        # unanswerable read (which is the correct behaviour for a genuinely broken
        # gateway, but not what these no-op tests intend to exercise).
        return "0x" + ("0" * 64)


def _write_swap_only_strategy_files(tmp_path) -> tuple[str, str]:
    """Create the minimal strategy.py + config.json the CLI needs to load.

    The test patches ``load_strategy_from_file`` so the .py file content
    doesn't matter — but the file must exist (the CLI checks for it before
    delegating to the loader).
    """
    strategy_file = tmp_path / "strategy.py"
    strategy_file.write_text("# placeholder — load_strategy_from_file is monkeypatched\n")

    config_file = tmp_path / "config.json"
    config_file.write_text(
        json.dumps(
            {
                "chain": "arbitrum",
                "wallet_address": "0x0000000000000000000000000000000000000001",
                "deployment_id": "swap_only_strategy",
            }
        )
    )
    return str(strategy_file), str(config_file)


def _patch_gateway_and_resolver(monkeypatch: pytest.MonkeyPatch) -> None:
    """Wire the fake gateway client and silence TokenResolver gateway plumbing."""
    monkeypatch.setattr("almanak.framework.gateway_client.GatewayClient", _FakeGatewayClient)
    monkeypatch.setattr(
        "almanak.framework.data.tokens.resolver.TokenResolver.set_gateway_channel",
        lambda _self, _channel: None,
    )


# ---------------------------------------------------------------------------
# Branch 1 — `get_open_positions()` returns an empty summary.
# ---------------------------------------------------------------------------


class _SwapOnlyHoldStrategy:
    """Mimics a swap-only strategy in HOLD state — no positions, no intents."""

    STRATEGY_NAME = "swap_only_strategy"

    def __init__(self, config, chain: str, wallet_address: str) -> None:
        self.config = config
        self.chain = chain
        self.wallet_address = wallet_address
        self.deployment_id = "swap_only_strategy"
        self.generate_teardown_called = False

    def get_open_positions(self):
        return SimpleNamespace(positions=[])

    def create_market_snapshot(self):
        return SimpleNamespace(get_price_oracle_dict=lambda: {})

    def generate_teardown_intents(self, _mode, market=None):
        self.generate_teardown_called = True
        return []


def test_no_open_positions_exits_zero(
    cli_runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    """Branch 1: ``get_open_positions()`` empty → exit 0 + canonical log line.

    This is the dominant path for swap-only strategies that haven't bought
    anything (HOLD state, balance for the quote token is 0). The QA harness
    used to report exit 1 here as a real failure; this test pins exit 0.
    """
    _, config_file = _write_swap_only_strategy_files(tmp_path)

    monkeypatch.setattr(
        teardown_cli_module,
        "load_strategy_from_file",
        lambda _path: (_SwapOnlyHoldStrategy, None),
    )
    _patch_gateway_and_resolver(monkeypatch)

    result = cli_runner.invoke(
        teardown_cli_module.teardown,
        [
            "execute",
            "-d",
            str(tmp_path),
            "-c",
            config_file,
            "--no-gateway",
            "--mode",
            "graceful",
            "--force",
        ],
    )

    assert result.exit_code == 0, result.output
    assert _NO_OP_LOG_PHRASE in result.output
    # The canonical phrase must include the deployment id so QA logs are greppable.
    assert "swap_only_strategy" in result.output
    # The CLI must short-circuit BEFORE invoking the manager — no need to
    # generate teardown intents when there are no positions.
    instance = _LastInstanceTracker.last
    assert instance is not None
    assert instance.generate_teardown_called is False


# ---------------------------------------------------------------------------
# Branch 2 — `generate_teardown_intents()` returns an empty list, so the
# TeardownManager builds an _empty_result(success=True, intents_total=0).
# ---------------------------------------------------------------------------


class _LastInstanceTracker:
    """Module-level attribute used by the strategy classes below to expose
    the last constructed instance to the test for inspection (the CLI owns
    instantiation, so the test cannot capture the instance directly)."""

    last: object | None = None


# Re-bind _SwapOnlyHoldStrategy.__init__ to record the last instance. Done at
# class level so every instance constructed by the CLI is observable.
_orig_swap_only_init = _SwapOnlyHoldStrategy.__init__


def _tracking_swap_only_init(self, config, chain, wallet_address) -> None:
    _orig_swap_only_init(self, config, chain, wallet_address)
    _LastInstanceTracker.last = self


_SwapOnlyHoldStrategy.__init__ = _tracking_swap_only_init  # type: ignore[method-assign]


class _StrategyWithPositionAndEmptyIntents:
    """Strategy that reports a position but generates zero teardown intents.

    This drives the CLI past the empty-positions short-circuit and into the
    TeardownManager, which then returns ``_empty_result(success=True,
    intents_total=0)``. The CLI must recognize ``intents_total == 0`` and
    emit the canonical no-op log instead of the misleading
    ``[SUCCESS] Teardown completed successfully!`` (which sounds like an
    executed teardown).
    """

    STRATEGY_NAME = "strategy_with_position_no_intents"

    def __init__(self, config, chain: str, wallet_address: str) -> None:
        self.config = config
        self.chain = chain
        self.wallet_address = wallet_address
        self.deployment_id = "strategy_with_position_no_intents"
        _LastInstanceTracker.last = self

    async def pause(self) -> None:
        return None

    def get_open_positions(self):
        position = SimpleNamespace(
            position_type=SimpleNamespace(value="token"),
            protocol="uniswap_v4",
            chain=self.chain,
            position_id="dust_position",
            value_usd=Decimal("0.01"),
            health_factor=None,
            details={},
        )
        return SimpleNamespace(
            positions=[position],
            total_value_usd=Decimal("0.01"),
            chains_involved={self.chain},
            has_liquidation_risk=False,
        )

    def create_market_snapshot(self):
        return SimpleNamespace(get_price_oracle_dict=lambda: {})

    def generate_teardown_intents(self, _mode, market=None):
        return []


def test_empty_intents_exits_zero(
    cli_runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    """Branch 2: ``generate_teardown_intents()`` empty → manager returns
    ``_empty_result`` → CLI must exit 0 with the canonical no-op log.
    """
    _, config_file = _write_swap_only_strategy_files(tmp_path)

    monkeypatch.setattr(
        teardown_cli_module,
        "load_strategy_from_file",
        lambda _path: (_StrategyWithPositionAndEmptyIntents, None),
    )
    _patch_gateway_and_resolver(monkeypatch)

    result = cli_runner.invoke(
        teardown_cli_module.teardown,
        [
            "execute",
            "-d",
            str(tmp_path),
            "-c",
            config_file,
            "--no-gateway",
            "--mode",
            "graceful",
            "--force",
        ],
    )

    assert result.exit_code == 0, result.output
    assert _NO_OP_LOG_PHRASE in result.output
    assert "strategy_with_position_no_intents" in result.output
    # The misleading "[SUCCESS] Teardown completed successfully!" line must
    # NOT fire — that string belongs to executed teardowns, not the no-op
    # path; QA log scanners distinguish the two on these phrases.
    assert "[SUCCESS] Teardown completed successfully!" not in result.output


# ---------------------------------------------------------------------------
# Branch 3 — Every queued intent resolved to a no-op via the zero-balance
# SWAP short-circuit (`_zero_balance_swap_skip_reason`). The TeardownManager
# tallies these as `succeeded` (so result.success=True) but `intents_total > 0`.
# This is a real-world variant of "nothing to do" — the CLI must NOT print
# "[FAILED] Teardown failed" and must NOT call sys.exit(1).
# ---------------------------------------------------------------------------


class _StrategyWithZeroBalanceSwap:
    """Swap-only strategy that has a TOKEN position registered but the
    underlying balance is 0 (e.g. dust below the position threshold), so the
    queued SWAP intent is short-circuited by ``_zero_balance_swap_skip_reason``.
    """

    STRATEGY_NAME = "strategy_zero_balance_swap"

    def __init__(self, config, chain: str, wallet_address: str) -> None:
        self.config = config
        self.chain = chain
        self.wallet_address = wallet_address
        self.deployment_id = "strategy_zero_balance_swap"
        _LastInstanceTracker.last = self

    async def pause(self) -> None:
        return None

    def get_open_positions(self):
        # Report a non-empty positions list so we get past Branch 1, but
        # set value_usd=0 so SafetyGuard's loss-cap math doesn't try to
        # enforce anything against a real number.
        position = SimpleNamespace(
            position_type=SimpleNamespace(value="token"),
            protocol="uniswap_v4",
            chain=self.chain,
            position_id="phantom",
            value_usd=Decimal("0"),
            health_factor=None,
            details={},
        )
        return SimpleNamespace(
            positions=[position],
            total_value_usd=Decimal("0"),
            chains_involved={self.chain},
            has_liquidation_risk=False,
        )

    def create_market_snapshot(self):
        # market.balance(<token>) returns 0 — this is what triggers the
        # zero-balance skip path inside _execute_intents.
        market = MagicMock()
        market.balance = MagicMock(return_value=SimpleNamespace(balance=Decimal("0")))
        market.get_price_oracle_dict = lambda: {}
        return market

    def generate_teardown_intents(self, _mode, market=None):
        # SwapIntent shape that the zero-balance helper recognizes — must
        # have intent_type containing "SWAP", amount="all", and a from_token.
        return [
            SimpleNamespace(
                intent_type="SWAP",
                amount="all",
                from_token="WETH",
                to_token="USDC",
                chain=self.chain,
                max_slippage=Decimal("0.005"),
            )
        ]


def test_all_intents_skipped_as_no_op_exits_zero(
    cli_runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    """Branch 3: every intent skipped via zero-balance short-circuit →
    ``result.success=True``, ``intents_failed=0``, exit 0.

    This is the regression that BUG-39 (zero-balance skip) closed at the
    manager layer; this test pins the CLI behavior so a future refactor of
    either the manager OR the CLI cannot silently re-introduce exit 1.
    """
    _, config_file = _write_swap_only_strategy_files(tmp_path)

    monkeypatch.setattr(
        teardown_cli_module,
        "load_strategy_from_file",
        lambda _path: (_StrategyWithZeroBalanceSwap, None),
    )
    _patch_gateway_and_resolver(monkeypatch)

    result = cli_runner.invoke(
        teardown_cli_module.teardown,
        [
            "execute",
            "-d",
            str(tmp_path),
            "-c",
            config_file,
            "--no-gateway",
            "--mode",
            "graceful",
            "--force",
        ],
    )

    assert result.exit_code == 0, result.output
    # Whether the manager fires its [SUCCESS] line or the CLI surfaces the
    # canonical no-op message, the [FAILED] line must NEVER appear when
    # nothing actually failed.
    assert "[FAILED]" not in result.output


# ---------------------------------------------------------------------------
# Regression guard — a real teardown failure must still exit 1.
# ---------------------------------------------------------------------------


class _StrategyThatRaisesOnIntents:
    """Strategy whose ``generate_teardown_intents`` raises — proves the CLI
    still exits 1 when a real failure occurs and that the no-op path doesn't
    accidentally swallow legitimate errors."""

    STRATEGY_NAME = "strategy_that_raises"

    def __init__(self, config, chain: str, wallet_address: str) -> None:
        self.config = config
        self.chain = chain
        self.wallet_address = wallet_address
        self.deployment_id = "strategy_that_raises"
        _LastInstanceTracker.last = self

    async def pause(self) -> None:
        return None

    def get_open_positions(self):
        position = SimpleNamespace(
            position_type=SimpleNamespace(value="token"),
            protocol="uniswap_v4",
            chain=self.chain,
            position_id="real_position",
            value_usd=Decimal("100"),
            health_factor=None,
            details={},
        )
        return SimpleNamespace(
            positions=[position],
            total_value_usd=Decimal("100"),
            chains_involved={self.chain},
            has_liquidation_risk=False,
        )

    def create_market_snapshot(self):
        return SimpleNamespace(get_price_oracle_dict=lambda: {})

    def generate_teardown_intents(self, _mode, market=None):
        raise RuntimeError("synthetic failure — connector exploded")


def test_real_teardown_failure_still_exits_one(
    cli_runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    """Regression guard: a real teardown failure must still exit non-zero.

    The VIB-3705 fix tightens the no-op path; it must NOT mask genuine
    failures. ``generate_teardown_intents`` raising is the most direct way
    to drive the CLI into its exception path.
    """
    _, config_file = _write_swap_only_strategy_files(tmp_path)

    monkeypatch.setattr(
        teardown_cli_module,
        "load_strategy_from_file",
        lambda _path: (_StrategyThatRaisesOnIntents, None),
    )
    _patch_gateway_and_resolver(monkeypatch)

    result = cli_runner.invoke(
        teardown_cli_module.teardown,
        [
            "execute",
            "-d",
            str(tmp_path),
            "-c",
            config_file,
            "--no-gateway",
            "--mode",
            "graceful",
            "--force",
        ],
    )

    assert result.exit_code != 0, result.output
    assert _NO_OP_LOG_PHRASE not in result.output


# ---------------------------------------------------------------------------
# Direct unit test for the TeardownManager._empty_result contract — pinning
# the CLI's reliance on (success=True, intents_total=0) for Branch 2.
# ---------------------------------------------------------------------------


def test_teardown_manager_empty_result_is_success_with_zero_intents() -> None:
    """``_empty_result`` is the contract Branch 2 in the CLI depends on.

    If a future refactor flips ``_empty_result`` to ``success=False`` or
    sets ``intents_total > 0``, the CLI's Branch 2 detection (line 1027:
    ``if result.success and result.intents_total == 0``) silently breaks
    and swap-only HOLD strategies start emitting ``[SUCCESS] Teardown
    completed successfully!`` — wrong but at least exit-0. Pin both
    invariants here so a regression in either dimension surfaces.
    """
    from almanak.framework.teardown.teardown_manager import TeardownManager

    mgr = TeardownManager()
    result = mgr._empty_result(
        deployment_id="swap_only_strategy",
        mode="graceful",
        started_at=datetime.now(UTC),
    )
    assert result.success is True
    assert result.intents_total == 0
    assert result.intents_succeeded == 0
    assert result.intents_failed == 0
