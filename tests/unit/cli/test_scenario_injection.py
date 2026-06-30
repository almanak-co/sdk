"""Unit tests for `almanak strat test --inject` scenario injection (VIB-5529).

Two layers:

1. ``parse_scenario`` — the typed override parser (inline JSON, file path,
   validation / rejection of malformed documents).
2. ``apply_scenario`` — proves that seeding overrides onto a real
   ``MarketSnapshot`` makes a condition-driven ``decide()`` take the expected
   non-HOLD branch, and that WITHOUT injection the same strategy HOLDs (the
   force-action short-circuit bug this feature exists to close).
"""

from __future__ import annotations

import json
from decimal import Decimal

import pytest

from almanak.framework.cli._scenario import (
    ScenarioOverrides,
    ScenarioParseError,
    apply_scenario,
    parse_scenario,
)
from almanak.framework.market import MarketSnapshot

# --------------------------------------------------------------------------- #
# Layer 1 — parse_scenario
# --------------------------------------------------------------------------- #


def test_parse_inline_json_all_sections():
    raw = '{"prices": {"USDC": "0.95"}, "balances": {"WETH": "5"}, "indicators": {"rsi": {"WETH": 25}}}'
    overrides = parse_scenario(raw)
    assert overrides.prices == {"USDC": Decimal("0.95")}
    assert overrides.balances == {"WETH": Decimal("5")}
    assert overrides.rsi == {"WETH": Decimal("25")}
    assert not overrides.is_empty()


def test_parse_from_file(tmp_path):
    doc = {"prices": {"DAI": "0.97"}}
    path = tmp_path / "scenario.json"
    path.write_text(json.dumps(doc), encoding="utf-8")
    overrides = parse_scenario(str(path))
    assert overrides.prices == {"DAI": Decimal("0.97")}


def test_parse_rejects_unknown_top_level_key():
    # A typo like "price" (vs "prices") must fail loudly, not silently no-op.
    with pytest.raises(ScenarioParseError, match="unknown key"):
        parse_scenario('{"price": {"USDC": "0.95"}}')


def test_parse_rejects_unknown_indicator():
    with pytest.raises(ScenarioParseError, match="unknown indicator"):
        parse_scenario('{"indicators": {"macd": {"WETH": 1}}}')


def test_parse_rejects_non_numeric_price():
    with pytest.raises(ScenarioParseError, match="not a valid number"):
        parse_scenario('{"prices": {"USDC": "cheap"}}')


def test_parse_rejects_boolean_value():
    with pytest.raises(ScenarioParseError, match="boolean"):
        parse_scenario('{"prices": {"USDC": true}}')


def test_parse_rejects_rsi_out_of_range():
    with pytest.raises(ScenarioParseError, match="0..100"):
        parse_scenario('{"indicators": {"rsi": {"WETH": 250}}}')


def test_parse_rejects_empty_document():
    with pytest.raises(ScenarioParseError, match="no overrides"):
        parse_scenario("{}")


def test_parse_rejects_invalid_json():
    with pytest.raises(ScenarioParseError, match="invalid JSON"):
        parse_scenario("{not json}")


def test_parse_rejects_missing_file():
    with pytest.raises(ScenarioParseError, match="neither inline JSON"):
        parse_scenario("/no/such/scenario.json")


# --------------------------------------------------------------------------- #
# Layer 2 — apply_scenario drives a real condition-triggered decide()
# --------------------------------------------------------------------------- #


def _normal_oracle(token: str, quote: str = "USD", chain: str | None = None) -> Decimal:
    """Stub price oracle representing a calm market (stable on peg, WETH $2500)."""
    return {"USDC": Decimal("1.0"), "WETH": Decimal("2500")}.get(token.upper(), Decimal("1.0"))


def _normal_rsi(token: str, period: int = 14, timeframe: str | None = None):
    from almanak.framework.market.models import RSIData

    return RSIData(value=Decimal("50"))  # neutral


def _make_snapshot() -> MarketSnapshot:
    """A snapshot wired with live-ish providers (calm market) and seeded balances."""
    from almanak.framework.market.models import TokenBalance

    snap = MarketSnapshot(
        chain="ethereum",
        wallet_address="0x0000000000000000000000000000000000000001",
        price_oracle=_normal_oracle,
        rsi_provider=_normal_rsi,
    )
    # Baseline portfolio: 4 WETH @ $2500 + 1000 USDC = $11_000 (above the
    # drawdown threshold the decision function below uses).
    snap.set_balance("WETH", TokenBalance(symbol="WETH", balance=Decimal("4"), balance_usd=Decimal("10000")))
    snap.set_balance("USDC", TokenBalance(symbol="USDC", balance=Decimal("1000"), balance_usd=Decimal("1000")))
    return snap


def _decide(market: MarketSnapshot) -> str:
    """Representative condition-triggered decision (the logic --inject must reach).

    Mirrors the real-strategy shape the repro flagged: depeg guard, oversold
    entry, drawdown de-risk — all read through the normal MarketSnapshot API.
    """
    if market.price("USDC") < Decimal("0.97"):
        return "EXIT_DEPEG"
    if market.rsi("WETH").value < Decimal("30"):
        return "BUY_OVERSOLD"
    if market.total_portfolio_usd() < Decimal("9000"):
        return "DERISK_DRAWDOWN"
    return "HOLD"


def test_baseline_no_injection_holds():
    # Sanity: the calm-market snapshot HOLDs. Absent injection, behavior is unchanged.
    assert _decide(_make_snapshot()) == "HOLD"


def test_inject_rsi_triggers_oversold_entry():
    snap = _make_snapshot()
    applied = apply_scenario(snap, parse_scenario('{"indicators": {"rsi": {"WETH": 20}}}'))
    assert any("rsi[WETH]" in a for a in applied)
    assert _decide(snap) == "BUY_OVERSOLD"


def test_inject_price_triggers_depeg_branch():
    snap = _make_snapshot()
    apply_scenario(snap, parse_scenario('{"prices": {"USDC": "0.90"}}'))
    assert snap.price("USDC") == Decimal("0.90")
    assert _decide(snap) == "EXIT_DEPEG"


def test_inject_balances_and_prices_trigger_drawdown():
    snap = _make_snapshot()
    # Crash WETH price and shrink balances so portfolio NAV falls below $9000.
    apply_scenario(
        snap,
        parse_scenario('{"prices": {"WETH": "1500"}, "balances": {"WETH": "2", "USDC": "1000"}}'),
    )
    # 2 WETH @ $1500 + 1000 USDC = $4000 < $9000
    assert _decide(snap) == "DERISK_DRAWDOWN"


def test_apply_balance_usd_uses_overridden_price():
    snap = _make_snapshot()
    apply_scenario(snap, ScenarioOverrides(prices={"WETH": Decimal("2000")}, balances={"WETH": Decimal("3")}))
    # balance_usd must reflect the injected price (3 * 2000 = 6000), not the oracle's 2500.
    assert snap.balance_usd("WETH") == Decimal("6000")


def test_apply_returns_descriptions_for_logging():
    snap = _make_snapshot()
    applied = apply_scenario(
        snap,
        ScenarioOverrides(
            prices={"USDC": Decimal("0.95")},
            balances={"WETH": Decimal("1")},
            rsi={"WETH": Decimal("25")},
        ),
    )
    joined = " ".join(applied)
    assert "price[USDC]=0.95" in joined
    assert "balance[WETH]=1" in joined
    assert "rsi[WETH]=25" in joined


# --------------------------------------------------------------------------- #
# Layer 3 — the test lifecycle registers a working hook on the runner
# --------------------------------------------------------------------------- #


def test_lifecycle_registers_override_hook():
    """Passing ``inject=`` must register a runner hook that seeds the snapshot."""
    from unittest.mock import AsyncMock, MagicMock

    from almanak.framework.cli.run_helpers import _run_test_lifecycle
    from almanak.framework.runner.runner_models import IterationResult, IterationStatus

    runner = MagicMock()
    runner.setup_gateway_integration = MagicMock()
    runner.teardown_gateway_integration = MagicMock()
    runner._emit_iteration_summary = MagicMock()
    runner.run_iteration = AsyncMock(side_effect=[IterationResult(status=IterationStatus.HOLD, deployment_id="S:abc")])
    runner.config = MagicMock(enable_state_persistence=False)
    runner._snapshot_override_hook = None

    strategy = MagicMock(spec=["deployment_id", "STRATEGY_NAME", "chain", "force_action", "load_state_async"])
    strategy.deployment_id = "S:abc"
    strategy.STRATEGY_NAME = "S"
    strategy.chain = "ethereum"
    strategy.force_action = ""
    strategy.load_state_async = AsyncMock(return_value=False)
    strategy._wallet_activity_provider = None

    overrides = ScenarioOverrides(prices={"USDC": Decimal("0.95")})

    _run_test_lifecycle(
        runner=runner,
        strategy_instance=strategy,
        state_manager=MagicMock(),
        cleanup_fn=AsyncMock(),
        actions=[""],
        teardown=False,
        json_output=True,
        inject=overrides,
    )

    # The hook was registered and actually applies overrides to a snapshot.
    assert callable(runner._snapshot_override_hook)
    snap = _make_snapshot()
    runner._snapshot_override_hook(snap)
    assert snap.price("USDC") == Decimal("0.95")
