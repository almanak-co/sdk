"""Spark lending teardown via the HF-safe primitive (VIB-5469 / VIB-5417 / ALM-2900).

Spark is an Aave-V3 fork, so its teardown routes through the protocol-generic
``generate_lending_unwind`` primitive (TD-09) rather than a naive
``REPAY(all) → WITHDRAW(all)`` that strands the position on dust debt.

* supply + borrow  → repay → (withdraw/swap staircase) → withdraw-all  (ALM-2900)
* supply only      → withdraw-all                                       (VIB-5417)
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from almanak.framework.teardown.lending_unwind import generate_lending_unwind
from almanak.framework.teardown.models import TeardownMode


class _MockMarket:
    """Minimal MarketSnapshot stand-in for the lending primitive's live reads."""

    def __init__(self, *, debt_usd: Decimal, collateral_usd: Decimal, lltv: Decimal = Decimal("0.83")):
        self._debt = debt_usd
        self._coll = collateral_usd
        self._lltv = lltv

    def price(self, token: str) -> Decimal:
        return {"wstETH": Decimal("4000"), "DAI": Decimal("1")}.get(token, Decimal("0"))

    def balance(self, token: str, chain: str | None = None):
        return SimpleNamespace(balance=Decimal("0"))

    def position_health(self, protocol, market_id, collateral_price_usd=None, debt_price_usd=None):
        return SimpleNamespace(
            collateral_value_usd=self._coll,
            debt_value_usd=self._debt,
            lltv=self._lltv,
        )


def _types(intents) -> list[str]:
    return [i.intent_type.value for i in intents]


# ---------------------------------------------------------------------------
# The primitive directly, with protocol="spark"
# ---------------------------------------------------------------------------


def test_spark_supply_and_borrow_emits_repay_swap_withdraw():
    intents = generate_lending_unwind(
        market=_MockMarket(debt_usd=Decimal("1000"), collateral_usd=Decimal("4000")),
        protocol="spark",
        collateral_token="wstETH",
        borrow_token="DAI",
        chain="ethereum",
        mode=TeardownMode.SOFT,
        consolidate_to="wstETH",
    )
    types = _types(intents)
    # Every leg must be exercised: repay the debt and withdraw the collateral.
    assert "REPAY" in types
    assert "WITHDRAW" in types
    assert "SWAP" in types
    # The repay targets the debt token and the withdraw targets the collateral.
    repays = [i for i in intents if i.intent_type.value == "REPAY"]
    withdraws = [i for i in intents if i.intent_type.value == "WITHDRAW"]
    assert all(i.protocol == "spark" for i in repays + withdraws)
    assert any(i.token == "DAI" for i in repays)
    assert any(i.token == "wstETH" for i in withdraws)
    # The final withdraw is a withdraw-all (live-resolved), never a stale amount.
    assert withdraws[-1].withdraw_all is True
    # The TERMINAL swap sweeps the residual back into the collateral asset
    # (consolidate_to="wstETH") — not the default collateral→debt direction. Pin
    # the direction so a regression to a debt-denominated terminal sweep is caught.
    swaps = [i for i in intents if i.intent_type.value == "SWAP"]
    assert swaps[-1].to_token == "wstETH"


def test_spark_supply_only_emits_withdraw_all():
    intents = generate_lending_unwind(
        market=_MockMarket(debt_usd=Decimal("0"), collateral_usd=Decimal("4000")),
        protocol="spark",
        collateral_token="wstETH",
        borrow_token="DAI",
        chain="ethereum",
        mode=TeardownMode.SOFT,
        consolidate_to="wstETH",
    )
    types = _types(intents)
    assert types == ["WITHDRAW"]
    assert intents[0].withdraw_all is True
    assert intents[0].token == "wstETH"
    # No swap on a pure supply close when consolidating to the collateral asset.
    assert "SWAP" not in types


# ---------------------------------------------------------------------------
# spark_dai_eth_lifecycle.generate_teardown_intents routes through the primitive
# ---------------------------------------------------------------------------


def _make_lifecycle(*, supplied: Decimal, borrowed: Decimal):
    from strategies.incubating.spark_dai_eth_lifecycle.strategy import SparkDaiEthLifecycleStrategy

    with patch.object(SparkDaiEthLifecycleStrategy, "__init__", lambda self, *a, **k: None):
        strat = SparkDaiEthLifecycleStrategy.__new__(SparkDaiEthLifecycleStrategy)
    strat._chain = "ethereum"
    strat.collateral_token = "wstETH"
    strat.borrow_token = "DAI"
    strat._supplied_amount = supplied
    strat._borrowed_amount = borrowed
    return strat


def test_lifecycle_supply_and_borrow_routes_through_primitive():
    strat = _make_lifecycle(supplied=Decimal("1"), borrowed=Decimal("1000"))
    market = _MockMarket(debt_usd=Decimal("1000"), collateral_usd=Decimal("4000"))
    intents = strat.generate_teardown_intents(TeardownMode.SOFT, market=market)
    types = _types(intents)
    assert "REPAY" in types and "WITHDRAW" in types and "SWAP" in types


def test_lifecycle_supply_only_routes_to_withdraw_all():
    strat = _make_lifecycle(supplied=Decimal("1"), borrowed=Decimal("0"))
    market = _MockMarket(debt_usd=Decimal("0"), collateral_usd=Decimal("4000"))
    intents = strat.generate_teardown_intents(TeardownMode.SOFT, market=market)
    assert _types(intents) == ["WITHDRAW"]
    assert intents[0].withdraw_all is True


def test_lifecycle_nothing_open_returns_empty():
    strat = _make_lifecycle(supplied=Decimal("0"), borrowed=Decimal("0"))
    assert (
        strat.generate_teardown_intents(
            TeardownMode.SOFT, market=_MockMarket(debt_usd=Decimal("0"), collateral_usd=Decimal("0"))
        )
        == []
    )


def test_lifecycle_collateral_reported_as_supply_position():
    """Collateral must be a SUPPLY leg (closed by WITHDRAW), not a held TOKEN."""
    from almanak.framework.teardown.models import PositionType

    strat = _make_lifecycle(supplied=Decimal("1"), borrowed=Decimal("0"))
    strat._deployment_id = "spark-dep"
    strat.create_market_snapshot = lambda: _MockMarket(debt_usd=Decimal("0"), collateral_usd=Decimal("4000"))
    summary = strat.get_open_positions()
    coll = [p for p in summary.positions if p.position_id == "spark_wsteth_collateral"]
    assert coll and coll[0].position_type == PositionType.SUPPLY


# ---------------------------------------------------------------------------
# spark_lender (supply-only demo) closes via full_close withdraw_all
# ---------------------------------------------------------------------------


def test_spark_lender_supply_only_withdraw_all():
    from almanak.demo_strategies.spark_lender.strategy import SparkLenderStrategy

    with patch.object(SparkLenderStrategy, "__init__", lambda self, *a, **k: None):
        strat = SparkLenderStrategy.__new__(SparkLenderStrategy)
    strat._chain = "ethereum"
    strat.supply_token = "WETH"
    strat._supplied = True
    strat._supplied_amount = Decimal("1")
    strat._deployment_id = "spark-lender-dep"
    strat.STRATEGY_NAME = "demo_spark_lender"
    strat.create_market_snapshot = lambda: MagicMock(price=lambda t: Decimal("4000"))

    intents = strat.generate_teardown_intents(TeardownMode.SOFT)
    assert _types(intents) == ["WITHDRAW"]
    assert intents[0].withdraw_all is True
    assert intents[0].token == "WETH"
    assert intents[0].protocol == "spark"
