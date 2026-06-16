"""Durable regression guard: SUPPLY-then-BORROW partial-failure invariant.

PR #2827 migrated 8 incubating lending strategies off a single atomic
``Intent.borrow(collateral_amount>0)`` onto a SUPPLY-then-BORROW flow. An
``IntentSequence`` executes its legs as SEPARATE sequential transactions (it is
NOT atomic), so a SUPPLY can land on-chain while a later BORROW reverts.

The money-path INVARIANT this file locks in for every migrated strategy:

    After a SUPPLY succeeds, if a subsequent leg (BORROW) fails, the strategy's
    next decide() MUST NOT re-emit a SUPPLY intent — otherwise collateral is
    supplied twice = real money loss. The SUPPLY-success handler must advance
    the failure-revert target past the pre-supply IDLE state to a "supplied"
    stable state, so a BORROW failure resumes at BORROW, not SUPPLY.

One strategy (``aave_enso_carry_polygon``) violated this and was fixed in commit
94846156c by phase-separating SUPPLY with a ``SUPPLIED`` stable state. The other
seven were audited as already-correct (they reach the same outcome because their
``_transition`` / decide() logic stamps the previous-stable target to "supplied"
before issuing the BORROW). This parametrized guard proves the invariant holds
for all eight and any future migration.

Design notes
------------
Each strategy is driven through the REAL partial-failure scenario via an explicit,
commented per-strategy adapter (``_Case``). There is NO magic auto-discovery: every
strategy's fixture mirrors its own existing unit test (``test_<name>.py``) — same
``__new__`` bypass, same attribute names, same mock market — so a drift in any
strategy surfaces as a failed assertion here rather than a silent no-op.

The strategies fall into two state-machine shapes:

* Module-constant strategies expose ``IDLE`` / ``SUPPLIED`` constants and a
  ``_state`` (or ``_previous_stable`` / ``_previous_stable_state``) attribute.
* String-literal strategies use bare ``"idle"`` / ``"supplied"`` strings and a
  ``_loop_state`` attribute with ``_collateral_supplied`` for the supplied amount.

``aave_enso_carry_polygon`` is the only one whose SUPPLIED state emits a BORROW+SWAP
*sequence*; the rest emit a single BORROW intent. The scenario body handles both by
flattening the returned intent (sequence or single) and asserting no SUPPLY leg.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import pytest

# =============================================================================
# Shared mock builders (mirror the per-strategy test helpers)
# =============================================================================


def _mock_intent(intent_type_val: str, **attrs: Any) -> MagicMock:
    """Build a mock Intent whose ``intent_type.value`` is ``intent_type_val``."""
    intent = MagicMock()
    intent.intent_type = MagicMock()
    intent.intent_type.value = intent_type_val
    for key, value in attrs.items():
        setattr(intent, key, value)
    return intent


def _mock_result(success: bool = True, **attrs: Any) -> MagicMock:
    """Build a mock execution result."""
    result = MagicMock()
    result.success = success
    # Strategies probe ``result.swap_amounts`` / ``result.position_id`` defensively;
    # default them to falsy so the SUPPLY/BORROW paths under test stay simple.
    result.swap_amounts = attrs.get("swap_amounts", None)
    result.position_id = attrs.get("position_id", None)
    result.extracted_data = attrs.get("extracted_data", None)
    result.transaction_results = attrs.get("transaction_results", [])
    return result


def _flatten(intent: Any) -> list[Any]:
    """Return the leg list for a sequence, or ``[intent]`` for a single intent."""
    if intent is None:
        return []
    # IntentSequence exposes ``.intents``; a single Intent does not.
    legs = getattr(intent, "intents", None)
    if legs is not None:
        return list(legs)
    return [intent]


def _intent_type(intent: Any) -> str | None:
    it = getattr(intent, "intent_type", None)
    if it is None:
        return None
    return it.value if hasattr(it, "value") else str(it)


# =============================================================================
# Per-strategy adapter
# =============================================================================


@dataclass
class _Case:
    """Explicit adapter capturing exactly how to drive one strategy.

    Attributes
    ----------
    name:
        Pytest parametrization id (the strategy folder name).
    build:
        Returns a fully-initialized strategy instance via the strategy's own
        ``__new__`` bypass (mirrors its existing unit-test fixture).
    market:
        Returns a mock ``MarketSnapshot`` whose ``.price`` resolves the tokens
        this strategy looks up, so ``decide()`` does not HOLD on missing prices.
    state_attr:
        Name of the state-string attribute (``_state`` or ``_loop_state``).
    supplied_attr:
        Name of the supplied-collateral-amount attribute
        (``_supplied_amount`` or ``_collateral_supplied``).
    idle_value / supplied_value:
        The state-string values for the pre-supply IDLE state and the post-supply
        stable state. Imported from module constants where they exist.
    supply_amount:
        The collateral amount the strategy supplies from IDLE (its
        ``collateral_amount`` config).
    """

    name: str
    build: Callable[[], Any]
    market: Callable[[], Any]
    state_attr: str
    supplied_attr: str
    idle_value: str
    supplied_value: str
    supply_amount: Decimal = field(default=Decimal("0.05"))

    def state(self, strat: Any) -> str:
        return getattr(strat, self.state_attr)

    def supplied(self, strat: Any) -> Decimal:
        return getattr(strat, self.supplied_attr)


# -----------------------------------------------------------------------------
# Builders — one per strategy, each a faithful copy of that strategy's existing
# test fixture (so the adapter tracks the real strategy, not a guessed shape).
# -----------------------------------------------------------------------------


def _build_aave_enso_carry_polygon() -> Any:
    from strategies.incubating.aave_enso_carry_polygon.strategy import (
        IDLE,
        AaveEnsoCarryPolygonStrategy,
    )

    strat = AaveEnsoCarryPolygonStrategy.__new__(AaveEnsoCarryPolygonStrategy)
    strat._deployment_id = "test-aave-enso-carry-polygon"
    strat._chain = "polygon"
    strat._wallet_address = "0x" + "0" * 40
    strat._config = {}
    strat._hot_config = None
    strat.collateral_token = "WETH"
    strat.collateral_amount = Decimal("0.5")
    strat.borrow_token = "USDC"
    strat.ltv_target = Decimal("0.5")
    strat.swap_to = "WETH"
    strat.max_slippage_pct = Decimal("3.0")
    # Fixed override so IDLE emits a SUPPLY without needing live prices.
    strat.borrow_amount_override = Decimal("300")
    strat._state = IDLE
    strat._previous_stable_state = IDLE
    strat._supplied_amount = Decimal("0")
    strat._borrowed_amount = Decimal("0")
    strat._swap_amount_out = Decimal("0")
    return strat


def _build_aave_v3_pancakeswap_teardown_bsc() -> Any:
    from strategies.incubating.aave_v3_pancakeswap_teardown_bsc.strategy import (
        IDLE,
        AaveV3PancakeswapTeardownBscStrategy,
    )

    strat = AaveV3PancakeswapTeardownBscStrategy.__new__(AaveV3PancakeswapTeardownBscStrategy)
    strat._chain = "bsc"
    strat._wallet_address = "0x" + "0" * 40
    strat._deployment_id = "test-aave-pancake-bsc"
    strat.STRATEGY_NAME = "aave_v3_pancakeswap_teardown_bsc"
    strat.collateral_token = "WBNB"
    strat.collateral_amount = Decimal("0.5")
    strat.borrow_token = "USDC"
    strat.swap_to_token = "USDT"
    strat.ltv_target = Decimal("0.3")
    strat.market = "usdc"
    strat._state = IDLE
    strat._previous_stable = IDLE
    strat._supplied_amount = Decimal("0")
    strat._borrowed_amount = Decimal("0")
    strat._swapped_amount = Decimal("0")
    return strat


def _build_aave_v3_sushiswap_carry_bsc() -> Any:
    from strategies.incubating.aave_v3_sushiswap_carry_bsc.strategy import (
        IDLE,
        AaveV3SushiswapCarryBscStrategy,
    )

    strat = AaveV3SushiswapCarryBscStrategy.__new__(AaveV3SushiswapCarryBscStrategy)
    strat._chain = "bsc"
    strat._wallet_address = "0x" + "0" * 40
    strat._deployment_id = "test-aave-sushi-bsc"
    strat.STRATEGY_NAME = "aave_v3_sushiswap_carry_bsc"
    strat.collateral_token = "WETH"
    strat.collateral_amount = Decimal("0.1")
    strat.borrow_token = "USDC"
    strat.swap_to_token = "USDT"
    strat.ltv_target = Decimal("0.3")
    strat._state = IDLE
    strat._previous_stable = IDLE
    strat._supplied_amount = Decimal("0")
    strat._borrowed_amount = Decimal("0")
    strat._swapped_amount = Decimal("0")
    return strat


def _build_compound_v3_aerodrome_yield_farm_base() -> Any:
    from strategies.incubating.compound_v3_aerodrome_yield_farm_base.strategy import (
        IDLE,
        CompoundV3AerodromeYieldFarmBaseStrategy,
    )

    strat = CompoundV3AerodromeYieldFarmBaseStrategy.__new__(
        CompoundV3AerodromeYieldFarmBaseStrategy
    )
    strat._chain = "base"
    strat._wallet_address = "0x" + "0" * 40
    strat._deployment_id = "test-compound-aero-base"
    strat.STRATEGY_NAME = "compound_v3_aerodrome_yield_farm_base"
    strat.collateral_token = "WETH"
    strat.collateral_amount = Decimal("0.05")
    strat.borrow_token = "USDC"
    strat.compound_market = "usdc"
    strat.ltv_target = Decimal("0.3")
    strat.lp_pool = "WETH/USDC"
    strat.lp_amount0_weth = Decimal("0.005")
    strat.lp_amount1_usdc = Decimal("10")
    strat._state = IDLE
    strat._previous_stable_state = IDLE
    strat._supplied_amount = Decimal("0")
    strat._borrowed_amount = Decimal("0")
    strat._lp_position_active = False
    strat._lp_position_id = None
    return strat


def _build_compound_v3_uniswap_v3_teardown_arbitrum() -> Any:
    from strategies.incubating.compound_v3_uniswap_v3_teardown_arbitrum.strategy import (
        IDLE,
        CompoundV3UniswapV3TeardownArbitrumStrategy,
    )

    strat = CompoundV3UniswapV3TeardownArbitrumStrategy.__new__(
        CompoundV3UniswapV3TeardownArbitrumStrategy
    )
    strat._chain = "arbitrum"
    strat._wallet_address = "0x" + "0" * 40
    strat._deployment_id = "test-compound-univ3-arb"
    strat.STRATEGY_NAME = "compound_v3_uniswap_v3_teardown_arbitrum"
    strat.collateral_token = "WETH"
    strat.collateral_amount = Decimal("0.05")
    strat.borrow_token = "USDC"
    strat.swap_to_token = "USDT"
    strat.ltv_target = Decimal("0.3")
    strat.market = "usdc"
    strat._state = IDLE
    strat._previous_stable = IDLE
    strat._supplied_amount = Decimal("0")
    strat._borrowed_amount = Decimal("0")
    strat._swapped_amount = Decimal("0")
    return strat


def _build_compound_v3_weth_market_arbitrum() -> Any:
    from strategies.incubating.compound_v3_weth_market_arbitrum.strategy import (
        CompoundV3WETHMarketArbitrumStrategy,
    )

    strat = CompoundV3WETHMarketArbitrumStrategy.__new__(CompoundV3WETHMarketArbitrumStrategy)
    strat._chain = "arbitrum"
    strat._wallet_address = "0x" + "0" * 40
    strat._deployment_id = "test-compound-weth-arb"
    strat.STRATEGY_NAME = "compound_v3_weth_market_arbitrum"
    strat.collateral_token = "wstETH"
    strat.collateral_amount = Decimal("0.05")
    strat.borrow_token = "WETH"
    strat.ltv_target = Decimal("0.3")
    strat.market = "weth"
    strat._loop_state = "idle"
    strat._previous_stable_state = "idle"
    strat._collateral_supplied = Decimal("0")
    strat._borrowed_amount = Decimal("0")
    return strat


def _build_morpho_blue_enso_lifecycle_base() -> Any:
    from strategies.incubating.morpho_blue_enso_lifecycle_base.strategy import (
        MorphoBlueEnsoLifecycleBaseStrategy,
    )

    strat = MorphoBlueEnsoLifecycleBaseStrategy.__new__(MorphoBlueEnsoLifecycleBaseStrategy)
    strat._deployment_id = "test-morpho-blue-enso-base"
    strat._chain = "base"
    strat._wallet_address = "0x" + "0" * 40
    strat.collateral_token = "wstETH"
    strat.collateral_amount = Decimal("0.1")
    strat.borrow_token = "USDC"
    strat.swap_to_token = "WETH"
    strat.swap_amount_usd = Decimal("20")
    strat.ltv_target = Decimal("0.3")
    strat.market_id = "0x13c42741a359ac4a8aa8287d2be109dcf28344484f91185f9a79bd5a805a55ae"
    strat._loop_state = "idle"
    strat._previous_stable_state = "idle"
    strat._collateral_supplied = Decimal("0")
    strat._borrowed_amount = Decimal("0")
    return strat


def _build_morpho_uniswap_yield_stack_arbitrum() -> Any:
    from strategies.incubating.morpho_uniswap_yield_stack_arbitrum.strategy import (
        MorphoUniswapYieldStackArbitrumStrategy,
    )

    strat = MorphoUniswapYieldStackArbitrumStrategy.__new__(
        MorphoUniswapYieldStackArbitrumStrategy
    )
    strat._chain = "arbitrum"
    strat._wallet_address = "0x" + "0" * 40
    strat._deployment_id = "test-morpho-univ3-arb"
    strat.collateral_token = "WETH"
    strat.collateral_amount = Decimal("0.05")
    strat.borrow_token = "USDC"
    strat.ltv_target = Decimal("0.3")
    # market_id MUST be set, else decide() HOLDs from IDLE (on-chain discovery
    # is disabled on Anvil forks — VIB-2339). A fake id is enough for unit logic.
    strat.market_id = "0x" + "ab" * 32
    strat.lp_pool = "WETH/USDC/500"
    strat.lp_range_width_pct = Decimal("0.2")
    strat._loop_state = "idle"
    strat._previous_stable_state = "idle"
    strat._collateral_supplied = Decimal("0")
    strat._borrowed_amount = Decimal("0")
    strat._swapped_weth_amount = Decimal("0")
    strat._lp_usdc_amount = Decimal("0")
    strat._lp_position_id = None
    return strat


# -----------------------------------------------------------------------------
# Market factories — each resolves exactly the tokens its strategy prices.
# -----------------------------------------------------------------------------


def _market(prices: dict[str, Decimal]) -> Any:
    market = MagicMock()

    def price_fn(token: str) -> Decimal:
        if token in prices:
            return prices[token]
        raise ValueError(f"Unknown token: {token}")

    market.price = MagicMock(side_effect=price_fn)
    market.balance.return_value = MagicMock(balance=Decimal("10000"))
    return market


# =============================================================================
# Adapter table — 8 explicit rows, no auto-discovery.
# =============================================================================

_CASES: list[_Case] = [
    # aave_enso_carry_polygon: module constants, _previous_stable_state,
    # SUPPLIED emits a BORROW+SWAP *sequence*. supply_amount=collateral_amount=0.5.
    _Case(
        name="aave_enso_carry_polygon",
        build=_build_aave_enso_carry_polygon,
        market=lambda: _market({"WETH": Decimal("2400"), "ETH": Decimal("2400"), "USDC": Decimal("1")}),
        state_attr="_state",
        supplied_attr="_supplied_amount",
        idle_value="idle",
        supplied_value="supplied",
        supply_amount=Decimal("0.5"),
    ),
    # aave_v3_pancakeswap_teardown_bsc: module constants, _previous_stable,
    # SUPPLIED emits a single BORROW. supply_amount=0.5.
    _Case(
        name="aave_v3_pancakeswap_teardown_bsc",
        build=_build_aave_v3_pancakeswap_teardown_bsc,
        market=lambda: _market({"WBNB": Decimal("300"), "USDC": Decimal("1"), "USDT": Decimal("1")}),
        state_attr="_state",
        supplied_attr="_supplied_amount",
        idle_value="idle",
        supplied_value="supplied",
        supply_amount=Decimal("0.5"),
    ),
    # aave_v3_sushiswap_carry_bsc: same shape as pancakeswap. supply_amount=0.1.
    _Case(
        name="aave_v3_sushiswap_carry_bsc",
        build=_build_aave_v3_sushiswap_carry_bsc,
        market=lambda: _market({"WETH": Decimal("2400"), "USDC": Decimal("1"), "USDT": Decimal("1")}),
        state_attr="_state",
        supplied_attr="_supplied_amount",
        idle_value="idle",
        supplied_value="supplied",
        supply_amount=Decimal("0.1"),
    ),
    # compound_v3_aerodrome_yield_farm_base: module constants,
    # _previous_stable_state, SUPPLIED emits a single BORROW. supply_amount=0.05.
    _Case(
        name="compound_v3_aerodrome_yield_farm_base",
        build=_build_compound_v3_aerodrome_yield_farm_base,
        market=lambda: _market({"WETH": Decimal("2400"), "USDC": Decimal("1")}),
        state_attr="_state",
        supplied_attr="_supplied_amount",
        idle_value="idle",
        supplied_value="supplied",
        supply_amount=Decimal("0.05"),
    ),
    # compound_v3_uniswap_v3_teardown_arbitrum: same shape as pancakeswap.
    _Case(
        name="compound_v3_uniswap_v3_teardown_arbitrum",
        build=_build_compound_v3_uniswap_v3_teardown_arbitrum,
        market=lambda: _market({"WETH": Decimal("2400"), "USDC": Decimal("1"), "USDT": Decimal("1")}),
        state_attr="_state",
        supplied_attr="_supplied_amount",
        idle_value="idle",
        supplied_value="supplied",
        supply_amount=Decimal("0.05"),
    ),
    # compound_v3_weth_market_arbitrum: STRING literals, _loop_state,
    # _collateral_supplied. decide() needs prices up front. supply_amount=0.05.
    _Case(
        name="compound_v3_weth_market_arbitrum",
        build=_build_compound_v3_weth_market_arbitrum,
        market=lambda: _market({"wstETH": Decimal("3500"), "WETH": Decimal("3000")}),
        state_attr="_loop_state",
        supplied_attr="_collateral_supplied",
        idle_value="idle",
        supplied_value="supplied",
        supply_amount=Decimal("0.05"),
    ),
    # morpho_blue_enso_lifecycle_base: STRING literals, _loop_state,
    # _collateral_supplied. supply_amount=0.1.
    _Case(
        name="morpho_blue_enso_lifecycle_base",
        build=_build_morpho_blue_enso_lifecycle_base,
        market=lambda: _market({"wstETH": Decimal("2000"), "USDC": Decimal("1"), "WETH": Decimal("2000")}),
        state_attr="_loop_state",
        supplied_attr="_collateral_supplied",
        idle_value="idle",
        supplied_value="supplied",
        supply_amount=Decimal("0.1"),
    ),
    # morpho_uniswap_yield_stack_arbitrum: STRING literals, _loop_state,
    # _collateral_supplied, requires market_id set. supply_amount=0.05.
    _Case(
        name="morpho_uniswap_yield_stack_arbitrum",
        build=_build_morpho_uniswap_yield_stack_arbitrum,
        market=lambda: _market({"WETH": Decimal("3000"), "USDC": Decimal("1")}),
        state_attr="_loop_state",
        supplied_attr="_collateral_supplied",
        idle_value="idle",
        supplied_value="supplied",
        supply_amount=Decimal("0.05"),
    ),
]


# =============================================================================
# The invariant scenario
# =============================================================================


@pytest.mark.parametrize("case", _CASES, ids=[c.name for c in _CASES])
def test_supply_success_then_borrow_failure_never_resupplies(case: _Case) -> None:
    """SUPPLY lands, BORROW reverts -> strategy must NOT re-emit SUPPLY.

    Drives the real partial-failure path through ``decide()`` /
    ``on_intent_executed`` and asserts the money-path invariant: collateral is
    supplied exactly once, and the BORROW-failure resume point is the "supplied"
    stable state — never the pre-supply IDLE state (which would re-supply).
    """
    strat = case.build()
    market = case.market()

    # --- (a) From IDLE, decide() must emit a SUPPLY (single or first leg). ---
    assert case.state(strat) == case.idle_value
    first = strat.decide(market)
    first_legs = _flatten(first)
    assert first_legs, f"{case.name}: decide() from IDLE returned nothing"
    assert _intent_type(first_legs[0]) == "SUPPLY", (
        f"{case.name}: expected SUPPLY from IDLE, got {[_intent_type(i) for i in first_legs]}"
    )
    # The SUPPLY phase should be standalone (not bundled with BORROW) — this is
    # the whole point of the migration. A bundled [SUPPLY, BORROW] sequence would
    # re-supply on revert.
    assert len(first_legs) == 1, (
        f"{case.name}: IDLE should emit a standalone SUPPLY, got a "
        f"{len(first_legs)}-leg sequence {[_intent_type(i) for i in first_legs]}"
    )

    # --- (b) SUPPLY succeeds -> strategy advances to the 'supplied' stable state. ---
    strat.on_intent_executed(
        _mock_intent("SUPPLY", amount=case.supply_amount), True, _mock_result()
    )
    assert case.state(strat) == case.supplied_value, (
        f"{case.name}: after SUPPLY success expected state '{case.supplied_value}', "
        f"got '{case.state(strat)}'"
    )
    supplied_after_supply = case.supplied(strat)
    assert supplied_after_supply > 0, (
        f"{case.name}: supplied amount not recorded after SUPPLY success"
    )

    # --- (c) decide() again -> BORROW leg(s), no SUPPLY. ---
    second = strat.decide(market)
    second_legs = _flatten(second)
    second_types = [_intent_type(i) for i in second_legs]
    assert "BORROW" in second_types, (
        f"{case.name}: expected BORROW after SUPPLIED, got {second_types}"
    )
    assert "SUPPLY" not in second_types, (
        f"{case.name}: SUPPLIED state re-emitted SUPPLY (got {second_types})"
    )

    # --- (d) BORROW fails -> resume point must NOT be the pre-supply IDLE state. ---
    strat.on_intent_executed(
        _mock_intent("BORROW", borrow_amount=Decimal("100")), False, _mock_result(success=False)
    )
    state_after_fail = case.state(strat)
    assert state_after_fail != case.idle_value, (
        f"{case.name}: BORROW failure reverted to IDLE ('{state_after_fail}') — "
        f"next decide() would RE-SUPPLY collateral (double-supply money loss)"
    )
    assert state_after_fail == case.supplied_value, (
        f"{case.name}: BORROW failure should resume at '{case.supplied_value}', "
        f"got '{state_after_fail}'"
    )
    # Collateral tracked, unchanged — supplied exactly once.
    assert case.supplied(strat) == supplied_after_supply, (
        f"{case.name}: supplied amount changed after BORROW failure "
        f"({supplied_after_supply} -> {case.supplied(strat)})"
    )

    # --- (e) Next decide() must NOT re-supply; collateral still unchanged. ---
    retry = strat.decide(market)
    retry_types = [_intent_type(i) for i in _flatten(retry)]
    assert "SUPPLY" not in retry_types, (
        f"{case.name}: re-supplied collateral after BORROW failure (got {retry_types}) "
        f"— this is the double-supply money-loss bug the migration must prevent"
    )
    assert "BORROW" in retry_types, (
        f"{case.name}: retry after BORROW failure should re-emit BORROW, got {retry_types}"
    )
    assert case.supplied(strat) == supplied_after_supply, (
        f"{case.name}: collateral re-supplied on retry "
        f"({supplied_after_supply} -> {case.supplied(strat)})"
    )


def test_all_eight_migrated_strategies_are_covered() -> None:
    """Guard against silent drift: exactly the 8 PR-#2827 strategies are exercised."""
    expected = {
        "aave_enso_carry_polygon",
        "aave_v3_pancakeswap_teardown_bsc",
        "aave_v3_sushiswap_carry_bsc",
        "compound_v3_aerodrome_yield_farm_base",
        "compound_v3_uniswap_v3_teardown_arbitrum",
        "compound_v3_weth_market_arbitrum",
        "morpho_blue_enso_lifecycle_base",
        "morpho_uniswap_yield_stack_arbitrum",
    }
    assert {c.name for c in _CASES} == expected
