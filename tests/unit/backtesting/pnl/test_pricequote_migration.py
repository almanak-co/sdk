"""ALM-2943: typed PriceQuote migration pins for USD↔token-unit conversion.

Every migrated site must satisfy three invariants:

1. Healthy data is a no-op — a positive market price converts exactly as the
   pre-migration ``amount_usd / price`` (and ``units * price``).
2. Absence raises — a missing or non-positive price for a non-cash token
   raises :class:`PriceUnavailableError` instead of booking USD figures as
   unit counts (implicit $1) or silently skipping the leg.
3. The $1 cash plane is doctrine, not a fallback bug — cash-equivalent
   stables (USDC/USDT/DAI, see ``CASH_EQUIVALENT_STABLECOIN_SYMBOLS``)
   without a market quote resolve at units == USD, mirroring how the
   portfolio holds them inside ``cash_usd`` at face value (#3318). A present
   market price always wins over the plane (depeg: 0.98 is used, never
   silently re-pinned to $1 — pre-migration behavior preserved).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from almanak.framework.backtesting.models import IntentType
from almanak.framework.backtesting.pnl import _engine_helpers
from almanak.framework.backtesting.pnl.data_provider import MarketState
from almanak.framework.backtesting.pnl.engine import (
    DefaultFeeModel,
    DefaultSlippageModel,
    PnLBacktester,
)
from almanak.framework.market.errors import PriceUnavailableError


class _EmptyDataProvider:
    provider_name = "mock_empty"

    async def iterate(self, config: Any):  # pragma: no cover - never yields
        if False:
            yield


def _backtester() -> PnLBacktester:
    return PnLBacktester(
        data_provider=_EmptyDataProvider(),
        fee_models={"default": DefaultFeeModel()},
        slippage_models={"default": DefaultSlippageModel()},
    )


def _market_state(prices: dict[str, Decimal] | None = None) -> MarketState:
    return MarketState(
        timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        prices=prices
        if prices is not None
        else {
            "WETH": Decimal("3000"),
            "USDC": Decimal("1"),
            # Non-cash stable-ish token trading at a depeg — must be valued
            # at its market price, never re-pinned to $1.
            "SDAI": Decimal("0.98"),
            # Present-but-zero price: a data defect that must read as absence.
            "ZERO": Decimal("0"),
        },
    )


def _flows(
    engine: PnLBacktester,
    intent: Any,
    intent_type: IntentType,
    market_state: MarketState,
    amount_usd: Decimal = Decimal("500"),
) -> tuple[dict, dict]:
    return engine._calculate_token_flows(
        intent=intent,
        intent_type=intent_type,
        amount_usd=amount_usd,
        executed_price=Decimal("1"),
        fee_usd=Decimal("0"),
        slippage_usd=Decimal("0"),
        market_state=market_state,
    )


@dataclass
class _SingleTokenIntent:
    token: str


@dataclass
class _VaultIntent:
    deposit_token: str


@dataclass
class _LPIntent:
    token0: str
    token1: str


@dataclass
class _SwapIntent:
    from_token: str
    to_token: str


# Every simple-flow lane and the token attribute it spends/receives through.
_SIMPLE_LANES = [
    (IntentType.SUPPLY, "tokens_out"),
    (IntentType.WITHDRAW, "tokens_in"),
    (IntentType.BORROW, "tokens_in"),
    (IntentType.REPAY, "tokens_out"),
]


class TestFlowLaneHealthyDataNoOp:
    """A positive market price converts exactly as before the migration."""

    @pytest.mark.parametrize(("intent_type", "leg"), _SIMPLE_LANES)
    def test_simple_flows_use_market_price(self, intent_type: IntentType, leg: str):
        engine = _backtester()
        tokens_in, tokens_out = _flows(engine, _SingleTokenIntent(token="WETH"), intent_type, _market_state())
        target = tokens_out if leg == "tokens_out" else tokens_in
        assert target == {"WETH": Decimal("500") / Decimal("3000")}

    def test_swap_uses_market_prices(self):
        engine = _backtester()
        tokens_in, tokens_out = _flows(
            engine, _SwapIntent(from_token="USDC", to_token="WETH"), IntentType.SWAP, _market_state()
        )
        assert tokens_out == {"USDC": Decimal("500")}
        assert tokens_in == {"WETH": Decimal("500") / Decimal("3000")}

    def test_depeg_market_price_wins_for_non_cash(self):
        """A non-cash stable at 0.98 is valued at 0.98 — never at $1."""
        engine = _backtester()
        _, tokens_out = _flows(engine, _SingleTokenIntent(token="SDAI"), IntentType.SUPPLY, _market_state())
        assert tokens_out == {"SDAI": Decimal("500") / Decimal("0.98")}

    def test_depeg_present_price_wins_even_for_cash_equivalent(self):
        """The $1 plane only fills ABSENCE: a present USDC quote (even 0.98)
        converts at market, exactly as pre-migration ``amount_usd / price``.
        The cash plane governs how held stables are swept into ``cash_usd``,
        not how a live market quote is consumed."""
        engine = _backtester()
        _, tokens_out = _flows(
            engine,
            _SingleTokenIntent(token="USDC"),
            IntentType.SUPPLY,
            _market_state({"USDC": Decimal("0.98")}),
        )
        assert tokens_out == {"USDC": Decimal("500") / Decimal("0.98")}


class TestFlowLaneAbsenceRaises:
    """Missing/non-positive price for a non-cash token raises, never $1/skip."""

    @pytest.mark.parametrize(("intent_type", "_leg"), _SIMPLE_LANES)
    def test_simple_flows_missing_price_raises(self, intent_type: IntentType, _leg: str):
        engine = _backtester()
        with pytest.raises(PriceUnavailableError):
            _flows(engine, _SingleTokenIntent(token="XYZ"), intent_type, _market_state())

    @pytest.mark.parametrize(("intent_type", "_leg"), _SIMPLE_LANES)
    def test_simple_flows_zero_price_raises(self, intent_type: IntentType, _leg: str):
        """A present-but-zero price is a data defect and reads as absence —
        the pre-migration code silently recorded NO flow for this leg."""
        engine = _backtester()
        with pytest.raises(PriceUnavailableError):
            _flows(engine, _SingleTokenIntent(token="ZERO"), intent_type, _market_state())

    def test_lp_open_missing_price_raises(self):
        engine = _backtester()
        with pytest.raises(PriceUnavailableError):
            _flows(engine, _LPIntent(token0="XYZ", token1="USDC"), IntentType.LP_OPEN, _market_state())

    def test_lp_close_missing_price_raises(self):
        engine = _backtester()
        with pytest.raises(PriceUnavailableError):
            _flows(engine, _LPIntent(token0="WETH", token1="XYZ"), IntentType.LP_CLOSE, _market_state())

    @pytest.mark.parametrize("intent_type", [IntentType.VAULT_DEPOSIT, IntentType.VAULT_REDEEM])
    def test_vault_missing_price_raises(self, intent_type: IntentType):
        engine = _backtester()
        with pytest.raises(PriceUnavailableError):
            _flows(engine, _VaultIntent(deposit_token="XYZ"), intent_type, _market_state())


class TestFlowLaneCashPlane:
    """Cash-equivalent stables without a quote stay on the $1 plane (#3318)."""

    @pytest.mark.parametrize(("intent_type", "leg"), _SIMPLE_LANES)
    def test_simple_flows_cash_equivalent_absent_price(self, intent_type: IntentType, leg: str):
        engine = _backtester()
        state = _market_state({"WETH": Decimal("3000")})  # no USDT quote
        tokens_in, tokens_out = _flows(engine, _SingleTokenIntent(token="USDT"), intent_type, state)
        target = tokens_out if leg == "tokens_out" else tokens_in
        assert target == {"USDT": Decimal("500")}

    def test_swap_cash_equivalent_legs_absent_price(self):
        engine = _backtester()
        state = _market_state({"WETH": Decimal("3000")})  # no DAI quote
        tokens_in, tokens_out = _flows(engine, _SwapIntent(from_token="DAI", to_token="WETH"), IntentType.SWAP, state)
        assert tokens_out == {"DAI": Decimal("500")}
        assert tokens_in == {"WETH": Decimal("500") / Decimal("3000")}

    def test_lp_open_cash_equivalent_token1_absent_price(self):
        engine = _backtester()
        state = _market_state({"WETH": Decimal("3000")})
        _, tokens_out = _flows(engine, _LPIntent(token0="WETH", token1="USDC"), IntentType.LP_OPEN, state)
        assert tokens_out == {
            "WETH": Decimal("250") / Decimal("3000"),
            "USDC": Decimal("250"),
        }

    @pytest.mark.parametrize("intent_type", [IntentType.VAULT_DEPOSIT, IntentType.VAULT_REDEEM])
    def test_vault_cash_equivalent_absent_price(self, intent_type: IntentType):
        engine = _backtester()
        state = _market_state({"WETH": Decimal("3000")})
        tokens_in, tokens_out = _flows(engine, _VaultIntent(deposit_token="USDC"), intent_type, state)
        target = tokens_out if intent_type is IntentType.VAULT_DEPOSIT else tokens_in
        assert target == {"USDC": Decimal("500")}


class TestPositionDeltaTypedConversion:
    """Engine position-delta builders route USD→units through PriceQuote."""

    def _delta(self, engine: PnLBacktester, intent_type: IntentType, token: str, state: MarketState):
        from types import SimpleNamespace

        attribute = "deposit_token" if intent_type is IntentType.VAULT_DEPOSIT else "token"
        intent = SimpleNamespace(intent_type=intent_type, **{attribute: token})
        return engine._create_position_delta(
            intent=intent,
            intent_type=intent_type,
            protocol="aave_v3",
            tokens=[token],
            executed_price=Decimal("1"),
            timestamp=state.timestamp,
            market_state=state,
            amount_usd=Decimal("600"),
        )

    @pytest.mark.parametrize("intent_type", [IntentType.SUPPLY, IntentType.BORROW, IntentType.VAULT_DEPOSIT])
    def test_healthy_price_is_noop(self, intent_type: IntentType):
        engine = _backtester()
        position = self._delta(engine, intent_type, "WETH", _market_state())
        assert position is not None
        assert position.amounts == {"WETH": Decimal("600") / Decimal("3000")}

    @pytest.mark.parametrize("intent_type", [IntentType.SUPPLY, IntentType.BORROW, IntentType.VAULT_DEPOSIT])
    def test_missing_price_raises(self, intent_type: IntentType):
        engine = _backtester()
        with pytest.raises(PriceUnavailableError):
            self._delta(engine, intent_type, "XYZ", _market_state())

    @pytest.mark.parametrize("intent_type", [IntentType.SUPPLY, IntentType.BORROW, IntentType.VAULT_DEPOSIT])
    def test_cash_equivalent_absent_price_stays_on_plane(self, intent_type: IntentType):
        engine = _backtester()
        position = self._delta(engine, intent_type, "USDC", _market_state({"WETH": Decimal("3000")}))
        assert position is not None
        assert position.amounts == {"USDC": Decimal("600")}


class TestLendingBorrowHealthTypedConversion:
    """The borrow health check prices debt through a typed PriceQuote."""

    @staticmethod
    def _adapter_and_state(prices: dict[str, Decimal]):
        from almanak.framework.backtesting.adapters.lending_adapter import (
            LendingBacktestAdapter,
            LendingBacktestConfig,
        )
        from almanak.framework.backtesting.pnl.portfolio import SimulatedPortfolio

        @dataclass
        class _State:
            prices: dict[str, Decimal] = field(default_factory=dict)
            timestamp: datetime = field(default_factory=lambda: datetime(2024, 1, 1, tzinfo=UTC))

            def get_price(self, token: str) -> Decimal:
                if token not in self.prices:
                    raise KeyError(token)
                return self.prices[token]

        adapter = LendingBacktestAdapter(LendingBacktestConfig(strategy_type="lending", protocol="aave_v3"))
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("0"))
        return adapter, portfolio, _State(prices=prices)

    @staticmethod
    def _borrow_intent(borrow_token: str, borrow_amount: Decimal):
        from almanak.framework.intents.vocabulary import BorrowIntent

        return BorrowIntent(
            protocol="aave_v3",
            collateral_token="WETH",
            collateral_amount=Decimal("0"),
            borrow_token=borrow_token,
            borrow_amount=borrow_amount,
        )

    def test_missing_price_non_cash_raises(self):
        """Pre-migration: an unpriced LINK borrow was valued at $1 per unit,
        understating debt by the full token price. Now it refuses."""
        adapter, portfolio, state = self._adapter_and_state({"WETH": Decimal("2000")})
        with pytest.raises(PriceUnavailableError):
            adapter._execute_borrow(self._borrow_intent("LINK", Decimal("100")), portfolio, state)

    def test_cash_equivalent_absent_price_stays_on_plane(self):
        """DAI without a quote keeps its deliberate $1 debt valuation."""
        adapter, portfolio, state = self._adapter_and_state({"WETH": Decimal("2000")})
        fill = adapter._execute_borrow(self._borrow_intent("DAI", Decimal("20000")), portfolio, state)
        # No collateral at all: the health check fires with debt valued at
        # exactly $20,000 (units == USD on the cash plane).
        assert fill is not None
        assert fill.success is False
        assert fill.amount_usd == Decimal("20000")

    def test_market_price_used_when_present(self):
        """A depegged DAI quote (0.98) prices the debt at market, not $1."""
        adapter, portfolio, state = self._adapter_and_state({"WETH": Decimal("2000"), "DAI": Decimal("0.98")})
        fill = adapter._execute_borrow(self._borrow_intent("DAI", Decimal("20000")), portfolio, state)
        assert fill is not None
        assert fill.success is False
        assert fill.amount_usd == Decimal("20000") * Decimal("0.98")


class TestTypedConversionHelpers:
    """Direct pins on the shared conversion seam."""

    def test_units_from_usd_healthy(self):
        units = _engine_helpers.typed_units_from_usd(
            "WETH", Decimal("3000"), Decimal("600"), chain="ethereum", token_addresses=None, context="pin"
        )
        assert units == Decimal("0.2")

    def test_units_from_usd_absent_raises(self):
        with pytest.raises(PriceUnavailableError):
            _engine_helpers.typed_units_from_usd(
                "XYZ", None, Decimal("600"), chain="ethereum", token_addresses=None, context="pin"
            )

    def test_usd_from_units_absent_raises(self):
        with pytest.raises(PriceUnavailableError):
            _engine_helpers.typed_usd_from_units(
                "XYZ", None, Decimal("2"), chain="ethereum", token_addresses=None, context="pin"
            )

    def test_address_keyed_cash_equivalent_resolves_via_registered_map(self):
        """An address-native USDC key still lands on the $1 cash plane when
        the registered map identifies it as USDC."""
        usdc = ("base", "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913")
        units = _engine_helpers.typed_units_from_usd(
            usdc,
            None,
            Decimal("500"),
            chain="base",
            token_addresses={"USDC": usdc},
            context="pin",
        )
        assert units == Decimal("500")

    def test_address_keyed_unregistered_absent_raises(self):
        with pytest.raises(PriceUnavailableError):
            _engine_helpers.typed_units_from_usd(
                ("base", "0x4200000000000000000000000000000000000006"),
                None,
                Decimal("500"),
                chain="base",
                token_addresses=None,
                context="pin",
            )
