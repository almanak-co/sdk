"""Tests for MarketSnapshot gas-worthiness gate (min_trade_value / max_gas_ratio).

Covers:
    - estimate_swap_gas_cost_usd scaling from 21k baseline to chain-aware swap gas
    - is_trade_worthwhile ratio comparison (cheap L2 vs expensive L1)
    - fail-open behavior when the gas oracle is missing / returns zero cost
"""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

from almanak.framework.data.defi.gas import STANDARD_GAS_UNITS, GasPrice
from almanak.framework.market import MarketSnapshot
from almanak.framework.intents.compiler_constants import get_gas_estimate


def _make_gas_oracle(cost_usd: Decimal, chain: str = "arbitrum") -> MagicMock:
    """Build a mock gas oracle that returns a GasPrice with ``cost_usd``
    as ``estimated_cost_usd`` (i.e. the 21000-gas baseline cost).
    """
    gp = GasPrice(
        chain=chain,
        base_fee_gwei=Decimal("0.1"),
        priority_fee_gwei=Decimal("0.0"),
        max_fee_gwei=Decimal("0.1"),
        estimated_cost_usd=cost_usd,
        timestamp=datetime.now(UTC),
    )
    oracle = MagicMock()
    oracle.get_gas_price = AsyncMock(return_value=gp)
    return oracle


class TestEstimateSwapGasCostUsd:
    """estimate_swap_gas_cost_usd() scales 21k baseline to swap gas."""

    def test_scales_to_swap_simple_gas(self) -> None:
        """Returned cost = baseline * (swap_simple_gas / 21000) on arbitrum."""
        # Baseline = $0.01 for 21000 gas on arbitrum
        oracle = _make_gas_oracle(Decimal("0.01"), chain="arbitrum")
        snapshot = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0xtest",
            gas_oracle=oracle,
        )

        cost = snapshot.estimate_swap_gas_cost_usd("arbitrum")

        expected_scale = Decimal(get_gas_estimate("arbitrum", "swap_simple")) / Decimal(STANDARD_GAS_UNITS)
        expected = (Decimal("0.01") * expected_scale).quantize(Decimal("0.0001"))
        assert cost == expected
        assert cost > Decimal("0")

    def test_returns_zero_when_oracle_missing(self) -> None:
        """No oracle configured -> Decimal(0) (opt-in gate must not crash)."""
        snapshot = MarketSnapshot(chain="arbitrum", wallet_address="0xtest")
        assert snapshot.estimate_swap_gas_cost_usd("arbitrum") == Decimal("0")

    def test_returns_zero_when_underlying_cost_zero(self) -> None:
        """If the gas oracle can't price (no price oracle) -> Decimal(0)."""
        oracle = _make_gas_oracle(Decimal("0"), chain="ethereum")
        snapshot = MarketSnapshot(
            chain="ethereum",
            wallet_address="0xtest",
            gas_oracle=oracle,
        )
        assert snapshot.estimate_swap_gas_cost_usd("ethereum") == Decimal("0")

    def test_ethereum_costs_more_than_arbitrum_for_same_baseline(self) -> None:
        """Sanity check: given the same 21k cost, swap-gas scaling should be
        comparable on both chains (the difference shows up in live gas prices,
        not the static scale) — but the returned value must always be > 0.
        """
        oracle_l1 = _make_gas_oracle(Decimal("0.50"), chain="ethereum")  # $0.50 for 21k on L1
        oracle_l2 = _make_gas_oracle(Decimal("0.01"), chain="arbitrum")  # $0.01 for 21k on L2

        snap_l1 = MarketSnapshot(chain="ethereum", wallet_address="0xtest", gas_oracle=oracle_l1)
        snap_l2 = MarketSnapshot(chain="arbitrum", wallet_address="0xtest", gas_oracle=oracle_l2)

        cost_l1 = snap_l1.estimate_swap_gas_cost_usd()
        cost_l2 = snap_l2.estimate_swap_gas_cost_usd()

        assert cost_l1 > cost_l2
        assert cost_l2 > Decimal("0")


class TestIsTradeWorthwhile:
    """is_trade_worthwhile() ratio gate."""

    def test_cheap_l2_small_trade_returns_true(self) -> None:
        """Arbitrum gas is ~cents; a $100 trade is easily above 5% ratio floor."""
        oracle = _make_gas_oracle(Decimal("0.01"), chain="arbitrum")
        snapshot = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0xtest",
            gas_oracle=oracle,
        )
        assert snapshot.is_trade_worthwhile(Decimal("100"), "arbitrum") is True

    def test_expensive_l1_small_trade_returns_false(self) -> None:
        """High L1 gas ($5 baseline = ~$50 for a swap) makes $100 trade
        a clear gas-drain (50% > 5%)."""
        oracle = _make_gas_oracle(Decimal("5.00"), chain="ethereum")
        snapshot = MarketSnapshot(
            chain="ethereum",
            wallet_address="0xtest",
            gas_oracle=oracle,
        )
        assert snapshot.is_trade_worthwhile(Decimal("100"), "ethereum") is False

    def test_expensive_l1_large_trade_returns_true(self) -> None:
        """A $100k trade on L1 comfortably clears the 5% ratio even with $50 swap gas."""
        oracle = _make_gas_oracle(Decimal("5.00"), chain="ethereum")
        snapshot = MarketSnapshot(
            chain="ethereum",
            wallet_address="0xtest",
            gas_oracle=oracle,
        )
        assert snapshot.is_trade_worthwhile(Decimal("100000"), "ethereum") is True

    def test_custom_max_gas_ratio_tightens_gate(self) -> None:
        """A trade that passes at 5% can be rejected at 0.1%."""
        # arbitrum: baseline $0.01 -> ~$0.09 per swap; $10 trade => ratio ~0.9%
        oracle = _make_gas_oracle(Decimal("0.01"), chain="arbitrum")
        snapshot = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0xtest",
            gas_oracle=oracle,
        )
        assert snapshot.is_trade_worthwhile(Decimal("10"), "arbitrum", Decimal("0.05")) is True
        assert snapshot.is_trade_worthwhile(Decimal("10"), "arbitrum", Decimal("0.001")) is False

    def test_zero_or_negative_amount_returns_false(self) -> None:
        snapshot = MarketSnapshot(chain="arbitrum", wallet_address="0xtest")
        assert snapshot.is_trade_worthwhile(Decimal("0"), "arbitrum") is False
        assert snapshot.is_trade_worthwhile(Decimal("-1"), "arbitrum") is False

    def test_missing_oracle_fails_open(self) -> None:
        """When gas cost can't be estimated, we don't block the trade —
        callers wanting fail-closed behaviour must enforce locally."""
        snapshot = MarketSnapshot(chain="arbitrum", wallet_address="0xtest")
        assert snapshot.is_trade_worthwhile(Decimal("100"), "arbitrum") is True

    def test_gas_unavailable_error_fails_open(self) -> None:
        """If the oracle is configured but fails (e.g., transient RPC error),
        ``estimate_swap_gas_cost_usd`` raises ``GasUnavailableError``.
        ``is_trade_worthwhile`` must swallow that and fail-open to match its
        docstring contract."""
        from almanak.framework.market import GasUnavailableError

        oracle = MagicMock()
        oracle.get_gas_price = AsyncMock(side_effect=GasUnavailableError("arbitrum", "rpc timeout"))
        snapshot = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0xtest",
            gas_oracle=oracle,
        )

        assert snapshot.is_trade_worthwhile(Decimal("100"), "arbitrum") is True

    def test_zero_max_gas_ratio_returns_false(self) -> None:
        """A zero/negative max_gas_ratio means 'never worthwhile'."""
        oracle = _make_gas_oracle(Decimal("0.01"), chain="arbitrum")
        snapshot = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0xtest",
            gas_oracle=oracle,
        )
        assert snapshot.is_trade_worthwhile(Decimal("100"), "arbitrum", Decimal("0")) is False
