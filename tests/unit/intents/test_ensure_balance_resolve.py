"""Branch coverage for EnsureBalanceIntent.resolve.

Covers the sufficient-target hold path, source-chain selection (highest
balance wins, target chain skipped case-insensitively, insufficient chains
ignored), bridge-intent construction with propagated slippage / preferred
bridge, and the InsufficientBalanceError diagnostics. Pure in-memory
resolution — no chain access.
"""

from decimal import Decimal

import pytest

from almanak.framework.intents.bridge import BridgeIntent
from almanak.framework.intents.ensure_balance import (
    EnsureBalanceIntent,
    InsufficientBalanceError,
)
from almanak.framework.intents.vocabulary import HoldIntent


def _intent(**overrides):
    fields = {
        "token": "USDC",
        "min_amount": Decimal("100"),
        "target_chain": "arbitrum",
    }
    fields.update(overrides)
    return EnsureBalanceIntent(**fields)


class TestResolveHold:
    def test_sufficient_target_balance_holds(self):
        resolved = _intent().resolve(Decimal("150"), {"base": Decimal("500")})
        assert isinstance(resolved, HoldIntent)
        assert "Sufficient USDC balance on arbitrum" in resolved.reason

    def test_exact_balance_holds(self):
        resolved = _intent().resolve(Decimal("100"), {})
        assert isinstance(resolved, HoldIntent)


class TestResolveBridge:
    def test_bridges_shortfall_from_source(self):
        intent = _intent(max_slippage=Decimal("0.01"), preferred_bridge="across")
        resolved = intent.resolve(Decimal("40"), {"base": Decimal("80")})

        assert isinstance(resolved, BridgeIntent)
        assert resolved.token == "USDC"
        assert resolved.amount == Decimal("60")
        assert resolved.from_chain == "base"
        assert resolved.to_chain == "arbitrum"
        assert resolved.max_slippage == Decimal("0.01")
        assert resolved.preferred_bridge == "across"

    def test_prefers_highest_balance_source(self):
        resolved = _intent().resolve(
            Decimal("40"),
            {"base": Decimal("70"), "optimism": Decimal("90")},
        )
        assert resolved.from_chain == "optimism"

    def test_skips_target_chain_case_insensitively(self):
        resolved = _intent().resolve(
            Decimal("40"),
            {"ARBITRUM": Decimal("500"), "base": Decimal("80")},
        )
        assert resolved.from_chain == "base"

    def test_ignores_chains_below_shortfall(self):
        # base cannot cover the 60 shortfall alone; optimism can.
        resolved = _intent().resolve(
            Decimal("40"),
            {"base": Decimal("59"), "optimism": Decimal("61")},
        )
        assert resolved.from_chain == "optimism"


class TestResolveInsufficient:
    def test_no_single_chain_sufficient_raises(self):
        with pytest.raises(InsufficientBalanceError) as excinfo:
            _intent().resolve(Decimal("40"), {"base": Decimal("10"), "optimism": Decimal("20")})

        err = excinfo.value
        assert err.token == "USDC"
        assert err.min_amount == Decimal("100")
        assert err.target_chain == "arbitrum"
        # The error surfaces every balance including the target chain's.
        assert err.available_balances == {
            "base": Decimal("10"),
            "optimism": Decimal("20"),
            "arbitrum": Decimal("40"),
        }
        assert "base: 10" in str(err)

    def test_no_chains_at_all_raises_with_target_balance(self):
        with pytest.raises(InsufficientBalanceError) as excinfo:
            _intent().resolve(Decimal("40"), {})
        assert excinfo.value.available_balances == {"arbitrum": Decimal("40")}
