"""Branch coverage for PancakeSwapV3FeeModel fee-tier resolution.

_resolve_fee_tier has a four-level priority chain (explicit fee_tier ->
fee_tier_bps -> token_pair_tiers -> default). Every branch is exercised
here, both directly and through calculate_fee, which also owns the
intent-type gating (only SWAP pays fees). Pure computation — no network.
"""

from decimal import Decimal

import pytest

from almanak.connectors.pancakeswap_v3.fee_model import (
    PANCAKESWAP_FEE_TIER_MAP,
    PancakeSwapV3FeeModel,
    PancakeSwapV3FeeTier,
)
from almanak.framework.backtesting.models import IntentType


@pytest.fixture
def model():
    return PancakeSwapV3FeeModel()


@pytest.fixture
def pair_model():
    return PancakeSwapV3FeeModel(
        token_pair_tiers={("USDC", "USDT"): PancakeSwapV3FeeTier.LOWEST},
    )


class TestExplicitFeeTier:
    def test_enum_instance_returned_directly(self, model):
        assert model._resolve_fee_tier(fee_tier=PancakeSwapV3FeeTier.HIGH) is PancakeSwapV3FeeTier.HIGH

    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("100", PancakeSwapV3FeeTier.LOWEST),
            ("500", PancakeSwapV3FeeTier.LOW),
            ("2500", PancakeSwapV3FeeTier.MEDIUM),
            ("10000", PancakeSwapV3FeeTier.HIGH),
        ],
    )
    def test_string_enum_value_matched(self, model, raw, expected):
        assert model._resolve_fee_tier(fee_tier=raw) is expected

    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("lowest", PancakeSwapV3FeeTier.LOWEST),
            ("LOW", PancakeSwapV3FeeTier.LOW),
            ("Medium", PancakeSwapV3FeeTier.MEDIUM),
            ("high", PancakeSwapV3FeeTier.HIGH),
        ],
    )
    def test_string_name_matched_case_insensitive(self, model, raw, expected):
        assert model._resolve_fee_tier(fee_tier=raw) is expected

    def test_unmatched_string_falls_through_to_default(self, model):
        assert model._resolve_fee_tier(fee_tier="3000") is PancakeSwapV3FeeTier.MEDIUM

    def test_non_enum_non_string_falls_through_to_default(self, model):
        # An int fee_tier is neither the enum nor a string; it is ignored.
        assert model._resolve_fee_tier(fee_tier=500) is PancakeSwapV3FeeTier.MEDIUM

    def test_unmatched_string_falls_through_to_bps(self, model):
        tier = model._resolve_fee_tier(fee_tier="not-a-tier", fee_tier_bps=100)
        assert tier is PancakeSwapV3FeeTier.LOWEST

    def test_explicit_tier_beats_bps_and_pair(self, pair_model):
        tier = pair_model._resolve_fee_tier(
            fee_tier=PancakeSwapV3FeeTier.HIGH,
            fee_tier_bps=500,
            token_in="USDC",
            token_out="USDT",
        )
        assert tier is PancakeSwapV3FeeTier.HIGH


class TestFeeTierBps:
    @pytest.mark.parametrize(("bps", "expected"), list(PANCAKESWAP_FEE_TIER_MAP.items()))
    def test_known_bps_mapped(self, model, bps, expected):
        assert model._resolve_fee_tier(fee_tier_bps=bps) is expected

    def test_bps_accepts_numeric_string(self, model):
        assert model._resolve_fee_tier(fee_tier_bps="10000") is PancakeSwapV3FeeTier.HIGH

    def test_unknown_bps_falls_through_to_default(self, model):
        # 3000 is Uniswap's tier, not PancakeSwap's.
        assert model._resolve_fee_tier(fee_tier_bps=3000) is PancakeSwapV3FeeTier.MEDIUM

    def test_unknown_bps_falls_through_to_pair_lookup(self, pair_model):
        tier = pair_model._resolve_fee_tier(fee_tier_bps=3000, token_in="USDC", token_out="USDT")
        assert tier is PancakeSwapV3FeeTier.LOWEST

    def test_bps_beats_pair_lookup(self, pair_model):
        tier = pair_model._resolve_fee_tier(fee_tier_bps=10000, token_in="USDC", token_out="USDT")
        assert tier is PancakeSwapV3FeeTier.HIGH


class TestTokenPairLookup:
    def test_direct_ordering_matched(self, pair_model):
        tier = pair_model._resolve_fee_tier(token_in="USDC", token_out="USDT")
        assert tier is PancakeSwapV3FeeTier.LOWEST

    def test_reverse_ordering_matched(self, pair_model):
        tier = pair_model._resolve_fee_tier(token_in="USDT", token_out="USDC")
        assert tier is PancakeSwapV3FeeTier.LOWEST

    def test_lowercase_tokens_uppercased_before_lookup(self, pair_model):
        tier = pair_model._resolve_fee_tier(token_in="usdc", token_out="usdt")
        assert tier is PancakeSwapV3FeeTier.LOWEST

    def test_unknown_pair_falls_through_to_default(self, pair_model):
        tier = pair_model._resolve_fee_tier(token_in="WBNB", token_out="CAKE")
        assert tier is PancakeSwapV3FeeTier.MEDIUM

    def test_pair_lookup_skipped_without_mapping(self, model):
        # token_pair_tiers is None on the default model.
        tier = model._resolve_fee_tier(token_in="USDC", token_out="USDT")
        assert tier is PancakeSwapV3FeeTier.MEDIUM

    def test_pair_lookup_skipped_when_token_out_missing(self, pair_model):
        assert pair_model._resolve_fee_tier(token_in="USDC") is PancakeSwapV3FeeTier.MEDIUM

    def test_pair_lookup_skipped_when_token_in_missing(self, pair_model):
        assert pair_model._resolve_fee_tier(token_out="USDT") is PancakeSwapV3FeeTier.MEDIUM


class TestNoHints:
    def test_no_kwargs_returns_default(self, model):
        assert model._resolve_fee_tier() is PancakeSwapV3FeeTier.MEDIUM

    def test_custom_default_respected(self):
        model = PancakeSwapV3FeeModel(default_fee_tier=PancakeSwapV3FeeTier.LOW)
        assert model._resolve_fee_tier() is PancakeSwapV3FeeTier.LOW


class TestCalculateFee:
    @pytest.mark.parametrize("intent_type", [IntentType.LP_OPEN, IntentType.LP_CLOSE])
    def test_lp_operations_are_free(self, model, intent_type):
        assert model.calculate_fee(Decimal("1000"), intent_type=intent_type) == Decimal("0")

    @pytest.mark.parametrize("intent_type", [IntentType.BORROW, IntentType.BRIDGE])
    def test_non_swap_intents_are_free(self, model, intent_type):
        assert model.calculate_fee(Decimal("1000"), intent_type=intent_type) == Decimal("0")

    def test_default_intent_type_is_swap(self, model):
        # 0.25% of 1000 via the default MEDIUM tier.
        assert model.calculate_fee(Decimal("1000")) == Decimal("2.5")

    def test_swap_with_explicit_tier(self, model):
        fee = model.calculate_fee(
            Decimal("1000"),
            intent_type=IntentType.SWAP,
            fee_tier=PancakeSwapV3FeeTier.LOW,
        )
        assert fee == Decimal("0.5")

    def test_swap_with_pair_tier(self, pair_model):
        fee = pair_model.calculate_fee(
            Decimal("10000"),
            intent_type=IntentType.SWAP,
            token_in="USDT",
            token_out="USDC",
        )
        # LOWEST tier: 0.01% of 10000.
        assert fee == Decimal("1")
