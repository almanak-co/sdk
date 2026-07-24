"""Branch coverage for plan_builder step descriptions.

Covers every intent-type arm of ``get_step_description`` plus the
``get_intent_chain`` default. Pure formatting — stub intents carry only
the attributes each arm reads.
"""

from types import SimpleNamespace

import pytest

from almanak.framework.execution.plan_builder import (
    get_intent_chain,
    get_step_description,
)
from almanak.framework.intents.vocabulary import IntentType


def _intent(intent_type, **attrs):
    return SimpleNamespace(intent_type=intent_type, **attrs)


class TestGetIntentChain:
    def test_reads_chain_attribute(self):
        assert get_intent_chain(_intent(IntentType.SWAP, chain="base")) == "base"

    def test_falls_back_to_default(self):
        assert get_intent_chain(_intent(IntentType.SWAP), default_chain="optimism") == "optimism"

    def test_none_chain_falls_back(self):
        assert get_intent_chain(_intent(IntentType.SWAP, chain=None), "arbitrum") == "arbitrum"


class TestGetStepDescription:
    def test_same_chain_swap(self):
        intent = _intent(
            IntentType.SWAP,
            chain="base",
            from_token="USDC",
            to_token="WETH",
            protocol="uniswap_v3",
        )
        assert get_step_description(intent) == "Swap USDC → WETH on base via uniswap_v3"

    def test_cross_chain_swap(self):
        intent = _intent(
            IntentType.SWAP,
            chain="base",
            from_token="USDC",
            to_token="WETH",
            destination_chain="arbitrum",
            protocol="enso",
        )
        assert (
            get_step_description(intent)
            == "Cross-chain swap USDC (base) → WETH (arbitrum) via enso"
        )

    def test_swap_with_matching_destination_is_not_cross_chain(self):
        intent = _intent(
            IntentType.SWAP,
            chain="base",
            from_token="USDC",
            to_token="WETH",
            destination_chain="base",
            protocol="uniswap_v3",
        )
        assert get_step_description(intent).startswith("Swap ")

    def test_swap_missing_tokens_uses_placeholders(self):
        intent = _intent(IntentType.SWAP, chain="base", protocol="uniswap_v3")
        assert get_step_description(intent) == "Swap ? → ? on base via uniswap_v3"

    def test_supply(self):
        intent = _intent(IntentType.SUPPLY, chain="polygon", token="USDC", protocol="aave_v3")
        assert get_step_description(intent) == "Supply USDC to aave_v3 on polygon"

    def test_borrow(self):
        intent = _intent(
            IntentType.BORROW, chain="polygon", borrow_token="WETH", protocol="aave_v3"
        )
        assert get_step_description(intent) == "Borrow WETH from aave_v3 on polygon"

    @pytest.mark.parametrize(
        ("is_long", "direction"), [(True, "long"), (False, "short")]
    )
    def test_perp_open(self, is_long, direction):
        intent = _intent(
            IntentType.PERP_OPEN,
            chain="arbitrum",
            market="ETH-PERP",
            is_long=is_long,
            protocol="gmx_v2",
        )
        assert get_step_description(intent) == f"Open {direction} ETH-PERP on gmx_v2 (arbitrum)"

    def test_hold_with_reason(self):
        intent = _intent(IntentType.HOLD, chain="base", reason="rsi neutral")
        assert get_step_description(intent) == "Hold: rsi neutral"

    def test_hold_without_reason(self):
        intent = _intent(IntentType.HOLD, chain="base", reason="")
        assert get_step_description(intent) == "Hold position"

    def test_fallback_for_other_intent_types(self):
        intent = _intent(IntentType.BRIDGE, chain="base")
        assert get_step_description(intent) == "BRIDGE on base"
