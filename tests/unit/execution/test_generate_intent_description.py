"""Characterization tests for ``_generate_intent_description`` (Phase 7.1a).

These tests pin the exact output strings produced by
``_generate_intent_description`` today so that a subsequent phase-extraction
refactor can be verified against the observable behaviour.

Callers (UI / timeline / operator cards / logs) may match on these strings,
so every test asserts the full string, not substrings.

Intent type coverage:
    - SWAP (full / partial / protocol / missing metadata)
    - SUPPLY, BORROW, REPAY, WITHDRAW (full / missing metadata)
    - LP_OPEN, LP_CLOSE (tokens / pool fallback / bare)
    - PERP_OPEN, PERP_CLOSE (direction casing, leverage, market fallback)
    - BRIDGE (chain pair / to-only / missing)
    - HOLD (with reason / bare)
    - Unknown / lowercase / mixed-case intent_type (default branch)

Token-data shape coverage:
    - dict with ``symbol``
    - dict with only ``name`` (symbol absent)
    - bare string
    - None / missing / empty dict

Amount formatting coverage:
    - Wei-scale ints (with and without decimals metadata)
    - Sub-unit decimals (trailing-zero trimming)
    - Zero / empty / malformed
"""

from __future__ import annotations

import pytest

from almanak.framework.execution.orchestrator import _generate_intent_description
from almanak.framework.models.reproduction_bundle import ActionBundle

# ---------------------------------------------------------------------------
# SWAP
# ---------------------------------------------------------------------------


class TestSwapDescriptions:
    def test_swap_full_metadata_with_protocol(self) -> None:
        bundle = ActionBundle(
            intent_type="SWAP",
            metadata={
                "from_token": {"symbol": "WETH", "decimals": 18},
                "to_token": {"symbol": "USDC", "decimals": 6},
                "from_amount": "1000000000000000000",  # 1 WETH in wei
                "protocol": "Uniswap V3",
            },
        )
        assert _generate_intent_description(bundle) == "Swap 1 WETH \u2192 USDC via Uniswap V3"

    def test_swap_tokens_only_no_amount(self) -> None:
        bundle = ActionBundle(
            intent_type="SWAP",
            metadata={
                "from_token": {"symbol": "USDC"},
                "to_token": {"symbol": "DAI"},
            },
        )
        assert _generate_intent_description(bundle) == "Swap USDC \u2192 DAI"

    def test_swap_no_tokens_no_amount(self) -> None:
        bundle = ActionBundle(intent_type="SWAP", metadata={})
        assert _generate_intent_description(bundle) == "Swap tokens"

    def test_swap_falls_back_to_amount_key_when_from_amount_missing(self) -> None:
        bundle = ActionBundle(
            intent_type="SWAP",
            metadata={
                "from_token": {"symbol": "USDC", "decimals": 6},
                "to_token": {"symbol": "WETH"},
                "amount": "1500000000",  # 1500 USDC
            },
        )
        assert _generate_intent_description(bundle) == "Swap 1,500 USDC \u2192 WETH"

    def test_swap_uses_canonical_amount_in_key_from_compiler(self) -> None:
        """Pin that the compiler-shaped SWAP metadata (``amount_in``) is honoured.

        ``almanak/framework/intents/compiler.py`` emits ``amount_in`` as the
        canonical input-amount key; losing this lookup silently drops the
        amount and falls back to the token-only form.
        """
        bundle = ActionBundle(
            intent_type="SWAP",
            metadata={
                "from_token": {"symbol": "WETH", "decimals": 18},
                "to_token": {"symbol": "USDC", "decimals": 6},
                "amount_in": "1000000000000000000",  # 1 WETH in wei
                "protocol": "Uniswap V3",
            },
        )
        assert _generate_intent_description(bundle) == "Swap 1 WETH \u2192 USDC via Uniswap V3"

    def test_swap_amount_in_takes_priority_over_from_amount(self) -> None:
        """When multiple amount keys are present, ``amount_in`` wins (canonical)."""
        bundle = ActionBundle(
            intent_type="SWAP",
            metadata={
                "from_token": {"symbol": "WETH", "decimals": 18},
                "to_token": {"symbol": "USDC"},
                # Compiler-shaped canonical key: 2 WETH
                "amount_in": "2000000000000000000",
                # Legacy key with a different value - must be ignored.
                "from_amount": "1000000000000000000",
                "amount": "500000000000000000",
            },
        )
        assert _generate_intent_description(bundle) == "Swap 2 WETH \u2192 USDC"

    def test_swap_lowercase_intent_type_still_matches(self) -> None:
        bundle = ActionBundle(
            intent_type="swap",
            metadata={
                "from_token": "WETH",
                "to_token": "USDC",
            },
        )
        assert _generate_intent_description(bundle) == "Swap WETH \u2192 USDC"


# ---------------------------------------------------------------------------
# SUPPLY / BORROW / REPAY / WITHDRAW
# ---------------------------------------------------------------------------


class TestLendingDescriptions:
    def test_supply_full(self) -> None:
        bundle = ActionBundle(
            intent_type="SUPPLY",
            metadata={
                "supply_token": {"symbol": "WETH", "decimals": 18},
                "supply_amount": "2000000000000000",  # 0.002 WETH
                "protocol": "Aave V3",
            },
        )
        assert _generate_intent_description(bundle) == "Supply 0.002 WETH as collateral on Aave V3"

    def test_supply_token_only_no_amount(self) -> None:
        bundle = ActionBundle(
            intent_type="SUPPLY",
            metadata={"supply_token": {"symbol": "USDC"}, "protocol": "Aave V3"},
        )
        assert _generate_intent_description(bundle) == "Supply USDC as collateral on Aave V3"

    def test_supply_bare_default(self) -> None:
        bundle = ActionBundle(intent_type="SUPPLY", metadata={})
        assert _generate_intent_description(bundle) == "Supply collateral"

    def test_borrow_with_collateral_and_protocol(self) -> None:
        bundle = ActionBundle(
            intent_type="BORROW",
            metadata={
                "borrow_token": {"symbol": "USDC", "decimals": 6},
                "collateral_token": {"symbol": "WETH"},
                # 1,500 USDC in wei (strictly > 1e9 threshold)
                "borrow_amount": "1500000000",
                "protocol": "Aave V3",
            },
        )
        assert _generate_intent_description(bundle) == "Borrow 1,500 USDC against WETH collateral on Aave V3"

    def test_borrow_token_only(self) -> None:
        bundle = ActionBundle(
            intent_type="BORROW",
            metadata={"borrow_token": {"symbol": "USDC"}},
        )
        assert _generate_intent_description(bundle) == "Borrow USDC"

    def test_borrow_bare_default(self) -> None:
        bundle = ActionBundle(intent_type="BORROW", metadata={})
        assert _generate_intent_description(bundle) == "Borrow tokens"

    def test_repay_full(self) -> None:
        bundle = ActionBundle(
            intent_type="REPAY",
            metadata={
                "repay_token": {"symbol": "USDC", "decimals": 6},
                # 2,500 USDC in wei (strictly > 1e9 threshold)
                "repay_amount": "2500000001",
                "protocol": "Aave V3",
            },
        )
        assert _generate_intent_description(bundle) == "Repay 2,500 USDC on Aave V3"

    def test_repay_token_only_debt_fallback(self) -> None:
        bundle = ActionBundle(
            intent_type="REPAY",
            metadata={"repay_token": {"symbol": "USDC"}},
        )
        assert _generate_intent_description(bundle) == "Repay USDC debt"

    def test_repay_bare_default(self) -> None:
        bundle = ActionBundle(intent_type="REPAY", metadata={})
        assert _generate_intent_description(bundle) == "Repay debt"

    def test_withdraw_full(self) -> None:
        bundle = ActionBundle(
            intent_type="WITHDRAW",
            metadata={
                "withdraw_token": {"symbol": "WETH", "decimals": 18},
                "withdraw_amount": "500000000000000000",  # 0.5 WETH
                "protocol": "Aave V3",
            },
        )
        assert _generate_intent_description(bundle) == "Withdraw 0.5 WETH from Aave V3"

    def test_withdraw_token_only(self) -> None:
        bundle = ActionBundle(
            intent_type="WITHDRAW",
            metadata={"withdraw_token": {"symbol": "USDC"}},
        )
        assert _generate_intent_description(bundle) == "Withdraw USDC"

    def test_withdraw_bare_default(self) -> None:
        bundle = ActionBundle(intent_type="WITHDRAW", metadata={})
        assert _generate_intent_description(bundle) == "Withdraw from protocol"


# ---------------------------------------------------------------------------
# LP
# ---------------------------------------------------------------------------


class TestLPDescriptions:
    def test_lp_open_with_tokens_and_protocol(self) -> None:
        bundle = ActionBundle(
            intent_type="LP_OPEN",
            metadata={"token0": "WETH", "token1": "USDC", "protocol": "Uniswap V3"},
        )
        assert _generate_intent_description(bundle) == "Open LP: WETH/USDC on Uniswap V3"

    def test_lp_open_pool_fallback(self) -> None:
        bundle = ActionBundle(
            intent_type="LP_OPEN",
            metadata={"pool": "WETH-USDC-0.05%"},
        )
        assert _generate_intent_description(bundle) == "Open LP: WETH-USDC-0.05%"

    def test_lp_open_bare_default(self) -> None:
        bundle = ActionBundle(intent_type="LP_OPEN", metadata={})
        assert _generate_intent_description(bundle) == "Open LP position"

    def test_lp_close_with_tokens(self) -> None:
        bundle = ActionBundle(
            intent_type="LP_CLOSE",
            metadata={"token0": "WETH", "token1": "USDC", "protocol": "Uniswap V3"},
        )
        assert _generate_intent_description(bundle) == "Close LP: WETH/USDC on Uniswap V3"

    def test_lp_close_bare_default(self) -> None:
        bundle = ActionBundle(intent_type="LP_CLOSE", metadata={})
        assert _generate_intent_description(bundle) == "Close LP position"


# ---------------------------------------------------------------------------
# PERP
# ---------------------------------------------------------------------------


class TestPerpDescriptions:
    def test_perp_open_long_with_collateral_leverage(self) -> None:
        bundle = ActionBundle(
            intent_type="PERP_OPEN",
            metadata={
                "direction": "LONG",  # mixed casing, should be lower-cased
                "market": "ETH-PERP",
                "leverage": 5,
                "collateral_token": {"symbol": "USDC", "decimals": 6},
                # 100 USDC in wei (strictly > 1e9 threshold)
                "collateral_amount": "100000000001",
                "protocol": "Hyperliquid",
            },
        )
        assert _generate_intent_description(bundle) == "Open long: 100,000 USDC (5x) on Hyperliquid"

    def test_perp_open_short_market_fallback(self) -> None:
        bundle = ActionBundle(
            intent_type="PERP_OPEN",
            metadata={"direction": "short", "market": "BTC-PERP"},
        )
        assert _generate_intent_description(bundle) == "Open short: BTC-PERP"

    def test_perp_open_default_direction_long(self) -> None:
        bundle = ActionBundle(intent_type="PERP_OPEN", metadata={})
        assert _generate_intent_description(bundle) == "Open long position"

    def test_perp_close_with_market(self) -> None:
        bundle = ActionBundle(
            intent_type="PERP_CLOSE",
            metadata={"market": "ETH-PERP", "protocol": "Hyperliquid"},
        )
        assert _generate_intent_description(bundle) == "Close position: ETH-PERP on Hyperliquid"

    def test_perp_close_bare_default(self) -> None:
        bundle = ActionBundle(intent_type="PERP_CLOSE", metadata={})
        assert _generate_intent_description(bundle) == "Close perpetual position"


# ---------------------------------------------------------------------------
# BRIDGE
# ---------------------------------------------------------------------------


class TestBridgeDescriptions:
    def test_bridge_full(self) -> None:
        bundle = ActionBundle(
            intent_type="BRIDGE",
            metadata={
                "token": {"symbol": "USDC", "decimals": 6},
                # 2,000 USDC in wei (strictly > 1e9 threshold)
                "amount": "2000000001",
                "from_chain": "arbitrum",
                "to_chain": "base",
                "protocol": "LiFi",
            },
        )
        assert _generate_intent_description(bundle) == "Bridge 2,000 USDC: arbitrum \u2192 base via LiFi"

    def test_bridge_to_chain_only_falls_back_to_metadata_chain(self) -> None:
        bundle = ActionBundle(
            intent_type="BRIDGE",
            metadata={
                "token": {"symbol": "USDC"},
                "chain": "base",  # to_chain will default from here
            },
        )
        assert _generate_intent_description(bundle) == "Bridge USDC to base"

    def test_bridge_bare_default(self) -> None:
        bundle = ActionBundle(intent_type="BRIDGE", metadata={})
        assert _generate_intent_description(bundle) == "Bridge tokens"


# ---------------------------------------------------------------------------
# HOLD / TEARDOWN / Unknown
# ---------------------------------------------------------------------------


class TestMiscDescriptions:
    def test_hold_with_reason(self) -> None:
        bundle = ActionBundle(
            intent_type="HOLD",
            metadata={"reason": "spread too tight"},
        )
        assert _generate_intent_description(bundle) == "Hold: spread too tight"

    def test_hold_bare_default(self) -> None:
        bundle = ActionBundle(intent_type="HOLD", metadata={})
        assert _generate_intent_description(bundle) == "Hold position"

    def test_teardown_uses_default_title_case(self) -> None:
        # TEARDOWN is not a dedicated branch; it falls through to the default.
        bundle = ActionBundle(intent_type="TEARDOWN", metadata={})
        assert _generate_intent_description(bundle) == "Teardown"

    def test_underscore_intent_type_title_cased(self) -> None:
        bundle = ActionBundle(intent_type="custom_unknown_action", metadata={})
        assert _generate_intent_description(bundle) == "Custom Unknown Action"


# ---------------------------------------------------------------------------
# Malformed / edge-case metadata
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_metadata_is_none_treated_as_empty(self) -> None:
        bundle = ActionBundle(intent_type="SWAP", metadata={})
        # Force metadata to None via direct attribute mutation
        bundle.metadata = None  # type: ignore[assignment]
        # Falsy metadata should be replaced with {} inside the function.
        assert _generate_intent_description(bundle) == "Swap tokens"

    def test_token_symbol_falls_back_to_name(self) -> None:
        bundle = ActionBundle(
            intent_type="SWAP",
            metadata={
                "from_token": {"name": "Wrapped Ether"},
                "to_token": {"name": "USD Coin"},
            },
        )
        assert _generate_intent_description(bundle) == "Swap Wrapped Ether \u2192 USD Coin"

    def test_bare_string_token_accepted(self) -> None:
        bundle = ActionBundle(
            intent_type="SWAP",
            metadata={"from_token": "WETH", "to_token": "USDC"},
        )
        assert _generate_intent_description(bundle) == "Swap WETH \u2192 USDC"

    def test_malformed_amount_string_survives(self) -> None:
        # Non-numeric amount should not raise; falls through to str() path.
        bundle = ActionBundle(
            intent_type="SWAP",
            metadata={
                "from_token": {"symbol": "WETH"},
                "to_token": {"symbol": "USDC"},
                "from_amount": "not-a-number",
            },
        )
        assert _generate_intent_description(bundle) == "Swap not-a-number WETH \u2192 USDC"

    def test_zero_amount_treated_as_missing(self) -> None:
        bundle = ActionBundle(
            intent_type="SUPPLY",
            metadata={
                "supply_token": {"symbol": "WETH"},
                "supply_amount": 0,
            },
        )
        # Zero is falsy -> amount formatter returns "", so "as collateral"
        # form is used with the token only.
        assert _generate_intent_description(bundle) == "Supply WETH as collateral"

    def test_unicode_arrow_in_swap_output(self) -> None:
        bundle = ActionBundle(
            intent_type="SWAP",
            metadata={"from_token": "A", "to_token": "B"},
        )
        assert "\u2192" in _generate_intent_description(bundle)

    def test_small_decimal_amount_trims_trailing_zeros(self) -> None:
        # 0.0001 WETH (below the 1_000_000_000 wei-conversion threshold) stays
        # as-is; formatter should trim trailing zeros.
        bundle = ActionBundle(
            intent_type="SUPPLY",
            metadata={
                "supply_token": {"symbol": "WETH"},
                "supply_amount": "0.0001",
            },
        )
        assert _generate_intent_description(bundle) == "Supply 0.0001 WETH as collateral"

    def test_large_int_without_token_decimals_uses_default_18(self) -> None:
        # No decimals metadata -> defaults to 18, so 10**18 == 1.
        bundle = ActionBundle(
            intent_type="SUPPLY",
            metadata={
                "supply_token": {"symbol": "WETH"},
                "supply_amount": 10**18,
            },
        )
        assert _generate_intent_description(bundle) == "Supply 1 WETH as collateral"


# ---------------------------------------------------------------------------
# Parametrized smoke coverage across intent types
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("intent_type", "expected"),
    [
        ("SWAP", "Swap tokens"),
        ("SUPPLY", "Supply collateral"),
        ("BORROW", "Borrow tokens"),
        ("REPAY", "Repay debt"),
        ("WITHDRAW", "Withdraw from protocol"),
        ("LP_OPEN", "Open LP position"),
        ("LP_CLOSE", "Close LP position"),
        ("PERP_OPEN", "Open long position"),
        ("PERP_CLOSE", "Close perpetual position"),
        ("BRIDGE", "Bridge tokens"),
        ("HOLD", "Hold position"),
        ("UNKNOWN_ACTION", "Unknown Action"),
    ],
)
def test_bare_metadata_defaults(intent_type: str, expected: str) -> None:
    """Every known intent type must produce a deterministic bare default string."""
    bundle = ActionBundle(intent_type=intent_type, metadata={})
    assert _generate_intent_description(bundle) == expected
