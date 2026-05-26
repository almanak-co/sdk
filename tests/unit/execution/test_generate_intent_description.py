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

        ``almanak/connectors/traderjoe_v2/compiler.py`` emits ``amount_in`` as the
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
        # Production metadata always carries the canonical protocol key
        # (``aave_v3``) rather than the display name. ``_WEI_LENDING_PROTOCOLS``
        # is keyed by the canonical name so the wei-vs-human metadata
        # convention can be resolved consistently with the pre-flight checker.
        bundle = ActionBundle(
            intent_type="SUPPLY",
            metadata={
                "supply_token": {"symbol": "WETH", "decimals": 18},
                "supply_amount": "2000000000000000",  # 0.002 WETH (wei)
                "protocol": "aave_v3",
            },
        )
        assert _generate_intent_description(bundle) == "Supply 0.002 WETH as collateral on aave_v3"

    def test_supply_token_only_no_amount(self) -> None:
        bundle = ActionBundle(
            intent_type="SUPPLY",
            metadata={"supply_token": {"symbol": "USDC"}, "protocol": "aave_v3"},
        )
        assert _generate_intent_description(bundle) == "Supply USDC as collateral on aave_v3"

    def test_supply_bare_default(self) -> None:
        bundle = ActionBundle(intent_type="SUPPLY", metadata={})
        assert _generate_intent_description(bundle) == "Supply collateral"

    def test_borrow_with_collateral_and_protocol(self) -> None:
        bundle = ActionBundle(
            intent_type="BORROW",
            metadata={
                "borrow_token": {"symbol": "USDC", "decimals": 6},
                "collateral_token": {"symbol": "WETH"},
                # 1,500 USDC in wei. Aave V3 (in ``_WEI_LENDING_PROTOCOLS``)
                # encodes lending amounts as wei, so the formatter must scale.
                "borrow_amount": "1500000000",
                "protocol": "aave_v3",
            },
        )
        assert _generate_intent_description(bundle) == "Borrow 1,500 USDC against WETH collateral on aave_v3"

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
                # 2,500 USDC in wei. Aave V3 metadata is wei-encoded.
                "repay_amount": "2500000001",
                "protocol": "aave_v3",
            },
        )
        assert _generate_intent_description(bundle) == "Repay 2,500 USDC on aave_v3"

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
                "withdraw_amount": "500000000000000000",  # 0.5 WETH (wei)
                "protocol": "aave_v3",
            },
        )
        assert _generate_intent_description(bundle) == "Withdraw 0.5 WETH from aave_v3"

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
        # PERP_OPEN metadata stores the human-readable Decimal collateral
        # amount (see ``IntentCompiler._compile_perp_open`` and
        # ``DriftAdapter``: ``"collateral_amount": str(intent.collateral_amount)``).
        # The pre-fix test used a synthetic wei integer that the magnitude
        # heuristic incidentally re-scaled — replaced with the real shape
        # now that the formatter honours ``is_wei=False`` for perps.
        bundle = ActionBundle(
            intent_type="PERP_OPEN",
            metadata={
                "direction": "LONG",  # mixed casing, should be lower-cased
                "market": "ETH-PERP",
                "leverage": 5,
                "collateral_token": {"symbol": "USDC", "decimals": 6},
                "collateral_amount": "100000",  # human Decimal: $100,000 USDC
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
        # Bridge metadata stores already-human Decimal strings (see
        # ``BridgeCompiler.compile_bridge``: ``"amount": str(amount_decimal)``).
        # The previous test used a synthetic wei integer that the heuristic
        # incidentally rescaled \u2014 replaced with the real shape now that
        # ``_format_amount`` honours an explicit ``is_wei=False`` for bridge.
        bundle = ActionBundle(
            intent_type="BRIDGE",
            metadata={
                "token": {"symbol": "USDC", "decimals": 6},
                "amount": "2000",  # human Decimal (compiler convention)
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
# VIB-3747: small-amount 6-decimal token regression coverage.
#
# The previous ``_format_amount`` used a magnitude heuristic
# (``amount > 1_000_000_000``) to decide when to scale by ``10**decimals``.
# That broke for 6-decimal tokens at small dollar values: a $100 USDC SWAP
# carries ``amount_in = 100_000_000`` raw, which sat below the 1e9
# threshold and printed verbatim as ``Swap 100,000,000 USDC -> WETH``.
# The misleading log was reported as a fictitious "1e6 amount inflation"
# bug (BUG-55 in the April 29 QA batch). These tests pin the corrected
# behaviour: small 6-decimal SWAPs format as the human dollar amount.
# ---------------------------------------------------------------------------


class TestSmallAmountSixDecimalTokens:
    """Regression coverage for VIB-3747 (BUG-55, QA April 29 batch 17/18).

    Each test below would have failed under the pre-fix heuristic:
    100M USDC raw < 1e9 threshold -> formatter printed wei verbatim. With
    ``is_wei=True`` defaults plumbed through every ``_describe_*`` helper,
    the output is now the human dollar amount.
    """

    def test_swap_100_usdc_polygon_compile_metadata(self) -> None:
        """``enso_swap_polygon`` BUY: 100 USDC -> WETH at compile time.

        Mirrors the exact metadata shape emitted by
        ``IntentCompiler._compile_enso_swap_intent`` for the QA failure case.
        """
        bundle = ActionBundle(
            intent_type="SWAP",
            metadata={
                "from_token": {"symbol": "USDC", "decimals": 6, "address": "0x3c499c542cef5e3811e1192ce70d8cc03d5c3359"},
                "to_token": {"symbol": "WETH", "decimals": 18},
                "amount_in": "100000000",  # $100 USDC, 6 decimals
                "protocol": "enso",
                "chain": "polygon",
            },
        )
        # Pre-fix output: "Swap 100,000,000 USDC → WETH via enso".
        assert _generate_intent_description(bundle) == "Swap 100 USDC → WETH via enso"

    def test_swap_200_usdc_ethereum_ethena_morpho(self) -> None:
        """``ethena_susde_morpho_yield`` Ethereum: 200 USDC -> USDe.

        Same root cause as Polygon (BUG-55 extended in B18). 200 USDC is
        2e8 raw, still below the old 1e9 threshold.
        """
        bundle = ActionBundle(
            intent_type="SWAP",
            metadata={
                "from_token": {"symbol": "USDC", "decimals": 6},
                "to_token": {"symbol": "USDE", "decimals": 18},
                "amount_in": "200000000",  # $200 USDC
                "protocol": "enso",
                "chain": "ethereum",
            },
        )
        assert _generate_intent_description(bundle) == "Swap 200 USDC → USDE via enso"

    def test_swap_5_usdc_base(self) -> None:
        """``enso_swap_base`` SELL: 5 USDC swap. Below threshold by 3 orders."""
        bundle = ActionBundle(
            intent_type="SWAP",
            metadata={
                "from_token": {"symbol": "USDC", "decimals": 6},
                "to_token": {"symbol": "WETH", "decimals": 18},
                "amount_in": "5000000",  # $5 USDC
                "protocol": "enso",
                "chain": "base",
            },
        )
        assert _generate_intent_description(bundle) == "Swap 5 USDC → WETH via enso"

    def test_swap_10_usdt_arbitrum(self) -> None:
        """USDT (also 6 decimals) at $10. Same threshold trap as USDC."""
        bundle = ActionBundle(
            intent_type="SWAP",
            metadata={
                "from_token": {"symbol": "USDT", "decimals": 6},
                "to_token": {"symbol": "WETH", "decimals": 18},
                "amount_in": "10000000",  # $10 USDT
                "protocol": "uniswap_v3",
                "chain": "arbitrum",
            },
        )
        assert _generate_intent_description(bundle) == "Swap 10 USDT → WETH via uniswap_v3"

    def test_supply_100_usdc_aave_polygon(self) -> None:
        """Aave V3 SUPPLY uses wei-encoded ``supply_amount`` per
        ``_WEI_LENDING_PROTOCOLS``. $100 USDC = 1e8 raw < old threshold.
        """
        bundle = ActionBundle(
            intent_type="SUPPLY",
            metadata={
                "supply_token": {"symbol": "USDC", "decimals": 6},
                "supply_amount": "100000000",  # $100 USDC, wei
                "protocol": "aave_v3",
                "chain": "polygon",
            },
        )
        assert _generate_intent_description(bundle) == "Supply 100 USDC as collateral on aave_v3"

    def test_supply_100_usdc_morpho_blue_uses_human_amount(self) -> None:
        """Morpho Blue is OUTSIDE ``_WEI_LENDING_PROTOCOLS`` -- supply_amount
        is already human ("100"). Without my fix it was rescued by accident
        because the value was below 1e9; after the fix the explicit
        ``is_wei=False`` path keeps the human shape working too.
        """
        bundle = ActionBundle(
            intent_type="SUPPLY",
            metadata={
                "supply_token": {"symbol": "USDC", "decimals": 6},
                "supply_amount": "100",  # human Decimal (Morpho convention)
                "protocol": "morpho_blue",
                "chain": "ethereum",
            },
        )
        assert _generate_intent_description(bundle) == "Supply 100 USDC as collateral on morpho_blue"

    def test_swap_uses_amount_in_decimal_below_threshold(self) -> None:
        """Belt-and-braces: $0.50 USDC = 500_000 raw (well below threshold)."""
        bundle = ActionBundle(
            intent_type="SWAP",
            metadata={
                "from_token": {"symbol": "USDC", "decimals": 6},
                "to_token": {"symbol": "WETH", "decimals": 18},
                "amount_in": "500000",  # $0.50 USDC
                "protocol": "enso",
            },
        )
        assert _generate_intent_description(bundle) == "Swap 0.5 USDC → WETH via enso"

    def test_swap_curve_uses_human_decimal(self) -> None:
        """Curve SWAP compiler ships ``str(amount_decimal)`` (human),
        not wei. Description must NOT
        scale by ``10**decimals`` for the curve path or a $100 USDC swap
        would render as ``Swap 0.0001 USDC → DAI`` (Codex P2 catch).
        """
        bundle = ActionBundle(
            intent_type="SWAP",
            metadata={
                "from_token": {"symbol": "USDC", "decimals": 6},
                "to_token": {"symbol": "DAI", "decimals": 18},
                "amount_in": "100",  # human Decimal: 100 USDC
                "protocol": "curve",
                "chain": "ethereum",
            },
        )
        assert _generate_intent_description(bundle) == "Swap 100 USDC → DAI via curve"

    def test_swap_aerodrome_uses_human_decimal(self) -> None:
        """Aerodrome SWAP compiler ships ``str(amount_decimal)`` (human),
        not wei (see ``connectors/aerodrome/compiler.py``). Same as Curve.
        """
        bundle = ActionBundle(
            intent_type="SWAP",
            metadata={
                "from_token": {"symbol": "USDC", "decimals": 6},
                "to_token": {"symbol": "WETH", "decimals": 18},
                "amount_in": "50",  # human Decimal: 50 USDC
                "protocol": "aerodrome",
                "chain": "base",
            },
        )
        assert _generate_intent_description(bundle) == "Swap 50 USDC → WETH via aerodrome"

    def test_supply_protocol_case_insensitive_lookup(self) -> None:
        """``_lending_amount_is_wei`` and the pre-flight collector both
        normalise via ``_normalize_protocol_key``: lowercase + space/
        hyphen collapse. So a SUPPLY bundle carrying any of the common
        display variants (``"Aave V3"``, ``"AAVE_V3"``, ``"aave-v3"``)
        is classified consistently across both pipelines.
        """
        for variant in ("AAVE_V3", "Aave V3", "aave-v3", "  aave_v3  "):
            bundle = ActionBundle(
                intent_type="SUPPLY",
                metadata={
                    "supply_token": {"symbol": "USDC", "decimals": 6},
                    "supply_amount": "100000000",  # $100 USDC, wei
                    "protocol": variant,
                },
            )
            assert _generate_intent_description(bundle) == f"Supply 100 USDC as collateral on {variant}", (
                f"failed for protocol={variant!r}"
            )

    def test_swap_protocol_case_insensitive_lookup(self) -> None:
        """``_swap_amount_is_wei`` is also case-insensitive so a
        ``"Curve"`` / ``"CURVE"`` bundle stays in the human-Decimal
        branch instead of silently re-scaling by 10**decimals.
        """
        bundle = ActionBundle(
            intent_type="SWAP",
            metadata={
                "from_token": {"symbol": "USDC", "decimals": 6},
                "to_token": {"symbol": "DAI", "decimals": 18},
                "amount_in": "100",
                "protocol": "Curve",  # display-cased
            },
        )
        assert _generate_intent_description(bundle) == "Swap 100 USDC → DAI via Curve"


# ---------------------------------------------------------------------------
# Pre-flight collector coverage for VIB-3747.
#
# The same ``_WEI_LENDING_PROTOCOLS`` / ``_HUMAN_AMOUNT_SWAP_PROTOCOLS`` sets
# now drive both ``_describe_*`` (description rendering, this file's main
# subject) AND ``_preflight_*_requirements`` (balance-check pipeline, see
# ``test_preflight_balance_check.py`` for end-to-end coverage). Even though
# pre-flight has its own e2e tests, exercising the pure collector functions
# directly here pins the wei/human classification at the boundary -- so a
# future change that loosens membership semantics on either set would surface
# both as a description regression AND a pre-flight regression in one suite.
# ---------------------------------------------------------------------------


class TestPreflightCollectorClassification:
    """Direct unit coverage for the collector helpers' wei/human flagging."""

    def test_preflight_swap_curve_uses_human_decimal(self) -> None:
        """Curve SWAP pre-flight must NOT treat the metadata amount as
        wei. A Curve $100 USDC swap ships ``"100"`` (human Decimal); if
        the collector mis-classified it as wei, the pre-flight balance
        check would treat it as 100 raw wei (= 1e-16 USDC) and let any
        wallet pass -- silently bypassing the safety gate.
        """
        from almanak.framework.execution.orchestrator import _preflight_swap_requirements

        metadata = {
            "from_token": {"symbol": "USDC", "address": "0x" + "11" * 20, "decimals": 6, "is_native": False},
            "amount_in": "100",  # human Decimal: 100 USDC
            "protocol": "curve",
        }
        reqs = _preflight_swap_requirements(metadata, "curve")
        assert len(reqs) == 1
        symbol, _addr, amount_wei, decimals, _is_native = reqs[0]
        assert symbol == "USDC"
        assert decimals == 6
        # 100 USDC in wei = 100 * 10**6 = 100_000_000
        assert amount_wei == 100_000_000

    def test_preflight_swap_uniswap_v3_uses_wei(self) -> None:
        """Uniswap V3 SWAP pre-flight uses the wei-encoded ``amount_in``
        as-is. This is the default behaviour for every non-Curve /
        non-Aerodrome SWAP compiler.
        """
        from almanak.framework.execution.orchestrator import _preflight_swap_requirements

        metadata = {
            "from_token": {"symbol": "USDC", "address": "0x" + "11" * 20, "decimals": 6, "is_native": False},
            "amount_in": "100000000",  # wei: $100 USDC
            "protocol": "uniswap_v3",
        }
        reqs = _preflight_swap_requirements(metadata, "uniswap_v3")
        assert len(reqs) == 1
        _symbol, _addr, amount_wei, _decimals, _is_native = reqs[0]
        assert amount_wei == 100_000_000

    def test_preflight_supply_aave_v3_canonical_lowercase(self) -> None:
        """Sanity: canonical ``aave_v3`` is treated as wei."""
        from almanak.framework.execution.orchestrator import _preflight_supply_requirements

        metadata = {
            "supply_token": {"symbol": "USDC", "address": "0x" + "11" * 20, "decimals": 6, "is_native": False},
            "supply_amount": "100000000",  # wei: $100 USDC
            "protocol": "aave_v3",
        }
        reqs = _preflight_supply_requirements(metadata, "aave_v3")
        assert len(reqs) == 1
        _symbol, _addr, amount_wei, _decimals, _is_native = reqs[0]
        assert amount_wei == 100_000_000

    def test_preflight_supply_morpho_blue_human_decimal(self) -> None:
        """Sanity: morpho_blue is OUTSIDE the wei set -> human Decimal."""
        from almanak.framework.execution.orchestrator import _preflight_supply_requirements

        metadata = {
            "supply_token": {"symbol": "USDC", "address": "0x" + "11" * 20, "decimals": 6, "is_native": False},
            "supply_amount": "100",  # human Decimal: 100 USDC
            "protocol": "morpho_blue",
        }
        reqs = _preflight_supply_requirements(metadata, "morpho_blue")
        assert len(reqs) == 1
        _symbol, _addr, amount_wei, _decimals, _is_native = reqs[0]
        assert amount_wei == 100_000_000  # 100 * 10**6


class TestProtocolNormalizationHelpers:
    """Direct coverage for ``_lending_amount_is_wei`` / ``_swap_amount_is_wei``.

    These helpers must accept any casing ("aave_v3", "Aave V3",
    "AAVE_V3") so that description rendering and pre-flight checks
    agree regardless of caller. Without this, a metadata bundle from a
    non-canonical caller would silently disagree across the two sites.
    """

    def test_lending_amount_is_wei_canonical(self) -> None:
        from almanak.framework.execution.orchestrator import _lending_amount_is_wei

        assert _lending_amount_is_wei("aave_v3") is True
        assert _lending_amount_is_wei("spark") is True
        assert _lending_amount_is_wei("radiant_v2") is True
        assert _lending_amount_is_wei("morpho_blue") is False
        assert _lending_amount_is_wei("compound_v3") is False

    def test_lending_amount_is_wei_case_insensitive(self) -> None:
        from almanak.framework.execution.orchestrator import _lending_amount_is_wei

        # Upper-snake forms (``AAVE_V3``) previously caused drift: pre-flight
        # already lowercases via ``_preflight_collect_requirements`` and would
        # match, but the description path didn't lowercase so it fell through
        # to the human branch. Now both sides agree.
        assert _lending_amount_is_wei("AAVE_V3") is True
        assert _lending_amount_is_wei("Spark") is True
        assert _lending_amount_is_wei("RADIANT_V2") is True
        # Display names with spaces and hyphens are also normalised to the
        # canonical underscore-snake form by ``_normalize_protocol_key``,
        # so legacy / hand-built bundles classify consistently across the
        # description and pre-flight paths.
        assert _lending_amount_is_wei("Aave V3") is True
        assert _lending_amount_is_wei("aave-v3") is True
        assert _lending_amount_is_wei("  spark  ") is True  # whitespace
        # Non-members stay non-members regardless of formatting.
        assert _lending_amount_is_wei("Morpho Blue") is False
        assert _lending_amount_is_wei("MORPHO_BLUE") is False
        assert _lending_amount_is_wei("morpho-blue") is False

    def test_lending_amount_is_wei_non_string_returns_false(self) -> None:
        """Defence-in-depth: malformed metadata (None, int, dict) must
        not crash -- return False (safer default for description, where
        a wrong wei classification would visibly under-render).
        """
        from almanak.framework.execution.orchestrator import _lending_amount_is_wei

        assert _lending_amount_is_wei(None) is False
        assert _lending_amount_is_wei(123) is False
        assert _lending_amount_is_wei({}) is False

    def test_swap_amount_is_wei_canonical(self) -> None:
        from almanak.framework.execution.orchestrator import _swap_amount_is_wei

        assert _swap_amount_is_wei("uniswap_v3") is True  # default
        assert _swap_amount_is_wei("enso") is True
        assert _swap_amount_is_wei("pendle") is True
        assert _swap_amount_is_wei("curve") is False
        assert _swap_amount_is_wei("aerodrome") is False

    def test_swap_amount_is_wei_case_insensitive(self) -> None:
        from almanak.framework.execution.orchestrator import _swap_amount_is_wei

        # Display-name and casing variants all normalise to the canonical
        # key via ``_normalize_protocol_key`` (lowercase + space/hyphen
        # collapse) so the description and pre-flight paths agree.
        assert _swap_amount_is_wei("Curve") is False
        assert _swap_amount_is_wei("CURVE") is False
        assert _swap_amount_is_wei("Aerodrome") is False
        # Spaces and hyphens collapse to underscores.
        assert _swap_amount_is_wei("Uniswap V3") is True  # not in human set
        assert _swap_amount_is_wei("uniswap-v3") is True

    def test_swap_amount_is_wei_non_string_defaults_true(self) -> None:
        """When ``protocol`` is missing/None, default to wei. Matches the
        legacy behaviour where ``_format_amount`` always tried to scale
        by ``10**decimals`` whenever ``token_data`` was present.
        """
        from almanak.framework.execution.orchestrator import _swap_amount_is_wei

        assert _swap_amount_is_wei(None) is True
        assert _swap_amount_is_wei("") is True
        assert _swap_amount_is_wei(123) is True


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
        # Pin a wei-encoded protocol (``aave_v3``) so the formatter scales
        # by 10**decimals; the test's purpose is the default-18 behaviour
        # of ``_get_token_decimals``, not the wei/human distinction.
        bundle = ActionBundle(
            intent_type="SUPPLY",
            metadata={
                "supply_token": {"symbol": "WETH"},
                "supply_amount": 10**18,
                "protocol": "aave_v3",
            },
        )
        assert _generate_intent_description(bundle) == "Supply 1 WETH as collateral on aave_v3"


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
