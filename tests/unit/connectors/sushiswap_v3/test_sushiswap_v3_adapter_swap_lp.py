"""Unit tests for SushiSwapV3Adapter swap and LP operations.

Covers:
- swap_exact_input / swap_exact_output happy paths and error branches
- open_lp_position / close_lp_position
- _build_approve_tx + allowance cache
- _build_exact_input_single_tx / _build_exact_output_single_tx description formatting
- _get_quote_exact_input / _get_quote_exact_output
- _is_native_token branches
- _get_placeholder_prices, _get_default_price_oracle
- Config validation
- Pad helpers
- to_dict roundtrip dataclasses
- set_allowance / clear_allowance_cache
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.framework.connectors.sushiswap_v3.adapter import (
    DEFAULT_FEE_TIER,
    LPResult,
    SushiSwapV3Adapter,
    SushiSwapV3Config,
    SwapQuote,
    SwapResult,
    SwapType,
    TransactionData,
)
from almanak.framework.data.tokens.exceptions import TokenResolutionError
from almanak.framework.data.tokens.models import ResolvedToken

WALLET = "0x1234567890123456789012345678901234567890"
USDC_ARB = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
WETH_ARB = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"
WBTC_ARB = "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f"
NATIVE_PLACEHOLDER = "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE"


# -------------------------------------------------------------------------
# Resolver helpers
# -------------------------------------------------------------------------


def _make_resolver():
    """Build a TokenResolver mock with a prebuilt token table."""
    tokens = {
        "USDC": ResolvedToken(symbol="USDC", address=USDC_ARB, decimals=6, chain="arbitrum", chain_id=42161),
        "WETH": ResolvedToken(symbol="WETH", address=WETH_ARB, decimals=18, chain="arbitrum", chain_id=42161),
        "WBTC": ResolvedToken(symbol="WBTC", address=WBTC_ARB, decimals=8, chain="arbitrum", chain_id=42161),
        "ETH": ResolvedToken(symbol="ETH", address=NATIVE_PLACEHOLDER, decimals=18, chain="arbitrum", chain_id=42161),
        # Address keys (production resolver supports both symbol & address lookup)
        USDC_ARB: ResolvedToken(symbol="USDC", address=USDC_ARB, decimals=6, chain="arbitrum", chain_id=42161),
        USDC_ARB.lower(): ResolvedToken(symbol="USDC", address=USDC_ARB, decimals=6, chain="arbitrum", chain_id=42161),
        WETH_ARB: ResolvedToken(symbol="WETH", address=WETH_ARB, decimals=18, chain="arbitrum", chain_id=42161),
        WETH_ARB.lower(): ResolvedToken(symbol="WETH", address=WETH_ARB, decimals=18, chain="arbitrum", chain_id=42161),
        WBTC_ARB: ResolvedToken(symbol="WBTC", address=WBTC_ARB, decimals=8, chain="arbitrum", chain_id=42161),
        WBTC_ARB.lower(): ResolvedToken(symbol="WBTC", address=WBTC_ARB, decimals=8, chain="arbitrum", chain_id=42161),
        NATIVE_PLACEHOLDER: ResolvedToken(symbol="ETH", address=NATIVE_PLACEHOLDER, decimals=18, chain="arbitrum", chain_id=42161),
        NATIVE_PLACEHOLDER.lower(): ResolvedToken(symbol="ETH", address=NATIVE_PLACEHOLDER, decimals=18, chain="arbitrum", chain_id=42161),
    }
    wrapped_eth = ResolvedToken(symbol="WETH", address=WETH_ARB, decimals=18, chain="arbitrum", chain_id=42161)

    def _resolve(token, chain, **kwargs):
        if token in tokens:
            return tokens[token]
        # Handle "USDC" symbol lookup case-insensitively
        if isinstance(token, str) and token.upper() in tokens:
            return tokens[token.upper()]
        raise TokenResolutionError(token=token, chain=chain, reason="not in test fixture")

    def _resolve_for_swap(token, chain):
        return wrapped_eth

    resolver = MagicMock()
    resolver.resolve.side_effect = _resolve
    resolver.resolve_for_swap.side_effect = _resolve_for_swap
    return resolver


@pytest.fixture
def resolver():
    return _make_resolver()


@pytest.fixture
def config():
    return SushiSwapV3Config(
        chain="arbitrum",
        wallet_address=WALLET,
        price_provider={
            "USDC": Decimal("1"),
            "WETH": Decimal("3400"),
            "ETH": Decimal("3400"),
            "WBTC": Decimal("65000"),
        },
    )


@pytest.fixture
def adapter(config, resolver):
    return SushiSwapV3Adapter(config, token_resolver=resolver)


# -------------------------------------------------------------------------
# Config validation
# -------------------------------------------------------------------------


class TestConfigValidation:
    def test_unsupported_chain_raises(self):
        with pytest.raises(ValueError, match="Unsupported chain"):
            SushiSwapV3Config(
                chain="unknown", wallet_address=WALLET,
                allow_placeholder_prices=True,
            )

    def test_negative_slippage_raises(self):
        with pytest.raises(ValueError, match="Slippage"):
            SushiSwapV3Config(
                chain="arbitrum", wallet_address=WALLET,
                default_slippage_bps=-1,
                allow_placeholder_prices=True,
            )

    def test_excessive_slippage_raises(self):
        with pytest.raises(ValueError, match="Slippage"):
            SushiSwapV3Config(
                chain="arbitrum", wallet_address=WALLET,
                default_slippage_bps=10001,
                allow_placeholder_prices=True,
            )

    def test_invalid_fee_tier_raises(self):
        with pytest.raises(ValueError, match="fee tier"):
            SushiSwapV3Config(
                chain="arbitrum", wallet_address=WALLET,
                default_fee_tier=12345,
                allow_placeholder_prices=True,
            )

    def test_no_price_provider_raises_by_default(self):
        with pytest.raises(ValueError, match="price_provider"):
            SushiSwapV3Config(
                chain="arbitrum", wallet_address=WALLET,
                # No price_provider, no allow_placeholder_prices=True
            )

    def test_allow_placeholders_succeeds(self):
        c = SushiSwapV3Config(
            chain="arbitrum", wallet_address=WALLET,
            allow_placeholder_prices=True,
        )
        assert c.chain == "arbitrum"

    def test_to_dict(self):
        c = SushiSwapV3Config(
            chain="arbitrum", wallet_address=WALLET,
            allow_placeholder_prices=True,
        )
        d = c.to_dict()
        assert d["chain"] == "arbitrum"
        assert d["default_fee_tier"] == DEFAULT_FEE_TIER


# -------------------------------------------------------------------------
# Adapter init / placeholder prices
# -------------------------------------------------------------------------


class TestAdapterInit:
    def test_uses_placeholder_prices_when_no_provider(self, resolver):
        config = SushiSwapV3Config(
            chain="arbitrum", wallet_address=WALLET,
            allow_placeholder_prices=True,
        )
        adapter = SushiSwapV3Adapter(config, token_resolver=resolver)
        assert adapter._using_placeholders is True
        assert "ETH" in adapter._price_provider
        assert "USDC" in adapter._price_provider

    def test_uses_provided_prices(self, config, resolver):
        adapter = SushiSwapV3Adapter(config, token_resolver=resolver)
        assert adapter._using_placeholders is False
        assert adapter._price_provider["WETH"] == Decimal("3400")

    def test_get_default_price_oracle_returns_provider(self, adapter):
        prices = adapter._get_default_price_oracle()
        assert prices is adapter._price_provider


# -------------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------------


class TestPadHelpers:
    def test_pad_address(self):
        result = SushiSwapV3Adapter._pad_address("0xABCDEF1234567890abcdef1234567890ABCDEF12")
        assert len(result) == 64
        assert "0x" not in result
        assert result.endswith("abcdef1234567890abcdef1234567890abcdef12")

    def test_pad_uint256(self):
        result = SushiSwapV3Adapter._pad_uint256(0)
        assert result == "0" * 64

    def test_pad_uint256_large(self):
        result = SushiSwapV3Adapter._pad_uint256(2**256 - 1)
        assert result == "f" * 64

    def test_pad_uint24(self):
        result = SushiSwapV3Adapter._pad_uint24(3000)
        assert len(result) == 64
        assert int(result, 16) == 3000


class TestIsNativeToken:
    @pytest.mark.parametrize("token", ["ETH", "MATIC", "AVAX", "BNB", "eth", "Eth"])
    def test_native_symbols(self, adapter, token):
        assert adapter._is_native_token(token) is True

    def test_native_placeholder_address(self, adapter):
        assert adapter._is_native_token(NATIVE_PLACEHOLDER) is True

    def test_lowercase_placeholder(self, adapter):
        assert adapter._is_native_token(NATIVE_PLACEHOLDER.lower()) is True

    def test_non_native(self, adapter):
        assert adapter._is_native_token("USDC") is False
        assert adapter._is_native_token(USDC_ARB) is False


class TestGetTokenSymbol:
    def test_symbol_passthrough_when_not_address(self, adapter):
        assert adapter._get_token_symbol("USDC") == "USDC"

    def test_address_resolves_to_symbol(self, adapter):
        sym = adapter._get_token_symbol(USDC_ARB)
        assert sym == "USDC"

    def test_unresolvable_address_truncated(self, adapter, resolver):
        # Make resolver fail
        resolver.resolve.side_effect = TokenResolutionError(
            token="0xdead", chain="arbitrum", reason="missing"
        )
        result = adapter._get_token_symbol("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef")
        assert "..." in result


# -------------------------------------------------------------------------
# Quotes
# -------------------------------------------------------------------------


class TestQuoteExactInput:
    def test_basic_quote(self, adapter):
        q = adapter._get_quote_exact_input(
            token_in=USDC_ARB, token_out=WETH_ARB,
            amount_in=1000 * 10**6, fee_tier=3000,
        )
        assert q.amount_in == 1000 * 10**6
        assert q.amount_out > 0
        assert q.fee_tier == 3000
        assert q.effective_price > 0

    def test_zero_amount_in(self, adapter):
        q = adapter._get_quote_exact_input(
            token_in=USDC_ARB, token_out=WETH_ARB,
            amount_in=0, fee_tier=3000,
        )
        assert q.amount_out == 0
        assert q.effective_price == Decimal("0")


class TestQuoteExactOutput:
    def test_basic_quote(self, adapter):
        q = adapter._get_quote_exact_output(
            token_in=USDC_ARB, token_out=WETH_ARB,
            amount_out=10**18, fee_tier=3000,
        )
        assert q.amount_out == 10**18
        assert q.amount_in > 0


# -------------------------------------------------------------------------
# Approve
# -------------------------------------------------------------------------


class TestBuildApproveTx:
    def test_creates_approve_when_no_allowance(self, adapter):
        tx = adapter._build_approve_tx(USDC_ARB, "0xabc", 1000)
        assert tx is not None
        assert tx.tx_type == "approve"
        assert tx.to == USDC_ARB
        assert tx.value == 0
        assert tx.data.startswith("0x095ea7b3")

    def test_skips_approve_when_cached_sufficient(self, adapter):
        adapter.set_allowance(USDC_ARB, "0xabc", 10**30)
        tx = adapter._build_approve_tx(USDC_ARB, "0xabc", 1000)
        assert tx is None

    def test_creates_approve_when_cached_insufficient(self, adapter):
        adapter.set_allowance(USDC_ARB, "0xabc", 100)
        tx = adapter._build_approve_tx(USDC_ARB, "0xabc", 1000)
        assert tx is not None

    def test_clear_allowance_cache(self, adapter):
        adapter.set_allowance(USDC_ARB, "0xabc", 10**30)
        adapter.clear_allowance_cache()
        # Next approve should create a tx
        tx = adapter._build_approve_tx(USDC_ARB, "0xabc", 1000)
        assert tx is not None


# -------------------------------------------------------------------------
# Build exact input single / exact output single
# -------------------------------------------------------------------------


class TestBuildExactInputSingleTx:
    def test_calldata_format(self, adapter):
        tx = adapter._build_exact_input_single_tx(
            token_in=USDC_ARB, token_out=WETH_ARB, fee=3000,
            recipient=WALLET, amount_in=1000 * 10**6,
            amount_out_minimum=29 * 10**16,
        )
        assert tx.data.startswith("0x414bf389")
        assert tx.tx_type == "swap"
        assert tx.value == 0
        assert "USDC" in tx.description
        assert "WETH" in tx.description

    def test_with_value(self, adapter):
        tx = adapter._build_exact_input_single_tx(
            token_in=WETH_ARB, token_out=USDC_ARB, fee=3000,
            recipient=WALLET, amount_in=10**18, amount_out_minimum=0,
            value=10**18,
        )
        assert tx.value == 10**18


class TestBuildExactOutputSingleTx:
    def test_calldata_format(self, adapter):
        tx = adapter._build_exact_output_single_tx(
            token_in=USDC_ARB, token_out=WETH_ARB, fee=3000,
            recipient=WALLET, amount_out=10**18,
            amount_in_maximum=4000 * 10**6,
        )
        assert tx.data.startswith("0xdb3e2198")
        assert tx.tx_type == "swap"
        assert "USDC" in tx.description


# -------------------------------------------------------------------------
# swap_exact_input
# -------------------------------------------------------------------------


class TestSwapExactInput:
    def test_basic_swap(self, adapter):
        result = adapter.swap_exact_input("USDC", "WETH", Decimal("1000"))
        assert result.success is True
        # 2 transactions: approve + swap
        assert len(result.transactions) == 2
        assert result.transactions[0].tx_type == "approve"
        assert result.transactions[1].tx_type == "swap"
        assert result.amount_in == 1000 * 10**6
        assert result.amount_out_minimum > 0
        assert result.gas_estimate > 0

    def test_native_input_skips_approve(self, adapter):
        result = adapter.swap_exact_input("ETH", "USDC", Decimal("1"))
        assert result.success is True
        # Only swap (no approve for native)
        assert len(result.transactions) == 1
        assert result.transactions[0].tx_type == "swap"
        assert result.transactions[0].value == 10**18

    def test_unknown_input_token_returns_error(self, adapter):
        result = adapter.swap_exact_input("UNKNOWN_TOKEN_FOO", "USDC", Decimal("1"))
        assert result.success is False
        assert result.error is not None

    def test_unknown_output_token_returns_error(self, adapter):
        result = adapter.swap_exact_input("USDC", "UNKNOWN_TOKEN_BAR", Decimal("1"))
        assert result.success is False
        assert result.error is not None

    def test_with_explicit_recipient(self, adapter):
        custom_recipient = "0x9999999999999999999999999999999999999999"
        result = adapter.swap_exact_input(
            "USDC", "WETH", Decimal("1000"),
            recipient=custom_recipient,
        )
        assert result.success is True

    def test_with_zero_slippage(self, adapter):
        result = adapter.swap_exact_input(
            "USDC", "WETH", Decimal("1000"), slippage_bps=0,
        )
        # slippage_bps=0 -> amount_out_minimum == quote.amount_out
        assert result.success is True
        assert result.amount_out_minimum == result.quote.amount_out

    def test_with_explicit_fee_tier(self, adapter):
        result = adapter.swap_exact_input(
            "USDC", "WETH", Decimal("1000"), fee_tier=500,
        )
        assert result.success is True
        assert result.quote.fee_tier == 500

    def test_cached_allowance_skips_approve(self, adapter):
        # Pre-fill cache
        adapter.set_allowance(USDC_ARB, adapter.addresses["swap_router"], 10**30)
        result = adapter.swap_exact_input("USDC", "WETH", Decimal("1000"))
        assert result.success is True
        # Only swap (no approve)
        assert len(result.transactions) == 1
        assert result.transactions[0].tx_type == "swap"


# -------------------------------------------------------------------------
# swap_exact_output
# -------------------------------------------------------------------------


class TestSwapExactOutput:
    def test_basic(self, adapter):
        result = adapter.swap_exact_output("USDC", "WETH", Decimal("0.1"))
        assert result.success is True
        # approve + swap
        assert len(result.transactions) == 2
        assert result.amount_in > 0
        assert result.amount_out_minimum == int(Decimal("0.1") * Decimal(10**18))

    def test_native_input_skips_approve(self, adapter):
        result = adapter.swap_exact_output("ETH", "USDC", Decimal("3000"))
        assert result.success is True
        assert len(result.transactions) == 1
        assert result.transactions[0].value > 0

    def test_unknown_input(self, adapter):
        result = adapter.swap_exact_output("UNKNOWN_FOO", "USDC", Decimal("1"))
        assert result.success is False

    def test_unknown_output(self, adapter):
        result = adapter.swap_exact_output("USDC", "UNKNOWN_BAR", Decimal("1"))
        assert result.success is False

    def test_zero_slippage(self, adapter):
        result = adapter.swap_exact_output(
            "USDC", "WETH", Decimal("0.1"), slippage_bps=0,
        )
        assert result.success is True
        # amount_in_maximum == quote.amount_in when slippage=0
        assert result.amount_in == result.quote.amount_in


# -------------------------------------------------------------------------
# open_lp_position
# -------------------------------------------------------------------------


class TestOpenLPPosition:
    def test_basic(self, adapter):
        result = adapter.open_lp_position(
            "USDC", "WETH",
            Decimal("1000"), Decimal("0.5"),
        )
        assert result.success is True
        # 2 approves + mint
        assert len(result.transactions) == 3
        # last tx should be mint
        assert result.transactions[-1].tx_type == "mint"
        assert result.position_info["fee_tier"] == DEFAULT_FEE_TIER

    def test_unknown_token0(self, adapter):
        # _resolve_token raises -> caught by outer try/except -> LPResult error
        result = adapter.open_lp_position(
            "UNKNOWN_FOO", "USDC", Decimal("1"), Decimal("1"),
        )
        assert result.success is False
        assert result.error is not None

    def test_unknown_token1(self, adapter):
        result = adapter.open_lp_position(
            "USDC", "UNKNOWN_BAR", Decimal("1"), Decimal("1"),
        )
        assert result.success is False
        assert result.error is not None

    def test_token_swap_flow(self, adapter):
        """When user-provided (token0, token1) is NOT in pool-sorted order,
        the adapter must rewrite both addresses AND amounts to sorted order.

        WETH (0x82aF…) < USDC (0xaf88…) by address, so the canonical
        pool order is (WETH, USDC). We pass (USDC, WETH) — adapter must swap.
        """
        # Sanity: pool ordering is WETH first by raw address comparison
        assert WETH_ARB.lower() < USDC_ARB.lower()

        result = adapter.open_lp_position(
            "USDC", "WETH",  # unsorted input
            Decimal("1000"), Decimal("0.5"),  # 1000 USDC, 0.5 WETH
        )
        assert result.success is True

        info = result.position_info
        # Sorted token0 = WETH, token1 = USDC (case-insensitive match — adapter
        # may emit checksummed addresses)
        assert info["token0"].lower() == WETH_ARB.lower()
        assert info["token1"].lower() == USDC_ARB.lower()

        # Amounts swapped to match sorted order: amount0 = 0.5 WETH (18 dec),
        # amount1 = 1000 USDC (6 dec).
        assert int(info["amount0"]) == int(Decimal("0.5") * Decimal(10**18))
        assert int(info["amount1"]) == int(Decimal("1000") * Decimal(10**6))

    def test_explicit_fee_and_recipient(self, adapter):
        custom_recipient = "0x8888888888888888888888888888888888888888"
        result = adapter.open_lp_position(
            "USDC", "WETH", Decimal("1000"), Decimal("0.5"),
            fee_tier=500, slippage_bps=10, recipient=custom_recipient,
        )
        assert result.success is True
        assert result.position_info["fee_tier"] == 500

    def test_zero_slippage(self, adapter):
        result = adapter.open_lp_position(
            "USDC", "WETH", Decimal("1000"), Decimal("0.5"),
            slippage_bps=0,
        )
        assert result.success is True


# -------------------------------------------------------------------------
# close_lp_position
# -------------------------------------------------------------------------


class TestCloseLPPosition:
    def test_basic(self, adapter):
        result = adapter.close_lp_position(token_id=42, liquidity=10**12)
        assert result.success is True
        # 2 transactions: decrease + collect
        assert len(result.transactions) == 2
        assert result.transactions[0].tx_type == "decrease_liquidity"
        assert result.transactions[1].tx_type == "collect"
        assert result.position_info["token_id"] == 42

    def test_explicit_recipient(self, adapter):
        custom = "0x7777777777777777777777777777777777777777"
        result = adapter.close_lp_position(
            token_id=10, liquidity=10**8, recipient=custom,
        )
        assert result.success is True


# -------------------------------------------------------------------------
# Dataclass to_dict (uncovered branches)
# -------------------------------------------------------------------------


class TestDataclassToDict:
    def test_swap_quote_to_dict(self):
        q = SwapQuote(
            token_in=USDC_ARB, token_out=WETH_ARB,
            amount_in=10**6, amount_out=10**18, fee_tier=3000,
        )
        d = q.to_dict()
        assert d["fee_tier"] == 3000
        assert d["amount_in"] == str(10**6)

    def test_swap_result_to_dict(self):
        r = SwapResult(success=True, amount_in=10**6, amount_out_minimum=10**18)
        d = r.to_dict()
        assert d["success"] is True
        assert d["amount_in"] == str(10**6)

    def test_swap_result_to_dict_with_quote_and_tx(self):
        q = SwapQuote(token_in="a", token_out="b", amount_in=1, amount_out=2, fee_tier=3000)
        tx = TransactionData(to="0x", value=0, data="0x", gas_estimate=1, description="x")
        r = SwapResult(success=True, transactions=[tx], quote=q)
        d = r.to_dict()
        assert d["quote"]["fee_tier"] == 3000
        assert len(d["transactions"]) == 1

    def test_transaction_data_to_dict(self):
        tx = TransactionData(to="0xabc", value=10, data="0xdata", gas_estimate=100, description="hi")
        d = tx.to_dict()
        assert d["to"] == "0xabc"
        assert d["value"] == "10"

    def test_lp_result_to_dict(self):
        r = LPResult(success=True, gas_estimate=12345, position_info={"x": 1})
        d = r.to_dict()
        assert d["success"] is True
        assert d["gas_estimate"] == 12345
        assert d["position_info"] == {"x": 1}


# -------------------------------------------------------------------------
# SwapType enum
# -------------------------------------------------------------------------


class TestSwapTypeEnum:
    def test_values(self):
        assert SwapType.EXACT_INPUT.value == "EXACT_INPUT"
        assert SwapType.EXACT_OUTPUT.value == "EXACT_OUTPUT"
