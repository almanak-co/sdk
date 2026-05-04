"""Coverage tests for PancakeSwapV3Adapter validation, error paths, and swap construction.

These tests target the uncovered branches in adapter.py:
- ``PancakeSwapV3Config.__post_init__`` validation errors (chain, wallet, slippage,
  fee tier, missing price_provider)
- ``PancakeSwapV3Adapter.__init__`` placeholder vs explicit price-provider paths
- ``swap_exact_input`` and ``swap_exact_output`` error paths (unknown token, invalid
  fee tier, generic exception)
- Slippage helpers ``_calculate_min_output`` / ``_calculate_max_input`` happy paths
  and price-availability / zero-price guards
- Static padding helpers
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.framework.connectors.pancakeswap_v3.adapter import (
    EXACT_INPUT_SINGLE_SELECTOR,
    EXACT_OUTPUT_SINGLE_SELECTOR,
    FEE_TIERS,
    PANCAKESWAP_V3_ADDRESSES,
    PancakeSwapV3Adapter,
    PancakeSwapV3Config,
    TransactionResult,
)
from almanak.framework.data.tokens.exceptions import TokenResolutionError
from almanak.framework.data.tokens.models import ResolvedToken

WALLET = "0x1234567890123456789012345678901234567890"
USDT_BSC = "0x55d398326f99059fF775485246999027B3197955"
WBNB_BSC = "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _resolved(symbol: str, address: str, decimals: int = 18, chain: str = "bsc", chain_id: int = 56):
    return ResolvedToken(
        symbol=symbol,
        address=address,
        decimals=decimals,
        chain=chain,
        chain_id=chain_id,
    )


def _resolver_for_pair(token_in_sym: str, token_in_addr: str, decimals_in: int,
                       token_out_sym: str, token_out_addr: str, decimals_out: int,
                       chain: str = "bsc", chain_id: int = 56) -> MagicMock:
    """Build a resolver that returns the right ResolvedToken per call.

    Production code looks up by symbol AND address (case variants). Map all
    forms so the mock resolves uniformly regardless of call order.
    """
    by_key: dict[str, ResolvedToken] = {}
    in_tok = _resolved(token_in_sym, token_in_addr, decimals_in, chain, chain_id)
    out_tok = _resolved(token_out_sym, token_out_addr, decimals_out, chain, chain_id)
    for key in (token_in_sym, token_in_sym.upper(), token_in_sym.lower(),
                token_in_addr, token_in_addr.lower(), token_in_addr.upper()):
        by_key[key] = in_tok
    for key in (token_out_sym, token_out_sym.upper(), token_out_sym.lower(),
                token_out_addr, token_out_addr.lower(), token_out_addr.upper()):
        by_key[key] = out_tok

    resolver = MagicMock()

    def _resolve(token: str, _chain: str):
        if token in by_key:
            return by_key[token]
        raise TokenResolutionError(token=token, chain=_chain, reason="unknown")

    resolver.resolve.side_effect = _resolve
    return resolver


# ---------------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------------


class TestPancakeSwapV3ConfigValidation:
    def test_alias_bnb_is_normalized_to_bsc(self):
        """`bnb` should be mapped to canonical `bsc` via resolve_chain_name."""
        cfg = PancakeSwapV3Config(
            chain="bnb",
            wallet_address=WALLET,
            allow_placeholder_prices=True,
        )
        assert cfg.chain == "bsc"

    def test_invalid_chain_raises_value_error(self):
        with pytest.raises(ValueError, match="Invalid chain"):
            PancakeSwapV3Config(
                chain="some-fictional-chain",
                wallet_address=WALLET,
                allow_placeholder_prices=True,
            )

    def test_invalid_wallet_no_0x_prefix_raises(self):
        with pytest.raises(ValueError, match="Invalid wallet address"):
            PancakeSwapV3Config(
                chain="bsc",
                wallet_address="ZZZ" + "1" * 39,  # no 0x
                allow_placeholder_prices=True,
            )

    def test_invalid_wallet_wrong_length_raises(self):
        with pytest.raises(ValueError, match="Invalid wallet address"):
            PancakeSwapV3Config(
                chain="bsc",
                wallet_address="0x123",
                allow_placeholder_prices=True,
            )

    def test_negative_slippage_raises(self):
        with pytest.raises(ValueError, match="Invalid slippage"):
            PancakeSwapV3Config(
                chain="bsc",
                wallet_address=WALLET,
                default_slippage_bps=-1,
                allow_placeholder_prices=True,
            )

    def test_excessive_slippage_raises(self):
        with pytest.raises(ValueError, match="Invalid slippage"):
            PancakeSwapV3Config(
                chain="bsc",
                wallet_address=WALLET,
                default_slippage_bps=10001,
                allow_placeholder_prices=True,
            )

    def test_invalid_fee_tier_raises(self):
        with pytest.raises(ValueError, match="Invalid fee tier"):
            PancakeSwapV3Config(
                chain="bsc",
                wallet_address=WALLET,
                default_fee_tier=300,  # not in {100, 500, 2500, 10000}
                allow_placeholder_prices=True,
            )

    def test_no_price_provider_raises_when_placeholders_disallowed(self):
        with pytest.raises(ValueError, match="requires price_provider"):
            PancakeSwapV3Config(
                chain="bsc",
                wallet_address=WALLET,
                allow_placeholder_prices=False,
            )

    def test_explicit_price_provider_accepted(self):
        cfg = PancakeSwapV3Config(
            chain="bsc",
            wallet_address=WALLET,
            price_provider={"USDT": Decimal("1"), "WBNB": Decimal("700")},
        )
        assert cfg.price_provider is not None
        assert cfg.price_provider["WBNB"] == Decimal("700")


# ---------------------------------------------------------------------------
# Adapter init paths
# ---------------------------------------------------------------------------


class TestAdapterInit:
    def test_init_with_explicit_price_provider_sets_using_placeholders_false(self):
        cfg = PancakeSwapV3Config(
            chain="bsc",
            wallet_address=WALLET,
            price_provider={"USDT": Decimal("1"), "WBNB": Decimal("700")},
        )
        adapter = PancakeSwapV3Adapter(cfg, token_resolver=MagicMock())
        assert adapter._using_placeholders is False
        assert adapter._price_provider == {"USDT": Decimal("1"), "WBNB": Decimal("700")}

    def test_init_with_placeholders_loads_default_price_table(self):
        cfg = PancakeSwapV3Config(
            chain="bsc",
            wallet_address=WALLET,
            allow_placeholder_prices=True,
        )
        adapter = PancakeSwapV3Adapter(cfg, token_resolver=MagicMock())
        assert adapter._using_placeholders is True
        # Spot-check the placeholder table
        assert adapter._price_provider["WBNB"] == Decimal("300")
        assert adapter._price_provider["USDT"] == Decimal("1")
        assert adapter._price_provider["CAKE"] == Decimal("2.50")

    def test_addresses_loaded_for_chain(self):
        cfg = PancakeSwapV3Config(
            chain="bsc",
            wallet_address=WALLET,
            allow_placeholder_prices=True,
        )
        adapter = PancakeSwapV3Adapter(cfg, token_resolver=MagicMock())
        # All three addresses should map to the canonical PancakeSwap V3 router/factory/quoter
        expected = PANCAKESWAP_V3_ADDRESSES["bsc"]
        assert adapter.swap_router_address == expected["swap_router"]
        assert adapter.factory_address == expected["factory"]
        assert adapter.quoter_address == expected["quoter"]


# ---------------------------------------------------------------------------
# swap_exact_input — error & happy paths
# ---------------------------------------------------------------------------


class TestSwapExactInputBranches:
    def test_unknown_token_in_returns_failure(self):
        cfg = PancakeSwapV3Config(
            chain="bsc", wallet_address=WALLET, allow_placeholder_prices=True
        )
        resolver = MagicMock()
        resolver.resolve.side_effect = TokenResolutionError(
            token="UNKNOWN", chain="bsc", reason="unknown"
        )
        adapter = PancakeSwapV3Adapter(cfg, token_resolver=resolver)
        result = adapter.swap_exact_input(
            token_in="UNKNOWN",
            token_out="WBNB",
            amount_in=Decimal("1"),
            amount_out_min=Decimal("0.001"),
        )
        assert result.success is False
        # _resolve_token raises -> caught by outer try/except -> error str contains
        # the wrapped TokenResolutionError reason.
        assert result.error and "UNKNOWN" in result.error

    def test_invalid_fee_tier_returns_failure(self):
        cfg = PancakeSwapV3Config(
            chain="bsc", wallet_address=WALLET, allow_placeholder_prices=True
        )
        resolver = _resolver_for_pair(
            "USDT", USDT_BSC, 18, "WBNB", WBNB_BSC, 18,
        )
        adapter = PancakeSwapV3Adapter(cfg, token_resolver=resolver)
        result = adapter.swap_exact_input(
            token_in="USDT",
            token_out="WBNB",
            amount_in=Decimal("1"),
            amount_out_min=Decimal("0.001"),
            fee_tier=42,  # not in FEE_TIERS
        )
        assert result.success is False
        assert result.error and "Invalid fee tier" in result.error

    def test_happy_path_explicit_amount_out_min_builds_calldata(self):
        cfg = PancakeSwapV3Config(
            chain="bsc", wallet_address=WALLET, allow_placeholder_prices=True
        )
        resolver = _resolver_for_pair(
            "USDT", USDT_BSC, 18, "WBNB", WBNB_BSC, 18,
        )
        adapter = PancakeSwapV3Adapter(cfg, token_resolver=resolver)
        result = adapter.swap_exact_input(
            token_in="USDT",
            token_out="WBNB",
            amount_in=Decimal("100"),
            amount_out_min=Decimal("0.1"),
            fee_tier=500,
            recipient="0x" + "ab" * 20,
            deadline=1234567890,
        )
        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["value"] == 0
        # Selector first
        assert result.tx_data["data"].startswith(EXACT_INPUT_SINGLE_SELECTOR)
        # to == swap_router
        assert result.tx_data["to"] == PANCAKESWAP_V3_ADDRESSES["bsc"]["swap_router"]

    def test_happy_path_uses_default_fee_tier_and_recipient(self):
        cfg = PancakeSwapV3Config(
            chain="bsc",
            wallet_address=WALLET,
            allow_placeholder_prices=True,
            default_fee_tier=2500,
        )
        resolver = _resolver_for_pair(
            "USDT", USDT_BSC, 18, "WBNB", WBNB_BSC, 18,
        )
        adapter = PancakeSwapV3Adapter(cfg, token_resolver=resolver)
        result = adapter.swap_exact_input(
            token_in="USDT",
            token_out="WBNB",
            amount_in=Decimal("10"),
            amount_out_min=Decimal("0.001"),
        )
        assert result.success is True
        # The fee field is at offset 4 + 32*2 = 68 hex chars after selector start
        # (selector = 0x + 8 = 10 chars). selector + addr_in(64) + addr_out(64) + fee(64)
        data = result.tx_data["data"]
        # 10 chars selector, 64 chars in_addr, 64 chars out_addr -> fee at [10+128:10+192]
        fee_hex = data[10 + 128: 10 + 192]
        assert int(fee_hex, 16) == 2500

    def test_swap_exact_input_handles_unknown_input_token_via_short_circuit(self):
        """If _resolve_token returns a non-None falsy via odd input -> path covered.

        We exercise the 'token_in_addr is None' branch by patching _resolve_token.
        """
        cfg = PancakeSwapV3Config(
            chain="bsc", wallet_address=WALLET, allow_placeholder_prices=True
        )
        adapter = PancakeSwapV3Adapter(cfg, token_resolver=MagicMock())
        adapter._resolve_token = MagicMock(side_effect=[None, "0x" + "11" * 20])  # type: ignore[method-assign]
        result = adapter.swap_exact_input(
            token_in="X",
            token_out="Y",
            amount_in=Decimal("1"),
            amount_out_min=Decimal("0"),
        )
        assert result.success is False
        assert result.error and "Unknown input token" in result.error

    def test_swap_exact_input_handles_unknown_output_token_via_short_circuit(self):
        cfg = PancakeSwapV3Config(
            chain="bsc", wallet_address=WALLET, allow_placeholder_prices=True
        )
        adapter = PancakeSwapV3Adapter(cfg, token_resolver=MagicMock())
        adapter._resolve_token = MagicMock(side_effect=["0x" + "11" * 20, None])  # type: ignore[method-assign]
        result = adapter.swap_exact_input(
            token_in="X",
            token_out="Y",
            amount_in=Decimal("1"),
            amount_out_min=Decimal("0"),
        )
        assert result.success is False
        assert result.error and "Unknown output token" in result.error

    def test_swap_exact_input_uses_calculator_when_amount_out_min_omitted(self):
        cfg = PancakeSwapV3Config(
            chain="bsc",
            wallet_address=WALLET,
            price_provider={"USDT": Decimal("1"), "WBNB": Decimal("700")},
        )
        resolver = _resolver_for_pair(
            "USDT", USDT_BSC, 18, "WBNB", WBNB_BSC, 18,
        )
        adapter = PancakeSwapV3Adapter(cfg, token_resolver=resolver)
        # No amount_out_min provided -> _calculate_min_output is exercised
        result = adapter.swap_exact_input(
            token_in="USDT",
            token_out="WBNB",
            amount_in=Decimal("700"),
            fee_tier=500,
        )
        assert result.success is True
        # Min output should be > 0 and < 1 WBNB after fee + slippage
        data = result.tx_data["data"]
        # amount_out_min_wei is at offset selector(10) + 64*5 = 330..394
        amt_out_min_hex = data[10 + 64 * 5: 10 + 64 * 6]
        amt_out_min_wei = int(amt_out_min_hex, 16)
        assert 0 < amt_out_min_wei < 10**18


# ---------------------------------------------------------------------------
# swap_exact_output — error & happy paths
# ---------------------------------------------------------------------------


class TestSwapExactOutputBranches:
    def test_unknown_token_in_returns_failure_via_short_circuit(self):
        cfg = PancakeSwapV3Config(
            chain="bsc", wallet_address=WALLET, allow_placeholder_prices=True
        )
        adapter = PancakeSwapV3Adapter(cfg, token_resolver=MagicMock())
        adapter._resolve_token = MagicMock(side_effect=[None, "0x" + "11" * 20])  # type: ignore[method-assign]
        result = adapter.swap_exact_output(
            token_in="X",
            token_out="Y",
            amount_out=Decimal("0.1"),
            amount_in_max=Decimal("100"),
        )
        assert result.success is False
        assert result.error and "Unknown input token" in result.error

    def test_unknown_token_out_returns_failure_via_short_circuit(self):
        cfg = PancakeSwapV3Config(
            chain="bsc", wallet_address=WALLET, allow_placeholder_prices=True
        )
        adapter = PancakeSwapV3Adapter(cfg, token_resolver=MagicMock())
        adapter._resolve_token = MagicMock(side_effect=["0x" + "11" * 20, None])  # type: ignore[method-assign]
        result = adapter.swap_exact_output(
            token_in="X",
            token_out="Y",
            amount_out=Decimal("0.1"),
            amount_in_max=Decimal("100"),
        )
        assert result.success is False
        assert result.error and "Unknown output token" in result.error

    def test_unknown_token_out_via_resolver_error_returns_failure(self):
        cfg = PancakeSwapV3Config(
            chain="bsc", wallet_address=WALLET, allow_placeholder_prices=True
        )
        resolver = MagicMock()
        resolver.resolve.side_effect = TokenResolutionError(
            token="UNKNOWN", chain="bsc", reason="unknown"
        )
        adapter = PancakeSwapV3Adapter(cfg, token_resolver=resolver)
        result = adapter.swap_exact_output(
            token_in="UNKNOWN",
            token_out="WBNB",
            amount_out=Decimal("0.1"),
            amount_in_max=Decimal("100"),
        )
        assert result.success is False

    def test_invalid_fee_tier_returns_failure(self):
        cfg = PancakeSwapV3Config(
            chain="bsc", wallet_address=WALLET, allow_placeholder_prices=True
        )
        resolver = _resolver_for_pair(
            "USDT", USDT_BSC, 18, "WBNB", WBNB_BSC, 18,
        )
        adapter = PancakeSwapV3Adapter(cfg, token_resolver=resolver)
        result = adapter.swap_exact_output(
            token_in="USDT",
            token_out="WBNB",
            amount_out=Decimal("0.1"),
            amount_in_max=Decimal("100"),
            fee_tier=99,
        )
        assert result.success is False
        assert result.error and "Invalid fee tier" in result.error

    def test_happy_path_explicit_amount_in_max_builds_calldata(self):
        cfg = PancakeSwapV3Config(
            chain="bsc", wallet_address=WALLET, allow_placeholder_prices=True
        )
        resolver = _resolver_for_pair(
            "USDT", USDT_BSC, 18, "WBNB", WBNB_BSC, 18,
        )
        adapter = PancakeSwapV3Adapter(cfg, token_resolver=resolver)
        result = adapter.swap_exact_output(
            token_in="USDT",
            token_out="WBNB",
            amount_out=Decimal("0.1"),
            amount_in_max=Decimal("100"),
            fee_tier=10000,
            recipient="0x" + "cd" * 20,
            deadline=999,
        )
        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["data"].startswith(EXACT_OUTPUT_SINGLE_SELECTOR)
        assert result.tx_data["to"] == PANCAKESWAP_V3_ADDRESSES["bsc"]["swap_router"]

    def test_happy_path_uses_calculator_when_amount_in_max_omitted(self):
        cfg = PancakeSwapV3Config(
            chain="bsc",
            wallet_address=WALLET,
            price_provider={"USDT": Decimal("1"), "WBNB": Decimal("700")},
        )
        resolver = _resolver_for_pair(
            "USDT", USDT_BSC, 18, "WBNB", WBNB_BSC, 18,
        )
        adapter = PancakeSwapV3Adapter(cfg, token_resolver=resolver)
        result = adapter.swap_exact_output(
            token_in="USDT",
            token_out="WBNB",
            amount_out=Decimal("1"),
            fee_tier=500,
        )
        assert result.success is True
        # amount_in_max_wei is at offset selector(10) + 64*5
        data = result.tx_data["data"]
        amt_in_max_hex = data[10 + 64 * 5: 10 + 64 * 6]
        amt_in_max_wei = int(amt_in_max_hex, 16)
        # ~ 700 USDT (18-decimals on BSC) plus fee/slippage
        assert amt_in_max_wei > 700 * 10**18


# ---------------------------------------------------------------------------
# Slippage helpers
# ---------------------------------------------------------------------------


class TestSlippageHelpers:
    def _make_adapter(self, prices: dict[str, Decimal]) -> PancakeSwapV3Adapter:
        cfg = PancakeSwapV3Config(
            chain="bsc", wallet_address=WALLET, price_provider=prices
        )
        resolver = _resolver_for_pair(
            "USDT", USDT_BSC, 18, "WBNB", WBNB_BSC, 18,
        )
        return PancakeSwapV3Adapter(cfg, token_resolver=resolver)

    def test_calculate_min_output_missing_token_in_price_raises(self):
        adapter = self._make_adapter({"WBNB": Decimal("700")})
        with pytest.raises(ValueError, match="Price data not available"):
            adapter._calculate_min_output(
                token_in="USDT",
                token_out="WBNB",
                amount_in=Decimal("1"),
                slippage_bps=50,
                fee_tier=500,
            )

    def test_calculate_min_output_missing_token_out_price_raises(self):
        adapter = self._make_adapter({"USDT": Decimal("1")})
        with pytest.raises(ValueError, match="Price data not available"):
            adapter._calculate_min_output(
                token_in="USDT",
                token_out="WBNB",
                amount_in=Decimal("1"),
                slippage_bps=50,
                fee_tier=500,
            )

    def test_calculate_min_output_zero_price_out_raises(self):
        adapter = self._make_adapter({"USDT": Decimal("1"), "WBNB": Decimal("0")})
        with pytest.raises(ValueError, match="Invalid price for WBNB"):
            adapter._calculate_min_output(
                token_in="USDT",
                token_out="WBNB",
                amount_in=Decimal("1"),
                slippage_bps=50,
                fee_tier=500,
            )

    def test_calculate_max_input_missing_token_price_raises(self):
        adapter = self._make_adapter({"WBNB": Decimal("700")})
        with pytest.raises(ValueError, match="Price data not available"):
            adapter._calculate_max_input(
                token_in="USDT",
                token_out="WBNB",
                amount_out=Decimal("1"),
                slippage_bps=50,
                fee_tier=500,
            )

    def test_calculate_max_input_zero_price_in_raises(self):
        adapter = self._make_adapter({"USDT": Decimal("0"), "WBNB": Decimal("700")})
        with pytest.raises(ValueError, match="Invalid price for USDT"):
            adapter._calculate_max_input(
                token_in="USDT",
                token_out="WBNB",
                amount_out=Decimal("1"),
                slippage_bps=50,
                fee_tier=500,
            )

    def test_calculate_min_output_address_input_uses_address_lookup(self):
        """When token_in is a 0x-prefixed address, the price lookup uses the
        address verbatim (no upper-casing). Price provider must be keyed
        accordingly."""
        adapter = self._make_adapter({USDT_BSC: Decimal("1"), "WBNB": Decimal("700")})
        out = adapter._calculate_min_output(
            token_in=USDT_BSC,
            token_out="WBNB",
            amount_in=Decimal("700"),
            slippage_bps=50,
            fee_tier=500,
        )
        assert out > 0


# ---------------------------------------------------------------------------
# Static helpers
# ---------------------------------------------------------------------------


class TestStaticHelpers:
    def test_pad_address_strips_0x_and_lowercases(self):
        padded = PancakeSwapV3Adapter._pad_address("0xABCDEF" + "00" * 17)
        assert len(padded) == 64
        # Lowercase
        assert padded == padded.lower()
        # Pad with zeros on left (right-aligned)
        assert padded.endswith("abcdef" + "00" * 17)

    def test_pad_uint256_zero(self):
        assert PancakeSwapV3Adapter._pad_uint256(0) == "0" * 64

    def test_pad_uint256_large(self):
        v = 2**256 - 1
        # Padding should still be 64 chars (no overflow expected for uint256)
        padded = PancakeSwapV3Adapter._pad_uint256(v)
        assert len(padded) == 64
        assert int(padded, 16) == v


# ---------------------------------------------------------------------------
# TransactionResult dataclass
# ---------------------------------------------------------------------------


class TestTransactionResult:
    def test_default_construction(self):
        r = TransactionResult(success=True)
        assert r.success is True
        assert r.tx_data is None
        assert r.gas_estimate == 0
        assert r.description == ""
        assert r.error is None

    def test_failure_with_error(self):
        r = TransactionResult(success=False, error="bad")
        assert r.success is False
        assert r.error == "bad"


# ---------------------------------------------------------------------------
# FEE_TIERS sanity
# ---------------------------------------------------------------------------


def test_fee_tiers_known_set():
    assert FEE_TIERS == {100, 500, 2500, 10000}
