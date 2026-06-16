"""Tests for Uniswap V4 Adapter."""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.connectors.uniswap_v4.adapter import (
    UniswapV4Adapter,
    UniswapV4Config,
)
from almanak.connectors.uniswap_v4.sdk import SwapQuote
from almanak.framework.data.tokens import TokenNotFoundError

# Known tokens for mock resolver
_KNOWN_TOKENS = {
    "0xaf88d065e77c8cc2239327c5edb3a432268e5831": ("0xaf88d065e77c8cC2239327C5EDb3A432268e5831", 6),
    "0x82af49447d8a07e3bd95bd0d56f35241523fbab1": ("0x82aF49447D8a07e3bd95BD0d56f35241523fBab1", 18),
    "0x0000000000000000000000000000000000000000": ("0x0000000000000000000000000000000000000000", 18),
}


def _make_resolver():
    """Create a mock token resolver that knows common Arbitrum tokens."""
    resolver = MagicMock()

    def _resolve(token, chain):
        key = token.lower()
        if key in _KNOWN_TOKENS:
            m = MagicMock()
            m.address, m.decimals = _KNOWN_TOKENS[key]
            return m
        raise ValueError(f"Unknown token {token}")

    def _resolve_for_swap(token, chain):
        return _resolve(token, chain)

    resolver.resolve.side_effect = _resolve
    resolver.resolve_for_swap.side_effect = _resolve_for_swap
    return resolver


class TestAdapterInit:
    def test_init_with_chain(self):
        adapter = UniswapV4Adapter(chain="arbitrum")
        assert adapter.chain == "arbitrum"

    def test_init_with_config(self):
        config = UniswapV4Config(
            chain="ethereum",
            wallet_address="0x1234567890123456789012345678901234567890",
            default_fee_tier=500,
        )
        adapter = UniswapV4Adapter(config=config)
        assert adapter.chain == "ethereum"
        assert adapter.default_fee_tier == 500

    def test_init_unsupported_chain(self):
        with pytest.raises(ValueError, match="not supported"):
            UniswapV4Adapter(chain="fantom")

    def test_init_no_args(self):
        with pytest.raises(ValueError, match="Either chain or config"):
            UniswapV4Adapter()


_TEST_WALLET = "0x1234567890123456789012345678901234567890"


class TestSwapExactInput:
    # USDC (6 dec) → WETH (18 dec) — VIB-3875 requires price_ratio for cross-decimal quotes.
    # 1 USDC ≈ 0.0003 ETH, so price_ratio (token_out per token_in) is Decimal("0.0003").
    _USDC_TO_WETH_PRICE_RATIO = Decimal("0.0003")
    _USDC_ADDR = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
    _WETH_ADDR = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"

    def test_basic_swap(self):
        config = UniswapV4Config(chain="arbitrum", wallet_address=_TEST_WALLET)
        adapter = UniswapV4Adapter(config=config, token_resolver=_make_resolver())
        result = adapter.swap_exact_input(
            token_in=self._USDC_ADDR,
            token_out=self._WETH_ADDR,
            amount_in=Decimal("1000"),
            slippage_bps=50,
            price_ratio=self._USDC_TO_WETH_PRICE_RATIO,
        )
        assert result.success is True
        assert len(result.transactions) == 3  # approve Permit2 + Permit2 approve router + swap
        assert result.amount_in > 0
        assert result.amount_out_minimum > 0

    def test_no_wallet_address_raises(self):
        adapter = UniswapV4Adapter(chain="arbitrum", token_resolver=_make_resolver())
        with pytest.raises(ValueError, match="wallet_address must be set"):
            adapter.swap_exact_input(
                token_in=self._USDC_ADDR,
                token_out=self._WETH_ADDR,
                amount_in=Decimal("1000"),
                # price_ratio supplied so the wallet_address check is what fires (not VIB-3875).
                price_ratio=self._USDC_TO_WETH_PRICE_RATIO,
            )

    def test_native_eth_no_approve(self):
        config = UniswapV4Config(chain="arbitrum", wallet_address=_TEST_WALLET)
        adapter = UniswapV4Adapter(config=config, token_resolver=_make_resolver())
        result = adapter.swap_exact_input(
            token_in="0x0000000000000000000000000000000000000000",
            token_out=self._USDC_ADDR,
            amount_in=Decimal("1"),
            # ETH (18 dec) → USDC (6 dec): price_ratio = USDC per ETH ≈ 3000.
            price_ratio=Decimal("3000"),
        )
        assert result.success is True
        # No approve needed for native ETH - just swap
        assert len(result.transactions) == 1

    def test_slippage_applied(self):
        config = UniswapV4Config(chain="arbitrum", wallet_address=_TEST_WALLET)
        adapter = UniswapV4Adapter(config=config, token_resolver=_make_resolver())
        result = adapter.swap_exact_input(
            token_in=self._USDC_ADDR,
            token_out=self._WETH_ADDR,
            amount_in=Decimal("1000"),
            slippage_bps=100,  # 1%
            price_ratio=self._USDC_TO_WETH_PRICE_RATIO,
        )
        assert result.success is True
        # amount_out_minimum should be ~99% of quote output
        assert result.amount_out_minimum > 0

    def _make_local_quote(self, amount_out: int = 300_000_000_000_000_000) -> SwapQuote:
        return SwapQuote(
            amount_in=1_000_000_000,
            amount_out=amount_out,
            fee_tier=3000,
            token_in=self._USDC_ADDR,
            token_out=self._WETH_ADDR,
        )

    def test_connected_online_quote_failure_fails_closed(self):
        """VIB-2058 C1: connected + online executable-quote failure MUST fail closed.

        This is the money-path fix. The prior behaviour silently fell back to the
        theoretical ``get_quote_local`` estimate and built a REAL swap whose
        ``amount_out_minimum`` was backed by a number that may not correspond to any
        on-chain pool — the iter-133 silent-no-op class. Correct behaviour: refuse to
        compile (no transactions, clear error), never fabricate the minOut basis.
        """
        config = UniswapV4Config(
            chain="arbitrum",
            wallet_address=_TEST_WALLET,
            rpc_url="https://arb.example.invalid",
        )
        adapter = UniswapV4Adapter(config=config, token_resolver=_make_resolver())
        adapter._sdk.get_quote = MagicMock(side_effect=ValueError("quoter unavailable"))
        adapter._sdk.get_quote_local = MagicMock(return_value=self._make_local_quote())

        result = adapter.swap_exact_input(
            token_in=self._USDC_ADDR,
            token_out=self._WETH_ADDR,
            amount_in=Decimal("1000"),
            slippage_bps=50,
            price_ratio=self._USDC_TO_WETH_PRICE_RATIO,
        )

        assert result.success is False
        assert result.transactions == []
        assert result.amount_out_minimum == 0
        assert "unavailable" in (result.error or "").lower()
        adapter._sdk.get_quote.assert_called_once()
        # The fabricating fallback must NOT have been consulted on the online path.
        adapter._sdk.get_quote_local.assert_not_called()

    def test_offline_mode_quote_failure_uses_local_estimate(self):
        """VIB-2058 C3: in a non-broadcasting compile (permission discovery /
        placeholders) a failed executable quote MAY degrade to the local estimate —
        nothing is signed or sent, so the theoretical number is harmless there.
        """
        config = UniswapV4Config(
            chain="arbitrum",
            wallet_address=_TEST_WALLET,
            rpc_url="https://arb.example.invalid",
        )
        adapter = UniswapV4Adapter(config=config, token_resolver=_make_resolver())
        local_quote = self._make_local_quote()
        adapter._sdk.get_quote = MagicMock(side_effect=ValueError("quoter unavailable"))
        adapter._sdk.get_quote_local = MagicMock(return_value=local_quote)

        result = adapter.swap_exact_input(
            token_in=self._USDC_ADDR,
            token_out=self._WETH_ADDR,
            amount_in=Decimal("1000"),
            slippage_bps=50,
            price_ratio=self._USDC_TO_WETH_PRICE_RATIO,
            offline_mode=True,
        )

        assert result.success is True
        assert result.amount_out_quoted == local_quote.amount_out
        assert result.quote_source == "local_estimate"
        adapter._sdk.get_quote.assert_called_once()
        adapter._sdk.get_quote_local.assert_called_once()

    def test_connected_success_uses_executable_quote_with_provenance(self):
        """VIB-2058: a successful executable quote backs the minOut and is stamped
        ``quote_source="onchain_quoter"``; the local estimate is never consulted.
        """
        config = UniswapV4Config(
            chain="arbitrum",
            wallet_address=_TEST_WALLET,
            rpc_url="https://arb.example.invalid",
        )
        adapter = UniswapV4Adapter(config=config, token_resolver=_make_resolver())
        onchain_quote = self._make_local_quote(amount_out=299_000_000_000_000_000)
        adapter._sdk.get_quote = MagicMock(return_value=onchain_quote)
        adapter._sdk.get_quote_local = MagicMock(side_effect=AssertionError("must not be called"))

        result = adapter.swap_exact_input(
            token_in=self._USDC_ADDR,
            token_out=self._WETH_ADDR,
            amount_in=Decimal("1000"),
            slippage_bps=50,
            price_ratio=self._USDC_TO_WETH_PRICE_RATIO,
        )

        assert result.success is True
        assert result.amount_out_quoted == onchain_quote.amount_out
        assert result.quote_source == "onchain_quoter"
        adapter._sdk.get_quote_local.assert_not_called()

    def test_offline_no_connection_uses_local_estimate(self):
        """VIB-2058 C3: with no gateway and no RPC the local estimate is the designed
        path (the executable quoter is physically unreachable). Provenance reflects it.
        """
        config = UniswapV4Config(chain="arbitrum", wallet_address=_TEST_WALLET)
        adapter = UniswapV4Adapter(config=config, token_resolver=_make_resolver())
        adapter._sdk.get_quote = MagicMock(side_effect=AssertionError("must not be called"))

        result = adapter.swap_exact_input(
            token_in=self._USDC_ADDR,
            token_out=self._WETH_ADDR,
            amount_in=Decimal("1000"),
            slippage_bps=50,
            price_ratio=self._USDC_TO_WETH_PRICE_RATIO,
        )

        assert result.success is True
        assert result.quote_source == "local_estimate"
        adapter._sdk.get_quote.assert_not_called()

    def test_high_price_impact_fails_closed(self):
        """VIB-2058 C2: an executable quote far below the oracle estimate (thin pool)
        exceeds the impact ceiling and MUST fail compilation rather than build a swap
        that would no-op or be sandwiched.
        """
        config = UniswapV4Config(
            chain="arbitrum",
            wallet_address=_TEST_WALLET,
            rpc_url="https://arb.example.invalid",
        )
        adapter = UniswapV4Adapter(config=config, token_resolver=_make_resolver())
        # Oracle implies ~0.3 WETH out for 1000 USDC; quoter returns ~0.15 WETH → 50%
        # impact, well over the default 5% ceiling.
        thin_quote = self._make_local_quote(amount_out=150_000_000_000_000_000)
        adapter._sdk.get_quote = MagicMock(return_value=thin_quote)

        result = adapter.swap_exact_input(
            token_in=self._USDC_ADDR,
            token_out=self._WETH_ADDR,
            amount_in=Decimal("1000"),
            slippage_bps=50,
            price_ratio=self._USDC_TO_WETH_PRICE_RATIO,
        )

        assert result.success is False
        assert result.transactions == []
        assert "price impact" in (result.error or "").lower()

    def test_within_tolerance_price_impact_passes(self):
        """VIB-2058 C2: an executable quote within the impact ceiling compiles."""
        config = UniswapV4Config(
            chain="arbitrum",
            wallet_address=_TEST_WALLET,
            rpc_url="https://arb.example.invalid",
        )
        adapter = UniswapV4Adapter(config=config, token_resolver=_make_resolver())
        # 1000 USDC * 0.0003 = 0.3 WETH oracle; quoter 0.297 WETH → 1% impact < 5%.
        healthy_quote = self._make_local_quote(amount_out=297_000_000_000_000_000)
        adapter._sdk.get_quote = MagicMock(return_value=healthy_quote)

        result = adapter.swap_exact_input(
            token_in=self._USDC_ADDR,
            token_out=self._WETH_ADDR,
            amount_in=Decimal("1000"),
            slippage_bps=50,
            price_ratio=self._USDC_TO_WETH_PRICE_RATIO,
        )

        assert result.success is True
        assert result.quote_source == "onchain_quoter"

    def test_zero_output_onchain_quote_fails_closed(self):
        """VIB-2058: a callable-but-degenerate pool whose Quoter returns amount_out=0
        WITHOUT reverting must fail closed even with no oracle price_ratio — otherwise
        the swap would compile with amount_out_minimum=0 (the silent-no-op corner the
        impact guard cannot see without an oracle).
        """
        config = UniswapV4Config(
            chain="arbitrum",
            wallet_address=_TEST_WALLET,
            rpc_url="https://arb.example.invalid",
        )
        adapter = UniswapV4Adapter(config=config, token_resolver=_make_resolver())
        zero_quote = self._make_local_quote(amount_out=0)
        adapter._sdk.get_quote = MagicMock(return_value=zero_quote)

        # No price_ratio (partial oracle) so the C2 guard is skipped — the zero-output
        # check is what must catch this.
        result = adapter.swap_exact_input(
            token_in=self._USDC_ADDR,
            token_out=self._WETH_ADDR,
            amount_in=Decimal("1000"),
            slippage_bps=50,
            price_ratio=None,
        )

        assert result.success is False
        assert result.transactions == []
        assert "zero output" in (result.error or "").lower()

    def test_onchain_quote_without_oracle_skips_impact_guard(self):
        """VIB-2058: an executable quote with no oracle price_ratio (partial oracle)
        compiles — the executable quote already proved pool existence; depth is left
        unguarded (SKIPPED_NO_ORACLE), mirroring the V3 swap path.
        """
        config = UniswapV4Config(
            chain="arbitrum",
            wallet_address=_TEST_WALLET,
            rpc_url="https://arb.example.invalid",
        )
        adapter = UniswapV4Adapter(config=config, token_resolver=_make_resolver())
        onchain_quote = self._make_local_quote(amount_out=296_000_000_000_000_000)
        adapter._sdk.get_quote = MagicMock(return_value=onchain_quote)
        adapter._sdk.get_quote_local = MagicMock(side_effect=AssertionError("must not be called"))

        result = adapter.swap_exact_input(
            token_in=self._USDC_ADDR,
            token_out=self._WETH_ADDR,
            amount_in=Decimal("1000"),
            slippage_bps=50,
            price_ratio=None,
        )

        assert result.success is True
        assert result.quote_source == "onchain_quoter"
        assert result.amount_out_minimum > 0

    def test_local_anvil_skips_price_impact_guard(self):
        """VIB-2058 C4: on a local Anvil fork the impact guard is skipped (fork pool
        state and live oracle prices are not time-aligned), so an otherwise-failing
        impact still compiles.
        """
        config = UniswapV4Config(
            chain="arbitrum",
            wallet_address=_TEST_WALLET,
            rpc_url="http://127.0.0.1:8545",
        )
        adapter = UniswapV4Adapter(config=config, token_resolver=_make_resolver())
        thin_quote = self._make_local_quote(amount_out=150_000_000_000_000_000)
        adapter._sdk.get_quote = MagicMock(return_value=thin_quote)

        result = adapter.swap_exact_input(
            token_in=self._USDC_ADDR,
            token_out=self._WETH_ADDR,
            amount_in=Decimal("1000"),
            slippage_bps=50,
            price_ratio=self._USDC_TO_WETH_PRICE_RATIO,
        )

        assert result.success is True
        assert result.quote_source == "onchain_quoter"


class TestTokenResolution:
    def test_resolve_native_symbol_for_v4_pool(self):
        """Native symbols remap to address(0) for v4 pool keys, per chain."""
        from almanak.connectors.uniswap_v4.sdk import NATIVE_CURRENCY

        for chain in ("ethereum", "arbitrum", "base"):
            adapter = UniswapV4Adapter(chain=chain)
            addr, dec = adapter._resolve_token("ETH", for_v4_pool=True)
            assert addr == NATIVE_CURRENCY, chain
            assert dec == 18, chain

    def test_foreign_native_not_remapped_for_v4_pool(self):
        """Deliberate behavior change (VIB-4851 A1): the legacy chain-blind set
        {ETH, AVAX, MATIC, BNB} remapped e.g. "MATIC" on ethereum — a real
        ERC-20 there — to address(0). The per-chain gate falls through to the
        resolver instead."""
        matic_erc20 = "0x7D1AfA7B718fb893dB30A3aBc0Cfc608AaCfeBB0"
        resolver = MagicMock()
        resolved = MagicMock()
        resolved.address = matic_erc20
        resolved.decimals = 18
        resolver.resolve_for_swap.return_value = resolved

        adapter = UniswapV4Adapter(chain="ethereum", token_resolver=resolver)
        addr, dec = adapter._resolve_token("MATIC", for_v4_pool=True)
        assert addr == matic_erc20
        assert dec == 18
        resolver.resolve_for_swap.assert_called_once_with("MATIC", "ethereum")

    def test_resolve_by_address(self):
        adapter = UniswapV4Adapter(chain="arbitrum", token_resolver=_make_resolver())
        addr, dec = adapter._resolve_token("0xaf88d065e77c8cC2239327C5EDb3A432268e5831")
        assert addr == "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
        assert dec == 6

    def test_resolve_by_address_without_resolver_raises(self):
        """Raw address without resolver must raise, not fallback to 18 decimals."""
        adapter = UniswapV4Adapter(chain="arbitrum")
        with pytest.raises(TokenNotFoundError):
            adapter._resolve_token("0xaf88d065e77c8cC2239327C5EDb3A432268e5831")

    def test_resolve_by_symbol_fallback(self):
        adapter = UniswapV4Adapter(chain="arbitrum")
        addr, dec = adapter._resolve_token("USDC")
        assert addr.lower() == "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
        assert dec == 6

    def test_resolve_with_token_resolver(self):
        resolver = MagicMock()
        resolved = MagicMock()
        resolved.address = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
        resolved.decimals = 6
        resolver.resolve_for_swap.return_value = resolved

        adapter = UniswapV4Adapter(chain="arbitrum", token_resolver=resolver)
        addr, dec = adapter._resolve_token("USDC")
        assert addr == "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
        assert dec == 6

    def test_resolve_unknown_token_raises_token_not_found(self):
        adapter = UniswapV4Adapter(chain="arbitrum")
        with pytest.raises(TokenNotFoundError):
            adapter._resolve_token("UNKNOWN_TOKEN_XYZ")

    def test_resolver_fallback_preserves_v3_catalogue_addresses(self):
        """VIB-4866 behaviour-preservation: the no-injected-resolver fallback now
        routes through the framework's connector-agnostic token resolver instead
        of importing ``uniswap_v3.UNISWAP_V3_TOKENS``. For every (chain, symbol)
        the OLD fallback could resolve — i.e. present in BOTH ``UNISWAP_V3_TOKENS``
        AND the old hard-coded ``decimals_map`` — the address must resolve
        identically (case-insensitively; EVM addresses are case-insensitive and
        the framework resolver returns lowercase, matching the injected-resolver
        path)."""
        from almanak.connectors.uniswap_v3.addresses import UNISWAP_V3_TOKENS
        from almanak.connectors.uniswap_v4.addresses import UNISWAP_V4

        # The exact decimals the old fallback hard-coded. Symbols outside this
        # map raised "decimals unknown" in the old code, so they are not part of
        # the preserved contract. BSC USDC/USDT are deliberately excluded: the
        # old map hard-coded 6, but those tokens are actually 18-decimal on BSC —
        # the framework resolver returns the correct 18 (a latent-bug fix,
        # asserted separately below).
        old_decimals_map = {
            "USDC": 6, "USDT": 6, "USDC.e": 6, "USDT.e": 6, "WBTC": 8,
            "WETH": 18, "ETH": 18, "DAI": 18, "LINK": 18, "UNI": 18,
            "WAVAX": 18, "AVAX": 18, "WMATIC": 18, "WBNB": 18,
        }  # fmt: skip
        checked = 0
        for chain, toks in UNISWAP_V3_TOKENS.items():
            if chain not in UNISWAP_V4:
                continue  # V4 adapter only constructs on V4-supported chains
            for symbol, old_address in toks.items():
                if symbol not in old_decimals_map:
                    continue  # old fallback raised — not part of the contract
                if chain == "bsc" and symbol in ("USDC", "USDT"):
                    continue  # bugfix asserted below
                adapter = UniswapV4Adapter(chain=chain)
                try:
                    addr, dec = adapter._resolve_token(symbol)
                except TokenNotFoundError:
                    pytest.fail(f"regression: {symbol} on {chain} no longer resolves")
                assert addr.lower() == old_address.lower(), f"{symbol} on {chain}"
                assert dec == old_decimals_map[symbol], f"{symbol} on {chain}"
                checked += 1
        assert checked > 30, f"expected broad coverage, only checked {checked}"

    def test_resolver_fallback_native_symbol_not_auto_wrapped(self):
        """The swap-path fallback (for_v4_pool=False) preserves the old behaviour
        of NOT auto-wrapping native symbols: ETH stays the native sentinel rather
        than resolving to WETH."""
        from almanak.framework.data.tokens.defaults import NATIVE_SENTINEL

        adapter = UniswapV4Adapter(chain="arbitrum")
        addr, dec = adapter._resolve_token("ETH")
        assert addr.lower() == NATIVE_SENTINEL.lower()
        assert dec == 18

    def test_resolver_fallback_corrects_bsc_stable_decimals(self):
        """Latent-bug fix surfaced by VIB-4866: the old hard-coded decimals_map
        returned 6 for BSC USDC/USDT, but both are 18-decimal on BSC. The
        framework resolver returns the correct value."""
        adapter = UniswapV4Adapter(chain="bsc")
        _, usdc_dec = adapter._resolve_token("USDC")
        _, usdt_dec = adapter._resolve_token("USDT")
        assert usdc_dec == 18
        assert usdt_dec == 18

    def test_resolver_fallback_unknown_symbol_raises(self):
        """Unknown symbols still fail-closed with TokenNotFoundError — no
        fabricated 18-decimals on the degraded fallback path."""
        adapter = UniswapV4Adapter(chain="arbitrum")
        with pytest.raises(TokenNotFoundError):
            adapter._resolve_token("FAKECOIN_XYZ")

    def test_adapter_has_no_uniswap_v3_import(self):
        """VIB-4866 / blueprint 22: no cross-connector CONNECTOR_IMPORT from
        uniswap_v3 may remain in the V4 adapter."""
        from pathlib import Path

        import almanak.connectors.uniswap_v4.adapter as v4_adapter

        assert not hasattr(v4_adapter, "UNISWAP_V3_TOKENS")
        source = Path(v4_adapter.__file__).read_text()
        assert "from almanak.connectors.uniswap_v3" not in source

    def test_raw_address_without_resolver_raises(self):
        """Raw addresses without a token_resolver must fail, not assume 18 decimals."""
        adapter = UniswapV4Adapter(chain="arbitrum")
        with pytest.raises(TokenNotFoundError):
            adapter.swap_exact_input(
                token_in="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
                token_out="0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
                amount_in=Decimal("1000"),
            )


class TestCompileSwapIntent:
    # USDC (6 dec) → WETH (18 dec) needs both token prices in the oracle so the
    # adapter can derive ``computed_price_ratio`` and bridge the decimal gap
    # (VIB-3875). Without both, the SDK now raises COMPILATION_PERMANENT.
    _PRICE_ORACLE = {"USDC": Decimal("1.0"), "WETH": Decimal("3000")}

    def test_compile_with_amount(self):
        config = UniswapV4Config(chain="arbitrum", wallet_address=_TEST_WALLET)
        adapter = UniswapV4Adapter(config=config)

        # Create a mock SwapIntent
        intent = MagicMock()
        intent.from_token = "USDC"
        intent.to_token = "WETH"
        intent.amount = Decimal("1000")
        intent.amount_usd = None
        intent.max_slippage = Decimal("0.005")
        intent.intent_id = "test-intent-1"

        bundle = adapter.compile_swap_intent(intent, self._PRICE_ORACLE)
        assert bundle.intent_type == "SWAP"
        assert len(bundle.transactions) > 0
        assert bundle.metadata["protocol_version"] == "v4"
        assert bundle.metadata["from_token"]["symbol"] == "USDC"
        assert bundle.metadata["from_token"]["address"] is not None
        assert bundle.metadata["to_token"]["symbol"] == "WETH"
        assert bundle.metadata["to_token"]["address"] is not None

    def test_compile_with_amount_usd(self):
        config = UniswapV4Config(chain="arbitrum", wallet_address=_TEST_WALLET)
        adapter = UniswapV4Adapter(config=config)

        intent = MagicMock()
        intent.from_token = "USDC"
        intent.to_token = "WETH"
        intent.amount = None
        intent.amount_usd = Decimal("1000")
        intent.max_slippage = Decimal("0.005")
        intent.intent_id = "test-intent-2"

        bundle = adapter.compile_swap_intent(intent, self._PRICE_ORACLE)
        assert bundle.intent_type == "SWAP"
        assert len(bundle.transactions) > 0

    def test_compile_amount_all_raises(self):
        adapter = UniswapV4Adapter(chain="arbitrum")

        intent = MagicMock()
        intent.from_token = "USDC"
        intent.to_token = "WETH"
        intent.amount = "all"
        intent.amount_usd = None
        intent.max_slippage = Decimal("0.005")
        intent.intent_id = "test-intent-3"

        with pytest.raises(ValueError, match="must be resolved"):
            adapter.compile_swap_intent(intent)

    def test_compile_no_amount_raises(self):
        adapter = UniswapV4Adapter(chain="arbitrum")

        intent = MagicMock()
        intent.from_token = "USDC"
        intent.to_token = "WETH"
        intent.amount = None
        intent.amount_usd = None
        intent.max_slippage = Decimal("0.005")
        intent.intent_id = "test-intent-4"

        with pytest.raises(ValueError, match="amount or amount_usd"):
            adapter.compile_swap_intent(intent)


class TestIntentCompilerV4Routing:
    """Test that IntentCompiler routes protocol='uniswap_v4' to V4 adapter."""

    def test_compiler_v4_routes_to_adapter(self):
        """Verify V4 compilation routes through UniswapV4Adapter and succeeds."""
        from almanak.framework.intents import SwapIntent
        from almanak.framework.intents.compiler import IntentCompiler

        compiler = IntentCompiler(
            chain="arbitrum",
            wallet_address=_TEST_WALLET,
            price_oracle={"USDC": Decimal("1.0"), "WETH": Decimal("2500.0")},
        )
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount=Decimal("100"),
            max_slippage=Decimal("0.20"),
            protocol="uniswap_v4",
            chain="arbitrum",
        )

        result = compiler.compile(intent)
        assert result.status.value == "SUCCESS"
        assert result.action_bundle is not None
        assert result.action_bundle.metadata["protocol"] == "uniswap_v4"
        from almanak.connectors.uniswap_v4.addresses import UNISWAP_V4

        assert result.action_bundle.metadata["router"] == UNISWAP_V4["arbitrum"]["universal_router"]
