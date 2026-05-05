"""Tests for Instrument, resolve_instrument, and CEX_SYMBOL_MAP."""

from __future__ import annotations

import pytest

from almanak.framework.data.models import (
    CEX_SYMBOL_MAP,
    Instrument,
    _canonicalize_symbol,
    resolve_instrument,
)

# ---------------------------------------------------------------------------
# Instrument construction
# ---------------------------------------------------------------------------


class TestInstrument:
    def test_basic_construction(self):
        inst = Instrument(base="WETH", quote="USDC", chain="arbitrum")
        assert inst.base == "WETH"
        assert inst.quote == "USDC"
        assert inst.chain == "arbitrum"
        assert inst.venue is None
        assert inst.address is None

    def test_normalization_uppercase(self):
        inst = Instrument(base="weth", quote="usdc", chain="Arbitrum")
        assert inst.base == "WETH"
        assert inst.quote == "USDC"
        assert inst.chain == "arbitrum"

    def test_venue_normalization(self):
        inst = Instrument(base="WETH", quote="USDC", chain="arbitrum", venue="Uniswap_V3")
        assert inst.venue == "uniswap_v3"

    def test_venue_none_stays_none(self):
        inst = Instrument(base="WETH", quote="USDC", chain="arbitrum", venue=None)
        assert inst.venue is None

    def test_with_address(self):
        addr = "0x1234567890abcdef1234567890abcdef12345678"
        inst = Instrument(base="WETH", quote="USDC", chain="arbitrum", address=addr)
        assert inst.address == addr

    def test_frozen(self):
        inst = Instrument(base="WETH", quote="USDC", chain="arbitrum")
        with pytest.raises(AttributeError):
            inst.base = "WBTC"  # type: ignore[misc]

    def test_pair_property(self):
        inst = Instrument(base="WETH", quote="USDC", chain="arbitrum")
        assert inst.pair == "WETH/USDC"

    def test_empty_base_raises(self):
        with pytest.raises(ValueError, match="base symbol"):
            Instrument(base="", quote="USDC", chain="arbitrum")

    def test_empty_quote_raises(self):
        with pytest.raises(ValueError, match="quote symbol"):
            Instrument(base="WETH", quote="", chain="arbitrum")

    def test_empty_chain_raises(self):
        with pytest.raises(ValueError, match="chain"):
            Instrument(base="WETH", quote="USDC", chain="")

    def test_equality(self):
        a = Instrument(base="WETH", quote="USDC", chain="arbitrum")
        b = Instrument(base="WETH", quote="USDC", chain="arbitrum")
        assert a == b

    def test_inequality_different_chain(self):
        a = Instrument(base="WETH", quote="USDC", chain="arbitrum")
        b = Instrument(base="WETH", quote="USDC", chain="base")
        assert a != b

    def test_hashable(self):
        inst = Instrument(base="WETH", quote="USDC", chain="arbitrum")
        s = {inst}
        assert inst in s

    def test_bridged_variant_explicit(self):
        inst = Instrument(base="USDC.E", quote="WETH", chain="arbitrum")
        assert inst.base == "USDC.E"
        assert inst.quote == "WETH"


# ---------------------------------------------------------------------------
# CEX symbol lookup
# ---------------------------------------------------------------------------


class TestCexSymbol:
    def test_binance_eth_usdc(self):
        inst = Instrument(base="WETH", quote="USDC", chain="arbitrum")
        assert inst.cex_symbol("binance") == "ETHUSDC"

    def test_binance_eth_usdt(self):
        inst = Instrument(base="WETH", quote="USDT", chain="ethereum")
        assert inst.cex_symbol("binance") == "ETHUSDT"

    def test_binance_btc_usdt(self):
        inst = Instrument(base="WBTC", quote="USDT", chain="ethereum")
        assert inst.cex_symbol("binance") == "BTCUSDT"

    def test_coinbase_eth_usd(self):
        inst = Instrument(base="WETH", quote="USDC", chain="ethereum")
        assert inst.cex_symbol("coinbase") == "ETH-USD"

    def test_unknown_exchange_returns_none(self):
        inst = Instrument(base="WETH", quote="USDC", chain="arbitrum")
        assert inst.cex_symbol("kraken") is None

    def test_unmapped_pair_returns_none(self):
        inst = Instrument(base="OBSCURE", quote="TOKEN", chain="arbitrum")
        assert inst.cex_symbol("binance") is None

    def test_exchange_case_insensitive(self):
        inst = Instrument(base="WETH", quote="USDC", chain="arbitrum")
        assert inst.cex_symbol("Binance") == "ETHUSDC"
        assert inst.cex_symbol("BINANCE") == "ETHUSDC"

    def test_binance_arb_usdt(self):
        inst = Instrument(base="ARB", quote="USDT", chain="arbitrum")
        assert inst.cex_symbol("binance") == "ARBUSDT"

    def test_binance_matic_usdt(self):
        inst = Instrument(base="WMATIC", quote="USDT", chain="polygon")
        assert inst.cex_symbol("binance") == "MATICUSDT"


# ---------------------------------------------------------------------------
# CEX_SYMBOL_MAP structure
# ---------------------------------------------------------------------------


class TestCexSymbolMap:
    def test_map_is_not_empty(self):
        assert len(CEX_SYMBOL_MAP) > 0

    def test_all_keys_are_tuples(self):
        for key in CEX_SYMBOL_MAP:
            assert isinstance(key, tuple)
            assert len(key) == 3

    def test_all_values_are_strings(self):
        for val in CEX_SYMBOL_MAP.values():
            assert isinstance(val, str)
            assert len(val) > 0


# ---------------------------------------------------------------------------
# _canonicalize_symbol
# ---------------------------------------------------------------------------


class TestCanonicalizeSymbol:
    def test_eth_to_weth(self):
        assert _canonicalize_symbol("ETH") == "WETH"

    def test_eth_lowercase(self):
        assert _canonicalize_symbol("eth") == "WETH"

    def test_matic_to_wmatic(self):
        assert _canonicalize_symbol("MATIC") == "WMATIC"

    def test_avax_to_wavax(self):
        assert _canonicalize_symbol("AVAX") == "WAVAX"

    def test_bnb_to_wbnb(self):
        assert _canonicalize_symbol("BNB") == "WBNB"

    def test_usdc_unchanged(self):
        assert _canonicalize_symbol("USDC") == "USDC"

    def test_weth_unchanged(self):
        assert _canonicalize_symbol("WETH") == "WETH"

    def test_bridged_usdc_e_unchanged(self):
        assert _canonicalize_symbol("USDC.e") == "USDC.E"

    def test_usdt_e_unchanged(self):
        assert _canonicalize_symbol("USDT.e") == "USDT.E"


# ---------------------------------------------------------------------------
# resolve_instrument
# ---------------------------------------------------------------------------


class TestResolveInstrument:
    def test_pair_string(self):
        inst = resolve_instrument("ETH/USDC", "arbitrum")
        assert inst.base == "WETH"
        assert inst.quote == "USDC"
        assert inst.chain == "arbitrum"

    def test_pair_string_whitespace(self):
        inst = resolve_instrument("  ETH / USDC  ", "arbitrum")
        assert inst.base == "WETH"
        assert inst.quote == "USDC"

    def test_single_symbol_defaults_quote_usdc(self):
        inst = resolve_instrument("WETH", "arbitrum")
        assert inst.base == "WETH"
        assert inst.quote == "USDC"

    def test_single_native_symbol_wrapped(self):
        inst = resolve_instrument("ETH", "ethereum")
        assert inst.base == "WETH"
        assert inst.quote == "USDC"

    def test_native_quote_wrapped(self):
        inst = resolve_instrument("USDC/ETH", "arbitrum")
        assert inst.base == "USDC"
        assert inst.quote == "WETH"

    def test_bridged_variant_preserved(self):
        inst = resolve_instrument("USDC.e/WETH", "arbitrum")
        assert inst.base == "USDC.E"
        assert inst.quote == "WETH"

    def test_instrument_passthrough_same_chain(self):
        original = Instrument(base="WETH", quote="USDC", chain="arbitrum", venue="uniswap_v3")
        result = resolve_instrument(original, "arbitrum")
        assert result is original

    def test_instrument_new_chain(self):
        original = Instrument(base="WETH", quote="USDC", chain="arbitrum", venue="uniswap_v3")
        result = resolve_instrument(original, "base")
        assert result is not original
        assert result.chain == "base"
        assert result.base == "WETH"
        assert result.quote == "USDC"
        assert result.venue == "uniswap_v3"

    def test_instrument_new_chain_venue_override(self):
        original = Instrument(base="WETH", quote="USDC", chain="arbitrum", venue="uniswap_v3")
        result = resolve_instrument(original, "base", venue="aerodrome")
        assert result.venue == "aerodrome"

    def test_venue_passed_through(self):
        inst = resolve_instrument("ETH/USDC", "arbitrum", venue="uniswap_v3")
        assert inst.venue == "uniswap_v3"

    def test_matic_pair(self):
        inst = resolve_instrument("MATIC/USDT", "polygon")
        assert inst.base == "WMATIC"
        assert inst.quote == "USDT"
        assert inst.chain == "polygon"

    def test_avax_pair(self):
        inst = resolve_instrument("AVAX/USDC", "avalanche")
        assert inst.base == "WAVAX"
        assert inst.quote == "USDC"

    def test_case_insensitive_chain(self):
        inst = resolve_instrument("WETH/USDC", "Arbitrum")
        assert inst.chain == "arbitrum"
