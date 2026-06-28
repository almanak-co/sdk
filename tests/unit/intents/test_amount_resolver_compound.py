"""Compound V3 amount resolver address lookup tests."""

from __future__ import annotations

from almanak.connectors._strategy_base.address_registry import AddressRegistry
from almanak.connectors.compound_v3.addresses import COMPOUND_V3_COMET_ADDRESSES
from almanak.framework.intents.amount_resolver import CompoundV3BalanceReader


def test_compound_v3_amount_resolver_uses_manifest_address_table() -> None:
    """Compound V3 balance reads resolve Comets through connector-owned addresses."""
    AddressRegistry.reset_cache()
    reader = CompoundV3BalanceReader()

    assert reader._get_comet_address("base", "usdc") == COMPOUND_V3_COMET_ADDRESSES["base"]["usdc"]
    assert reader._get_comet_address("optimism", None) == COMPOUND_V3_COMET_ADDRESSES["optimism"]["usdc"]
    assert reader._get_comet_address("base", None) is None


def test_compound_v3_amount_resolver_returns_none_for_unsupported_chain() -> None:
    """A chain with no Compound V3 Comet table resolves to None (not a crash),
    so the caller falls back to withdraw_all rather than guessing an address."""
    AddressRegistry.reset_cache()
    reader = CompoundV3BalanceReader()

    # avalanche ships no compound_v3 deployment in the connector address table.
    assert "avalanche" not in COMPOUND_V3_COMET_ADDRESSES
    assert reader._get_comet_address("avalanche", None) is None
    assert reader._get_comet_address("avalanche", "usdc") is None


# Comet.balanceOf()/borrowBalanceOf() report the BASE-asset position only; a
# non-base (collateral) token on the same market must NOT size off them.
_BASE = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"  # base asset (USDC)
_COLLATERAL = "0x4200000000000000000000000000000000000006"  # a non-base collateral (WETH)


def _reader_with_base(monkeypatch, base_addr: str, balance_wei: int):
    reader = CompoundV3BalanceReader()
    monkeypatch.setattr(reader, "_get_comet_address", lambda chain, market_id: "0xC0FFEE")
    base_word = "0x" + base_addr.replace("0x", "").zfill(64)
    balance_word = "0x" + format(balance_wei, "064x")

    def _fake_eth_call(gateway_client, chain, to, data):
        # baseToken() vs balanceOf()/borrowBalanceOf() distinguished by selector.
        return base_word if data == reader._BASE_TOKEN_SELECTOR else balance_word

    monkeypatch.setattr(reader, "_eth_call", _fake_eth_call)
    return reader


def test_compound_v3_non_base_token_supply_is_unmeasured(monkeypatch) -> None:
    """A collateral (non-base) token resolves to None (unmeasured) — never the base supply."""
    reader = _reader_with_base(monkeypatch, _BASE, balance_wei=12345)
    assert reader.get_supply_balance("base", _COLLATERAL, "0xWALLET", market_id="usdc", gateway_client=object()) is None
    assert reader.get_debt_balance("base", _COLLATERAL, "0xWALLET", market_id="usdc", gateway_client=object()) is None


def test_compound_v3_base_token_supply_reads_balance(monkeypatch) -> None:
    """The base token still reads the real Comet balance (no regression)."""
    reader = _reader_with_base(monkeypatch, _BASE, balance_wei=12345)
    assert reader.get_supply_balance("base", _BASE, "0xWALLET", market_id="usdc", gateway_client=object()) == 12345
    assert reader.get_debt_balance("base", _BASE, "0xWALLET", market_id="usdc", gateway_client=object()) == 12345


def test_compound_v3_unreadable_base_token_fails_closed(monkeypatch) -> None:
    """If baseToken() can't be read, FAIL CLOSED: unmeasured (None), never the
    base balance for an unverifiable token (CodeRabbit #3070)."""
    reader = CompoundV3BalanceReader()
    monkeypatch.setattr(reader, "_get_comet_address", lambda chain, market_id: "0xC0FFEE")
    # baseToken() returns nothing (RPC fault); balance reads would still answer.
    monkeypatch.setattr(reader, "_eth_call", lambda gw, chain, to, data: None)
    assert reader.get_supply_balance("base", _BASE, "0xWALLET", market_id="usdc", gateway_client=object()) is None
    assert reader.get_debt_balance("base", _BASE, "0xWALLET", market_id="usdc", gateway_client=object()) is None
