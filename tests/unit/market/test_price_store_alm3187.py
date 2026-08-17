"""ALM-3187 contract tests for typed price identity and blessed lookup."""

from __future__ import annotations

from decimal import Decimal

from almanak.framework.intents.compiler_queries import lenient_oracle_price
from almanak.framework.market import MarketSnapshotBuilder, PriceData
from almanak.framework.market.price_store import PriceStore, lookup_price

USDC_BASE = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"
WETH_ETHEREUM = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
SOL_USDC = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"


def test_lookup_precedence_is_typed_then_chain_key_then_legacy_address_then_symbol_then_peg() -> None:
    typed = ("base", USDC_BASE.lower())
    prices = {
        typed: Decimal("1.01"),
        f"base:{USDC_BASE}": Decimal("1.02"),
        USDC_BASE: Decimal("1.03"),
        "USDC": Decimal("1.04"),
    }

    found = lookup_price(
        prices,
        chain="base",
        address=USDC_BASE,
        symbol="USDC",
        peg=Decimal("1"),
    )

    assert found is not None
    assert (found.price, found.match) == (Decimal("1.01"), "identity")

    prices.pop(typed)
    found = lookup_price(prices, chain="base", address=USDC_BASE, symbol="USDC", peg=Decimal("1"))
    assert found is not None
    assert (found.price, found.match) == (Decimal("1.02"), "chain_address")

    prices.pop(f"base:{USDC_BASE}")
    found = lookup_price(prices, chain="base", address=USDC_BASE, symbol="USDC", peg=Decimal("1"))
    assert found is not None
    assert (found.price, found.match) == (Decimal("1.03"), "legacy_address")

    prices.pop(USDC_BASE)
    found = lookup_price(prices, chain="base", address=USDC_BASE, symbol="USDC", peg=Decimal("1"))
    assert found is not None
    assert (found.price, found.match) == (Decimal("1.04"), "symbol")

    prices.clear()
    found = lookup_price(prices, chain="base", address=USDC_BASE, symbol="USDC", peg=Decimal("1"))
    assert found is not None
    assert (found.price, found.match) == (Decimal("1"), "peg")


def test_plain_compatibility_mapping_is_usd_only() -> None:
    assert lookup_price({"WETH": Decimal("2000")}, symbol="WETH", quote="ETH") is None


def test_typed_store_isolates_quotes_for_the_same_identity() -> None:
    store: PriceStore[PriceData] = PriceStore()
    usd = PriceData(price=Decimal("2000"))
    eth = PriceData(price=Decimal("1"))
    store.put(USDC_BASE, chain="base", quote="USD", price=usd.price, data=usd, symbol="TOKEN")
    store.put(USDC_BASE, chain="base", quote="ETH", price=eth.price, data=eth, symbol="TOKEN")

    usd_found = lookup_price(store, chain="base", address=USDC_BASE, quote="USD")
    eth_found = lookup_price(store, chain="base", address=USDC_BASE, quote="ETH")

    assert usd_found is not None and usd_found.price == Decimal("2000")
    assert eth_found is not None and eth_found.price == Decimal("1")


def test_same_chain_duplicate_symbol_alias_is_ambiguous() -> None:
    store: PriceStore[PriceData] = PriceStore()
    first = store.put(
        USDC_BASE,
        chain="base",
        quote="USD",
        price=Decimal("1"),
        data=PriceData(price=Decimal("1")),
        symbol="COLLISION",
    )
    second_address = "0x" + "1" * 40
    second = store.put(
        second_address,
        chain="base",
        quote="USD",
        price=Decimal("2"),
        data=PriceData(price=Decimal("2")),
        symbol="COLLISION",
    )

    assert store.has_unambiguous_symbol_alias(first) is False
    assert store.has_unambiguous_symbol_alias(second) is False
    assert lookup_price(store, chain="base", symbol="COLLISION") is None


def test_chainless_address_lookup_fails_closed_on_cross_chain_collision_even_at_same_price() -> None:
    prices = {
        f"base:{USDC_BASE}": {"price_usd": "1.00"},
        f"ethereum:{USDC_BASE}": {"price_usd": "1.00"},
    }

    assert lookup_price(prices, token=USDC_BASE) is None


def test_chainless_bare_address_is_not_reclassified_as_a_symbol() -> None:
    prices = {USDC_BASE: Decimal("1.00")}

    assert lookup_price(prices, token=USDC_BASE) is None


def test_chainless_solana_mint_is_not_reclassified_as_case_insensitive_symbol() -> None:
    prices = {SOL_USDC.swapcase(): Decimal("1.00")}

    assert lookup_price(prices, token=SOL_USDC) is None


def test_explicit_chain_disambiguates_cross_chain_address_collision() -> None:
    prices = {
        f"base:{USDC_BASE}": {"price_usd": "1.00"},
        f"ethereum:{USDC_BASE}": {"price_usd": "2000.00"},
    }

    found = lookup_price(prices, token=USDC_BASE, chain="base")

    assert found is not None
    assert (found.price, found.key) == (Decimal("1.00"), f"base:{USDC_BASE}")


def test_typed_metadata_alias_cannot_cross_chain() -> None:
    prices = {
        ("ethereum", USDC_BASE): {"price_usd": "999", "symbol": "USDC"},
    }

    assert lookup_price(prices, chain="base", symbol="USDC") is None


def test_single_chain_legacy_map_can_supply_context_for_an_address_symbol_alias() -> None:
    unrelated = "0x" + "d" * 40
    prices = {"USDC": Decimal("1"), f"base:{unrelated}": Decimal("5")}

    found = lookup_price(prices, token=USDC_BASE)

    assert found is not None and found.price == Decimal("1")


def test_multi_chain_legacy_map_does_not_guess_context_for_an_address_symbol_alias() -> None:
    unrelated = "0x" + "d" * 40
    prices = {
        "USDC": Decimal("1"),
        f"base:{unrelated}": Decimal("5"),
        f"ethereum:{unrelated}": Decimal("6"),
    }

    assert lookup_price(prices, token=USDC_BASE) is None


def test_chainless_duplicate_symbol_is_ambiguous_even_at_same_price() -> None:
    store: PriceStore[PriceData] = PriceStore()
    store.put(
        USDC_BASE,
        chain="base",
        quote="USD",
        price=Decimal("1"),
        data=PriceData(price=Decimal("1")),
        symbol="COLLISION",
    )
    store.put(
        "0x" + "2" * 40,
        chain="ethereum",
        quote="USD",
        price=Decimal("1"),
        data=PriceData(price=Decimal("1")),
        symbol="COLLISION",
    )

    assert lookup_price(store, symbol="COLLISION") is None
    assert lookup_price(store, chain="base", symbol="COLLISION") is not None


def test_market_snapshot_compatibility_export_excludes_non_usd_quotes() -> None:
    market = MarketSnapshotBuilder.seeded(chain="ethereum", wallet_address="0xwallet")
    market.set_price_data("WETH", PriceData(price=Decimal("2000")), quote="USD")
    market.set_price_data("WETH", PriceData(price=Decimal("1")), quote="ETH")

    assert market.price("WETH", quote="ETH") == Decimal("1")
    assert market.get_price_oracle_dict() == {"WETH": Decimal("2000")}


def test_compatibility_export_canonicalizes_primary_chain_alias() -> None:
    market = MarketSnapshotBuilder.seeded(chain="mainnet", wallet_address="0xwallet")
    market.set_price_data(WETH_ETHEREUM, PriceData(price=Decimal("2000")))

    exported = market.get_price_oracle_dict()

    assert exported[f"ethereum:{WETH_ETHEREUM.lower()}"] == Decimal("2000")
    assert exported["WETH"] == Decimal("2000")
    assert lookup_price(exported, chain="mainnet", symbol="WETH") is not None


def test_compatibility_export_keeps_non_primary_symbol_only_price() -> None:
    market = MarketSnapshotBuilder.seeded(chain="arbitrum", wallet_address="0xwallet")
    market.set_price_data("AERO", PriceData(price=Decimal("1.25")), chain="base")

    exported = market.get_price_oracle_dict()

    assert exported["base:AERO"] == Decimal("1.25")
    assert exported["AERO"] == Decimal("1.25")
    found = lookup_price(exported, chain="base", symbol="AERO")
    assert found is not None and found.price == Decimal("1.25")


def test_compatibility_export_adds_unique_non_primary_address_symbol_alias() -> None:
    market = MarketSnapshotBuilder.seeded(chain="arbitrum", wallet_address="0xwallet")
    market.set_price_data(USDC_BASE, PriceData(price=Decimal("1.00")), chain="base")

    exported = market.get_price_oracle_dict()

    assert exported[f"base:{USDC_BASE.lower()}"] == Decimal("1.00")
    assert exported["USDC"] == Decimal("1.00")


def test_chain_qualified_symbol_lookup_canonicalizes_chain_alias() -> None:
    found = lookup_price({"ethereum:WETH": Decimal("2000")}, chain="mainnet", symbol="WETH")

    assert found is not None and found.price == Decimal("2000")


def test_lenient_oracle_price_zero_blocks_lower_priority_stablecoin_peg() -> None:
    assert lenient_oracle_price({"USDC": Decimal("0")}, "USDC", "arbitrum") is None


def test_lenient_oracle_price_walks_past_zero_to_wrapped_native_alias() -> None:
    prices = {"WETH": Decimal("0"), "ETH": Decimal("3000")}

    assert lenient_oracle_price(prices, "WETH", "arbitrum") == Decimal("3000")


def test_solana_mint_case_is_preserved_in_typed_store_and_compatibility_export() -> None:
    market = MarketSnapshotBuilder.seeded(chain="solana", wallet_address="wallet")
    market.set_price_data(SOL_USDC, PriceData(price=Decimal("0.9998")))

    exported = market.get_price_oracle_dict()

    assert exported[f"solana:{SOL_USDC}"] == Decimal("0.9998")
    assert f"solana:{SOL_USDC.upper()}" not in exported
    assert lookup_price(exported, chain="solana", address=SOL_USDC) is not None
    assert lookup_price(exported, chain="solana", address=SOL_USDC.swapcase()) is None
