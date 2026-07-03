"""Receipt-parser dynamic pool-meta transport for UNCURATED Curve pools (VIB-5628).

The Curve receipt parser holds no gateway client; on a static ``CURVE_POOLS``
miss it consults a runner-injected ``pool_meta_lookup`` sync callable to label an
uncurated pool's LP legs (coin addresses / symbols / pool_type). These tests
prove:

- static MISS + lookup HIT  -> legs labelled from the resolver metadata,
- ``pool_meta_lookup=None``  -> degrades to ``[]`` / ``""`` (Empty != Zero,
  never fabricates),
- the module-level helpers thread the callable and stay static-FIRST.

The lookup is mocked — no chain access.
"""

from __future__ import annotations

from almanak.connectors.curve.pool_resolver import CurvePoolMetadata
from almanak.connectors.curve.receipt_parser import (
    EVENT_TOPICS,
    CurveReceiptParser,
    _pool_coin_addresses,
    _pool_coin_symbols,
    _pool_type,
)

# Uncurated tricryptoUSDC pool (NOT in CURVE_POOLS) — the VIB-5628 real-fork pool.
UNCURATED_POOL = "0x7f86bf177dd4f3494b841a37e810a34dd56c829b"
LP_TOKEN = "0x7f86bf177dd4f3494b841a37e810a34dd56c829b"  # tricrypto LP == pool
WALLET = "0xaabbccddee1122334455667788990011aabbccdd"

USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
WBTC = "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599"
WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"

# CurvePoolMetadata the mocked resolver returns for the uncurated pool.
UNCURATED_META = CurvePoolMetadata(
    address=UNCURATED_POOL,
    lp_token=LP_TOKEN,
    coin_addresses=[USDC, WBTC, WETH],
    coin_decimals=[6, 8, 18],
    coin_symbols=["USDC", "WBTC", "WETH"],
    n_coins=3,
    pool_type="tricrypto",
    is_metapool=False,
    base_pool=None,
    base_pool_coin_addresses=None,
    base_pool_coins=None,
)


def _fake_lookup(pool_address: str, chain: str) -> CurvePoolMetadata | None:
    """Sync ``(pool_address, chain) -> metadata`` mock — hits only for the uncurated pool."""
    if pool_address.lower() == UNCURATED_POOL and chain == "ethereum":
        return UNCURATED_META
    return None


def _pad_hex(value: int) -> str:
    return f"{value:064x}"


def _build_add_liquidity_receipt(pool: str, amounts: list[int]) -> dict:
    """Minimal 3-coin AddLiquidity3 receipt + LP mint Transfer for ``pool``."""
    add_topic = EVENT_TOPICS["AddLiquidity3"].lower()
    transfer_topic = EVENT_TOPICS["Transfer"].lower()
    provider_topic = "0x" + "00" * 12 + WALLET[2:]
    zero_topic = "0x" + "00" * 32
    wallet_topic = "0x" + "00" * 12 + WALLET[2:]

    fees = [0, 0, 0]
    data_parts = [_pad_hex(a) for a in amounts] + [_pad_hex(f) for f in fees]
    data_parts += [_pad_hex(10**20), _pad_hex(10**21)]  # invariant, token_supply
    add_data = "0x" + "".join(data_parts)

    return {
        "status": 1,
        "from": WALLET,
        "transactionHash": "0x" + "ab" * 32,
        "blockNumber": 19_000_000,
        "gasUsed": 300_000,
        "logs": [
            {
                "address": pool,
                "topics": [add_topic, provider_topic],
                "data": add_data,
                "logIndex": 0,
            },
            {
                "address": LP_TOKEN,
                "topics": [transfer_topic, zero_topic, wallet_topic],
                "data": "0x" + _pad_hex(99 * 10**18),
                "logIndex": 1,
            },
        ],
    }


# --------------------------------------------------------------------------- #
# Module-level helper threading (static FIRST, dynamic on miss, fail-safe)
# --------------------------------------------------------------------------- #


class TestHelperThreading:
    def test_coin_addresses_static_miss_then_lookup_hit(self) -> None:
        out = _pool_coin_addresses(UNCURATED_POOL, "ethereum", _fake_lookup)
        assert out == [USDC, WBTC, WETH]

    def test_coin_symbols_static_miss_then_lookup_hit(self) -> None:
        out = _pool_coin_symbols(UNCURATED_POOL, "ethereum", _fake_lookup)
        assert out == ["USDC", "WBTC", "WETH"]

    def test_pool_type_static_miss_then_lookup_hit(self) -> None:
        assert _pool_type(UNCURATED_POOL, "ethereum", _fake_lookup) == "tricrypto"

    def test_no_lookup_degrades_to_empty(self) -> None:
        # pool_meta_lookup=None (the default): no fabrication, legacy shape.
        assert _pool_coin_addresses(UNCURATED_POOL, "ethereum") == []
        assert _pool_coin_symbols(UNCURATED_POOL, "ethereum") == []
        assert _pool_type(UNCURATED_POOL, "ethereum") == ""

    def test_lookup_miss_degrades_to_empty(self) -> None:
        # Lookup wired but returns None (non-pool address): still no fabrication.
        miss = lambda addr, chain: None  # noqa: E731
        assert _pool_coin_addresses(UNCURATED_POOL, "ethereum", miss) == []
        assert _pool_coin_symbols(UNCURATED_POOL, "ethereum", miss) == []
        assert _pool_type(UNCURATED_POOL, "ethereum", miss) == ""

    def test_lookup_raising_is_swallowed(self) -> None:
        # A malformed injected callable must never break the accounting path.
        def _boom(addr: str, chain: str) -> CurvePoolMetadata | None:
            raise RuntimeError("transport blew up")

        assert _pool_coin_addresses(UNCURATED_POOL, "ethereum", _boom) == []
        assert _pool_coin_symbols(UNCURATED_POOL, "ethereum", _boom) == []
        assert _pool_type(UNCURATED_POOL, "ethereum", _boom) == ""


# --------------------------------------------------------------------------- #
# Parser-level: uncurated LP_OPEN legs labelled only with the transport wired
# --------------------------------------------------------------------------- #


class TestParserLegLabeling:
    def test_lp_open_symbols_labelled_with_transport(self) -> None:
        receipt = _build_add_liquidity_receipt(UNCURATED_POOL, [1_000_000, 0, 0])
        parser = CurveReceiptParser(chain="ethereum", pool_meta_lookup=_fake_lookup)
        open_data = parser.extract_lp_open_data(receipt)
        assert open_data is not None
        assert open_data.pool_address == UNCURATED_POOL
        # Legs that would be EMPTY without the transport are now labelled.
        assert open_data.coin_symbols == ["USDC", "WBTC", "WETH"]

    def test_lp_open_symbols_empty_without_transport(self) -> None:
        receipt = _build_add_liquidity_receipt(UNCURATED_POOL, [1_000_000, 0, 0])
        parser = CurveReceiptParser(chain="ethereum")  # no pool_meta_lookup
        open_data = parser.extract_lp_open_data(receipt)
        assert open_data is not None
        # Static miss + no transport -> None (legacy path), never a fabricated leg.
        assert open_data.coin_symbols is None

    def test_primitive_money_legs_bound_to_coins_with_transport(self) -> None:
        # Single-sided USDC deposit (coin index 0): the leg must bind to USDC.
        receipt = _build_add_liquidity_receipt(UNCURATED_POOL, [1_000_000, 0, 0])
        parser = CurveReceiptParser(chain="ethereum", pool_meta_lookup=_fake_lookup)
        legs = parser.extract_primitive_money_legs(receipt)
        assert legs is not None
        # The leg builder resolves the coin address (from the transport metadata)
        # to its token identity; the funded coin (index 0) is USDC. Accept either
        # the resolved symbol or the raw address so the test does not couple to the
        # TokenResolver's symbol table.
        tokens = {leg.token.lower() for leg in legs.legs}
        assert USDC in tokens or "usdc" in tokens

    def test_primitive_money_legs_none_without_transport(self) -> None:
        receipt = _build_add_liquidity_receipt(UNCURATED_POOL, [1_000_000, 0, 0])
        parser = CurveReceiptParser(chain="ethereum")  # no pool_meta_lookup
        # Without the coin-address map the extractor cannot bind amounts -> None,
        # so the enricher falls back to the legacy two-slot path (never a guess).
        assert parser.extract_primitive_money_legs(receipt) is None
