"""Aster Perps perps-read decode/value pins + one-folder-one-row wiring (VIB-4930 PR-4).

Aster is the SECOND perp venue on the perps-read seam, and unlike the GMX
migration it has **no byte-parity oracle** — Aster never had framework perp
valuation. So the decode is pinned against the connector's own ABI
(``abis/TradingReaderFacet.json`` → ``getPositionsV2`` →
``ITradingReader.Position[]``) via an internal encode→decode round-trip, the
build-calls calldata is round-tripped against the SDK selector
(``SELECTOR_GET_POSITIONS_V2``), and the mark-to-market math is pinned by
documented known-good vectors whose expected values are derived by hand from
Aster's decimal conventions (qty 1e10, price 1e8, synthesised entry-notional
1e8 USD).

The strongest available cross-check is the connector's own struct definition:
the test encodes the exact ABI tuple the ``TradingReaderFacet`` returns and
asserts ``reduce_calls`` recovers every field with the right scale — there is no
SDK-side decode of ``getPositionsV2`` to diff against (the SDK only encodes the
selector; the on-chain view has no Python decode analogue), so the ABI tuple IS
the oracle.

Decimal conventions under test (each with its connector source):
  * qty 1e10  — sdk.py:119 ``QTY_DECIMALS=10``; receipt_parser.py:257
  * price 1e8 — sdk.py:118 ``PRICE_DECIMALS=8``; receipt_parser.py:279
  * margin    — marginToken native decimals (sdk.py:160); BSC USDT/USDC are 18
  * size_usd  — synthesised entry notional in 1e8 USD (perps_read.py reducer)
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from eth_abi import decode as abi_decode
from eth_abi import encode as abi_encode
from eth_utils import to_checksum_address

from almanak.connectors._strategy_base.perps_read_base import (
    PerpsPositionQuery,
    PerpsPositionValue,
)
from almanak.connectors._strategy_base.perps_read_registry import PerpsReadRegistry
# perps_read internals (private helpers like _aster_market_metadata, _POSITION_TUPLE)
# now live in the shared _aster_perps_core foundation; test them at their home.
from almanak.connectors._aster_perps_core import perps_read as aster_perps
from almanak.connectors.aster_perps.addresses import ASTER_PERPS_MARKETS
from almanak.connectors.aster_perps.sdk import (
    PRICE_DECIMALS,
    QTY_DECIMALS,
    SELECTOR_GET_POSITIONS_V2,
)

# Real BSC pairBase + margin-token addresses from the connector's own tables.
_BTC = ASTER_PERPS_MARKETS["bsc"]["BTC/USD"]  # BTCB
_ETH = ASTER_PERPS_MARKETS["bsc"]["ETH/USD"]  # ETH (BSC)
_BNB = ASTER_PERPS_MARKETS["bsc"]["BNB/USD"]  # WBNB
_USDT = "0x55d398326f99059fF775485246999027B3197955"  # BSC USDT (18 decimals)
_ACCOUNT = to_checksum_address("0x" + "11" * 20)

# BSC stablecoin margin is 18 decimals (NOT 6 — a BSC-specific fact this connector
# must get right; sdk.py:160 says margin is in the marginToken's native units).
_BSC_USDT_DECIMALS = 18


def _position_tuple(
    *,
    pair_base: str = _BTC,
    margin_token: str = _USDT,
    is_long: bool = True,
    margin: int = 0,
    qty: int = 0,
    entry_price: int = 0,
    timestamp: int = 1_700_000_000,
    position_hash: bytes = b"\x00" * 32,
    pair: str = "BTC/USD",
) -> tuple:
    """A synthetic ``ITradingReader.Position`` tuple in the exact ABI field order.

    Field order/types mirror ``abis/TradingReaderFacet.json`` getPositionsV2
    outputs (see perps_read._POSITION_TUPLE): positionHash, pair, pairBase,
    marginToken, isLong, margin, qty, entryPrice, stopLoss, takeProfit, openFee,
    executionFee, fundingFee, timestamp, holdingFee. The fields the reducer drops
    (stopLoss/takeProfit/fees) carry arbitrary non-zero sentinels to prove they
    are NOT leaking into the decoded position.
    """
    return (
        position_hash,
        pair,
        to_checksum_address(pair_base),
        to_checksum_address(margin_token),
        is_long,
        margin,  # uint96
        qty,  # uint80
        entry_price,  # uint64
        111,  # stopLoss (dropped)
        222,  # takeProfit (dropped)
        333,  # openFee (dropped)
        444,  # executionFee (dropped)
        -555,  # fundingFee int256 signed (dropped)
        timestamp,  # uint40
        666,  # holdingFee (dropped)
    )


def _encode(tuples: list[tuple]) -> str:
    return "0x" + abi_encode([aster_perps._GET_POSITIONS_V2_OUTPUT], [tuples]).hex()


def _query(**overrides) -> PerpsPositionQuery:
    base = {
        "chain": "bsc",
        "wallet_address": _ACCOUNT,
        "targets": {"router": to_checksum_address("0x" + "22" * 20)},
        "markets": aster_perps._markets_for_chain("bsc"),
    }
    base.update(overrides)
    return PerpsPositionQuery(**base)


# --------------------------------------------------------------------------- #
# reduce_calls: decode round-trip against the ABI struct (the only oracle)
# --------------------------------------------------------------------------- #


def test_reduce_decodes_every_field_with_correct_scale():
    qty = int(Decimal("1.5") * 10**QTY_DECIMALS)  # 1.5 BTC, 1e10 scale
    entry = int(Decimal("50000") * 10**PRICE_DECIMALS)  # $50,000, 1e8 scale
    margin = int(Decimal("5000") * 10**_BSC_USDT_DECIMALS)  # 5,000 USDT, 18 dec
    blob = _encode([_position_tuple(qty=qty, entry_price=entry, margin=margin, is_long=True)])

    # One market blob (BTC); the other two markets returned None (skipped).
    result = aster_perps._reduce_aster_positions(_query(), [blob, None, None])
    assert result.ok is True
    assert len(result.positions) == 1
    pos = result.positions[0]

    assert pos.account == _ACCOUNT
    assert pos.market == to_checksum_address(_BTC)
    assert pos.collateral_token == to_checksum_address(_USDT)
    assert pos.is_long is True
    assert pos.size_in_tokens == qty  # raw 1e10 qty preserved
    assert pos.collateral_amount == margin  # raw margin preserved
    assert pos.increased_at_time == 1_700_000_000
    assert pos.decreased_at_time == 0  # not in the Position struct
    # GMX-shaped fields with no Aster analogue stay 0 (never fabricated).
    assert pos.borrowing_factor == 0
    assert pos.funding_fee_amount_per_size == 0
    # Synthesised entry notional in 1e8 USD: 1.5 * 50000 * 1e8 = 7.5e12.
    assert pos.size_in_usd == int(Decimal("75000") * 10**PRICE_DECIMALS)
    # key_prefix is per-venue.
    assert pos.position_key == f"aster-{_BTC.lower()}-{_USDT.lower()}-long"


def test_reduce_size_usd_synthesis_round_trips_entry_price():
    """The synthesised size_in_usd must recover the on-chain entryPrice via
    size_usd / tokens (the framework valuer's entry-price derivation)."""
    qty = int(Decimal("0.3") * 10**QTY_DECIMALS)
    entry = int(Decimal("2500.5") * 10**PRICE_DECIMALS)
    blob = _encode([_position_tuple(pair_base=_ETH, qty=qty, entry_price=entry, margin=1)])
    pos = aster_perps._reduce_aster_positions(_query(markets=(_ETH,)), [blob]).positions[0]
    # size_usd / qty == entryPrice (to the reducer's 1e8 truncation).
    size_usd = Decimal(pos.size_in_usd) / Decimal(10**PRICE_DECIMALS)
    tokens = Decimal(pos.size_in_tokens) / Decimal(10**QTY_DECIMALS)
    recovered_entry = size_usd / tokens
    assert abs(recovered_entry - Decimal("2500.5")) < Decimal("0.00000001")


def test_reduce_multiple_positions_one_pair_long_and_short():
    """One pairBase can return several positions (e.g. a long and a short)."""
    long_t = _position_tuple(qty=10**10, entry_price=50000 * 10**8, margin=10**18, is_long=True)
    short_t = _position_tuple(qty=2 * 10**10, entry_price=51000 * 10**8, margin=2 * 10**18, is_long=False)
    blob = _encode([long_t, short_t])
    result = aster_perps._reduce_aster_positions(_query(markets=(_BTC,)), [blob])
    assert result.ok is True
    assert len(result.positions) == 2
    assert {p.is_long for p in result.positions} == {True, False}


def test_reduce_filters_inactive_zero_qty_positions():
    """A zero-qty/zero-entry row synthesises size_in_usd=0 -> is_active False -> dropped."""
    active = _position_tuple(qty=10**10, entry_price=50000 * 10**8, margin=10**18, is_long=True)
    inactive = _position_tuple(qty=0, entry_price=0, margin=0, is_long=False)
    blob = _encode([active, inactive])
    result = aster_perps._reduce_aster_positions(_query(markets=(_BTC,)), [blob])
    assert len(result.positions) == 1
    assert result.positions[0].is_long is True


def test_reduce_dropped_struct_fields_do_not_leak():
    """stopLoss/takeProfit/openFee/executionFee/fundingFee/holdingFee sentinels
    in _position_tuple must not appear on the decoded position."""
    blob = _encode([_position_tuple(qty=10**10, entry_price=50000 * 10**8, margin=10**18)])
    pos = aster_perps._reduce_aster_positions(_query(markets=(_BTC,)), [blob]).positions[0]
    # The decoded position exposes none of the sentinel values (111/222/333/...).
    leaked = {pos.borrowing_factor, pos.funding_fee_amount_per_size, pos.decreased_at_time}
    assert leaked == {0}


def test_reduce_empty_book_is_measured_not_failed():
    """A successful decode of empty arrays is a measured empty book (ok=True)."""
    empty = _encode([])
    result = aster_perps._reduce_aster_positions(_query(), [empty, empty, empty])
    assert result.ok is True
    assert result.positions == ()


def test_reduce_all_markets_none_is_unmeasured():
    """Empty≠Zero: every market blob None (whole read failed) -> ok=False."""
    assert aster_perps._reduce_aster_positions(_query(), [None, None, None]).ok is False
    assert aster_perps._reduce_aster_positions(_query(), []).ok is False


def test_reduce_single_failed_market_is_skipped_not_fatal():
    """One None market is skipped; the others still decode and read stays ok=True."""
    blob = _encode([_position_tuple(qty=10**10, entry_price=50000 * 10**8, margin=10**18)])
    # market0 None (failed), market1 has a position, market2 empty.
    result = aster_perps._reduce_aster_positions(_query(), [None, blob, _encode([])])
    assert result.ok is True
    assert len(result.positions) == 1


def test_reduce_garbage_blob_is_skipped():
    """A malformed (non-decodable) blob is skipped without failing the whole read
    when another market decoded; if it is the only non-None blob, the read is
    still ok=True (measured) with no positions — a garbage blob is not signalled
    as unmeasured, only an all-None read is."""
    good = _encode([_position_tuple(qty=10**10, entry_price=50000 * 10**8, margin=10**18)])
    result = aster_perps._reduce_aster_positions(_query(), [good, "0xdeadbeef", None])
    assert result.ok is True
    assert len(result.positions) == 1


# --------------------------------------------------------------------------- #
# build_calls: per-market calldata round-trip
# --------------------------------------------------------------------------- #


def test_build_calls_one_per_market_with_selector_and_args():
    router = to_checksum_address("0x" + "33" * 20)
    markets = aster_perps._markets_for_chain("bsc")
    calls = aster_perps._build_aster_calls(_query(targets={"router": router}, markets=markets))
    assert len(calls) == len(markets) == 3
    selector_hex = SELECTOR_GET_POSITIONS_V2.hex()
    for call, pair_base in zip(calls, markets, strict=True):
        assert call.to == router
        assert call.data.startswith("0x" + selector_hex)
        args = bytes.fromhex(call.data[2 + 8 :])  # strip "0x" + 4-byte selector
        trader, pb = abi_decode(["address", "address"], args)
        assert to_checksum_address(trader) == _ACCOUNT
        assert to_checksum_address(pb) == to_checksum_address(pair_base)


def test_build_calls_fail_closed_on_missing_router_or_markets():
    markets = aster_perps._markets_for_chain("bsc")
    # No router target -> [].
    assert (
        aster_perps._build_aster_calls(PerpsPositionQuery(chain="bsc", wallet_address=_ACCOUNT, markets=markets)) == []
    )
    # No markets -> [].
    assert aster_perps._build_aster_calls(_query(markets=())) == []


# --------------------------------------------------------------------------- #
# market_metadata
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("market,symbol", [(_BTC, "BTC"), (_ETH, "ETH"), (_BNB, "BNB")])
def test_market_metadata_known_markets(market, symbol):
    meta = aster_perps._aster_market_metadata(market, "bsc")
    assert meta is not None
    assert meta.index_token_symbol == symbol
    # Aster qty is always 1e10 regardless of the underlying asset's ERC-20
    # decimals -> metadata reports QTY_DECIMALS so the valuer recovers human qty.
    assert meta.index_token_decimals == QTY_DECIMALS == 10
    # Case-insensitive on the pairBase address.
    assert aster_perps._aster_market_metadata(market.lower(), "bsc") == meta


def test_market_metadata_unknown_returns_none():
    assert aster_perps._aster_market_metadata("0x" + "ab" * 20, "bsc") is None
    assert aster_perps._aster_market_metadata(_BTC, "arbitrum") is None  # not deployed


# --------------------------------------------------------------------------- #
# value_position: documented known-good vectors (no parity oracle — hand-derived)
# --------------------------------------------------------------------------- #

# Each vector below is computed by hand from Aster's conventions and pinned as
# literals. Common derivation for a position of ``q`` tokens, entry ``e``, mark
# ``m``, margin ``c`` USDT at $1, 18-dec margin:
#   size_in_usd (raw) = q*1e10 * e*1e8 // 1e10 = q*e*1e8   (1e8 USD scale)
#   size_usd          = size_in_usd / 1e8 = q*e
#   entry_price       = size_usd / q = e
#   pnl  (long)       = q * (m - e)        ;  (short) = q * (e - m)
#   collateral_value  = c                  (USDT @ $1)
#   net               = collateral_value + pnl
#   leverage          = size_usd / collateral_value = q*e / c
_USD8 = 10**PRICE_DECIMALS  # 1e8


def _value(*, q: Decimal, e: Decimal, m: Decimal, c: Decimal, is_long: bool) -> PerpsPositionValue:
    qty_raw = int(q * 10**QTY_DECIMALS)
    entry_raw = int(e * 10**PRICE_DECIMALS)
    size_usd_raw = (qty_raw * entry_raw) // (10**QTY_DECIMALS)  # mirrors the reducer
    return aster_perps.value_aster_position(
        size_in_usd=size_usd_raw,
        size_in_tokens=qty_raw,
        collateral_amount=int(c * 10**_BSC_USDT_DECIMALS),
        is_long=is_long,
        mark_price_usd=m,
        collateral_token_price_usd=Decimal("1"),
        collateral_token_decimals=_BSC_USDT_DECIMALS,
        index_token_decimals=QTY_DECIMALS,
        market=_BTC,
    )


def test_value_long_profit_known_vector():
    # 1.5 BTC long, entry 50000, mark 52000, 5000 USDT margin.
    # size=75000, entry=50000, pnl=1.5*(52000-50000)=3000, net=5000+3000=8000, lev=75000/5000=15.
    v = _value(q=Decimal("1.5"), e=Decimal("50000"), m=Decimal("52000"), c=Decimal("5000"), is_long=True)
    assert v.size_usd == Decimal("75000")
    assert v.entry_price_usd == Decimal("50000")
    assert v.mark_price_usd == Decimal("52000")
    assert v.unrealized_pnl_usd == Decimal("3000.0")
    assert v.collateral_value_usd == Decimal("5000")
    assert v.net_value_usd == Decimal("8000.0")
    assert v.leverage == Decimal("15")
    assert v.pending_fees_usd == Decimal("0")
    assert v.is_long is True


def test_value_long_loss_known_vector():
    # 1.5 BTC long, entry 50000, mark 48000: pnl=1.5*(48000-50000)=-3000, net=5000-3000=2000.
    v = _value(q=Decimal("1.5"), e=Decimal("50000"), m=Decimal("48000"), c=Decimal("5000"), is_long=True)
    assert v.unrealized_pnl_usd == Decimal("-3000.0")
    assert v.net_value_usd == Decimal("2000.0")


def test_value_short_profit_known_vector():
    # 2 ETH short, entry 3000, mark 2800: pnl=2*(3000-2800)=400, collateral 1000, net=1400.
    # size=6000, lev=6000/1000=6.
    v = _value(q=Decimal("2"), e=Decimal("3000"), m=Decimal("2800"), c=Decimal("1000"), is_long=False)
    assert v.size_usd == Decimal("6000")
    assert v.entry_price_usd == Decimal("3000")
    assert v.unrealized_pnl_usd == Decimal("400")
    assert v.net_value_usd == Decimal("1400")
    assert v.leverage == Decimal("6")
    assert v.is_long is False


def test_value_short_loss_known_vector():
    # 2 ETH short, entry 3000, mark 3300: pnl=2*(3000-3300)=-600, net=1000-600=400.
    v = _value(q=Decimal("2"), e=Decimal("3000"), m=Decimal("3300"), c=Decimal("1000"), is_long=False)
    assert v.unrealized_pnl_usd == Decimal("-600")
    assert v.net_value_usd == Decimal("400")


def test_value_breakeven_zero_pnl():
    v = _value(q=Decimal("1"), e=Decimal("50000"), m=Decimal("50000"), c=Decimal("5000"), is_long=True)
    assert v.unrealized_pnl_usd == Decimal("0")
    assert v.net_value_usd == Decimal("5000")


def test_value_non_stable_margin_priced_at_market():
    """WBNB margin (18 dec) valued at its market price, not assumed $1."""
    # 1 BNB long entry 600 mark 660, 10 WBNB margin @ $600 => collateral_value=6000.
    # size=600, pnl=1*(660-600)=60, net=6000+60=6060, lev=600/6000=0.1.
    qty_raw = int(Decimal("1") * 10**QTY_DECIMALS)
    entry_raw = int(Decimal("600") * 10**PRICE_DECIMALS)
    size_usd_raw = (qty_raw * entry_raw) // (10**QTY_DECIMALS)
    v = aster_perps.value_aster_position(
        size_in_usd=size_usd_raw,
        size_in_tokens=qty_raw,
        collateral_amount=int(Decimal("10") * 10**18),  # 10 WBNB (18 dec)
        is_long=True,
        mark_price_usd=Decimal("660"),
        collateral_token_price_usd=Decimal("600"),  # WBNB price
        collateral_token_decimals=18,
        index_token_decimals=QTY_DECIMALS,
        market=_BNB,
    )
    assert v.collateral_value_usd == Decimal("6000")
    assert v.unrealized_pnl_usd == Decimal("60")
    assert v.net_value_usd == Decimal("6060")
    assert v.leverage == Decimal("0.1")


def test_value_fees_reduce_net_value():
    v = aster_perps.value_aster_position(
        size_in_usd=(10**10 * 50000 * 10**8) // 10**10,
        size_in_tokens=10**10,
        collateral_amount=5000 * 10**18,
        is_long=True,
        mark_price_usd=Decimal("50000"),
        collateral_token_price_usd=Decimal("1"),
        collateral_token_decimals=18,
        index_token_decimals=QTY_DECIMALS,
        pending_funding_fees_usd=Decimal("30"),
        pending_borrowing_fees_usd=Decimal("20"),
        market=_BTC,
    )
    assert v.pending_fees_usd == Decimal("50")
    # collateral 5000 + pnl 0 - fees 50 = 4950.
    assert v.net_value_usd == Decimal("4950")


def test_value_zero_qty_and_zero_collateral_are_safe():
    # Zero qty -> zero pnl; zero collateral -> zero leverage (no div-by-zero).
    v = aster_perps.value_aster_position(
        size_in_usd=0,
        size_in_tokens=0,
        collateral_amount=0,
        is_long=True,
        mark_price_usd=Decimal("50000"),
        collateral_token_price_usd=Decimal("1"),
        collateral_token_decimals=18,
        index_token_decimals=QTY_DECIMALS,
        market=_BTC,
    )
    assert v.unrealized_pnl_usd == Decimal("0")
    assert v.leverage == Decimal("0")
    assert v.entry_price_usd == Decimal("0")


def test_value_does_not_use_gmx_1e30_scale():
    """Guard: a 1e8-scaled notional must value as ~$ones, proving Aster's scale is
    1e8 not GMX's 1e30 (a 1e30 divisor would make this ~1e-22, i.e. ~0)."""
    v = aster_perps.value_aster_position(
        size_in_usd=100 * 10**8,  # $100 in Aster's 1e8 scale
        size_in_tokens=10**10,  # 1 token
        collateral_amount=100 * 10**18,
        is_long=True,
        mark_price_usd=Decimal("100"),
        collateral_token_price_usd=Decimal("1"),
        collateral_token_decimals=18,
        index_token_decimals=QTY_DECIMALS,
        market=_BTC,
    )
    assert v.size_usd == Decimal("100")  # NOT 1e-22


# --------------------------------------------------------------------------- #
# One-folder-one-row: registry wiring + alias + self-containment
# --------------------------------------------------------------------------- #


def test_registry_supports_aster_and_pancakeswap_alias():
    assert "aster_perps" in PerpsReadRegistry.supported_protocols()
    assert PerpsReadRegistry.has("aster_perps") is True
    # pancakeswap_perps is the deprecated alias -> canonical aster_perps.
    assert PerpsReadRegistry.has("pancakeswap_perps") is True
    assert PerpsReadRegistry.canonical("pancakeswap_perps") == "aster_perps"
    assert PerpsReadRegistry.canonical("aster_perps") == "aster_perps"
    # Both venues coexist (GMX from PR-2, Aster from PR-4).
    assert "gmx_v2" in PerpsReadRegistry.supported_protocols()


def test_registry_resolve_plan_on_bsc():
    plan = PerpsReadRegistry.resolve_plan("aster_perps", PerpsPositionQuery(chain="bsc", wallet_address=_ACCOUNT))
    assert plan is not None
    # markets auto-filled from the spec's markets_for_chain; one call per market.
    assert plan.query.markets == aster_perps._markets_for_chain("bsc")
    assert len(plan.calls) == 3
    # The alias resolves to the same plan shape.
    plan_alias = PerpsReadRegistry.resolve_plan(
        "pancakeswap_perps", PerpsPositionQuery(chain="bsc", wallet_address=_ACCOUNT)
    )
    assert plan_alias is not None
    assert len(plan_alias.calls) == 3


def test_registry_resolve_plan_none_off_chain():
    # Aster is BSC-only; on a chain with no router address the plan is None
    # (the fast "not deployed here" gate discovery relies on).
    assert (
        PerpsReadRegistry.resolve_plan("aster_perps", PerpsPositionQuery(chain="arbitrum", wallet_address=_ACCOUNT))
        is None
    )


def test_registry_routes_metadata_and_value_to_aster():
    meta = PerpsReadRegistry.market_metadata("aster_perps", _BTC, "bsc")
    assert meta is not None and meta.index_token_symbol == "BTC"
    # Via the alias too.
    meta_alias = PerpsReadRegistry.market_metadata("pancakeswap_perps", _ETH, "bsc")
    assert meta_alias is not None and meta_alias.index_token_symbol == "ETH"
    val = PerpsReadRegistry.value_position(
        "aster_perps",
        size_in_usd=(10**10 * 50000 * 10**8) // 10**10,  # 1 BTC @ 50000 -> 1e8 USD scale
        size_in_tokens=10**10,  # 1 BTC (1e10 qty scale)
        collateral_amount=5000 * 10**18,
        is_long=True,
        mark_price_usd=Decimal("50000"),
        collateral_token_price_usd=Decimal("1"),
        collateral_token_decimals=18,
        index_token_decimals=QTY_DECIMALS,
        market=_BTC,
    )
    assert val is not None
    assert val.size_usd == Decimal("50000")  # 1 BTC * 50000


def test_spec_shape_is_per_market_single_role():
    spec = aster_perps.PERPS_READ_SPEC
    # Single contract role (the Diamond router).
    assert spec.contract_kinds == {"router": ("router",)}
    # Per-market venue: markets_for_chain is set (unlike GMX's range read).
    assert spec.markets_for_chain is not None
    assert spec.position_key_prefix == "aster"
