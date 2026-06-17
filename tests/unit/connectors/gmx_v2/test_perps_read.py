"""GMX V2 perps-read parity pins + relocated valuation math (VIB-4930).

The pure spec (``build_calls`` / ``reduce_calls`` / ``market_metadata`` /
``value_position``) reproduces the legacy GMX read+value path byte-for-byte. The
surviving cross-checkable oracles (the framework's own pre-refactor helpers were
deleted in PR-3) are:

* decode      -> ``GMXV2SDK._parse_raw_positions`` (the web3-decoded tuple mapping)
* metadata    -> the connector's ``_gmx_market_metadata`` self-consistency
* valuation   -> frozen known-good vectors (the legacy ``perps_valuer`` fn that
  PR-2 cross-checked against was removed in PR-3, so the math is pinned by literal
  field values computed from the relocated ``value_perps_position``)

This file also owns the relocated GMX mark-to-market math tests (moved here from
``tests/unit/test_perps_valuation.py`` in PR-3), repointed at
``almanak.connectors.gmx_v2.perps_read.value_perps_position`` / ``_GMX_USD_DECIMALS``.
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
from almanak.connectors.gmx_v2 import perps_read as gmx_perps
from almanak.connectors.gmx_v2.perps_read import _GMX_USD_DECIMALS, value_perps_position
from almanak.connectors.gmx_v2.sdk import GMXV2SDK

# Real GMX arbitrum market addresses (checksummed) the metadata tables key on.
_ETH_MARKET = "0x70d95587d40A2caf56bd97485aB3Eec10Bee6336"  # ETH/USD, 18 dec
_BTC_MARKET = "0x47c031236e19d024b42f8AE6780E44A573170703"  # BTC/USD, 8 dec
_DOGE_MARKET = "0x6853EA96FF216fAb11D2d930CE3C508556A4bdc4"  # DOGE/USD: symbol but NO decimals row
_ACCOUNT = to_checksum_address("0x" + "11" * 20)
_USDC = to_checksum_address("0x" + "cc" * 20)


def _props(market, *, size_usd, size_tok, col_amt, is_long, bf=7, ffaps=9):
    """A synthetic Position.Props tuple in the Reader's (addresses, numbers, flags) shape."""
    addresses = (_ACCOUNT, to_checksum_address(market), _USDC)
    # numbers[0..10]: size_usd, size_tok, col_amt, borrowingFactor, fundingFeeAmountPerSize,
    # longTokClaimable, shortTokClaimable, increasedAtBlock, decreasedAtBlock,
    # increasedAtTime, decreasedAtTime
    numbers = (size_usd, size_tok, col_amt, bf, ffaps, 11, 13, 100, 200, 1_700_000_000, 1_700_000_001)
    flags = (is_long,)
    return (addresses, numbers, flags)


def _encode(props_list):
    return "0x" + abi_encode([gmx_perps._GET_ACCOUNT_POSITIONS_OUTPUT], [props_list]).hex()


# --------------------------------------------------------------------------- #
# reduce_calls parity
# --------------------------------------------------------------------------- #


def test_reduce_matches_legacy_parse_raw_positions_active_subset():
    props_list = [
        _props(_ETH_MARKET, size_usd=10**31, size_tok=10**18, col_amt=10**6, is_long=True),
        _props(_BTC_MARKET, size_usd=0, size_tok=0, col_amt=5 * 10**6, is_long=False),  # inactive
        _props(_DOGE_MARKET, size_usd=2 * 10**31, size_tok=3 * 10**9, col_amt=10**18, is_long=False),
    ]
    result = gmx_perps._reduce_gmx_positions(
        PerpsPositionQuery(chain="arbitrum", wallet_address=_ACCOUNT), [_encode(props_list)]
    )
    assert result.ok is True
    # Oracle: parse ALL then keep active (size_in_usd > 0) — the legacy reader's filter.
    legacy = [d for d in GMXV2SDK._parse_raw_positions(props_list) if d["size_in_usd"] > 0]
    assert len(result.positions) == len(legacy) == 2
    for pos, leg in zip(result.positions, legacy, strict=True):
        assert pos.account == leg["account"]
        assert pos.market == leg["market"]
        assert pos.collateral_token == leg["collateral_token"]
        assert pos.size_in_usd == leg["size_in_usd"]
        assert pos.size_in_tokens == leg["size_in_tokens"]
        assert pos.collateral_amount == leg["collateral_amount"]
        assert pos.is_long == leg["is_long"]
        assert pos.borrowing_factor == leg["borrowing_factor"]
        assert pos.funding_fee_amount_per_size == leg["funding_fee_amount_per_size"]
        assert pos.increased_at_time == leg["increased_at_time"]
        assert pos.decreased_at_time == leg["decreased_at_time"]


def test_reduce_failed_read_is_unmeasured_empty_book_is_measured():
    query = PerpsPositionQuery(chain="arbitrum", wallet_address=_ACCOUNT)
    # Empty≠Zero: a failed/missing/garbage read is unmeasured (ok=False)...
    assert gmx_perps._reduce_gmx_positions(query, [None]).ok is False
    assert gmx_perps._reduce_gmx_positions(query, []).ok is False
    assert gmx_perps._reduce_gmx_positions(query, ["0xdeadbeef"]).ok is False
    # ...but a successful decode of an empty array is a measured empty book.
    empty = gmx_perps._reduce_gmx_positions(query, [_encode([])])
    assert empty.ok is True
    assert empty.positions == ()


# --------------------------------------------------------------------------- #
# build_calls
# --------------------------------------------------------------------------- #


def test_build_calls_targets_reader_with_datastore_arg():
    reader = to_checksum_address("0x" + "22" * 20)
    data_store = to_checksum_address("0x" + "33" * 20)
    query = PerpsPositionQuery(
        chain="arbitrum",
        wallet_address=_ACCOUNT,
        targets={"reader": reader, "data_store": data_store},
    )
    calls = gmx_perps._build_gmx_calls(query)
    assert len(calls) == 1
    assert calls[0].to == reader
    selector_hex = gmx_perps._GET_ACCOUNT_POSITIONS_SELECTOR.hex()
    assert calls[0].data.startswith("0x" + selector_hex)
    args = bytes.fromhex(calls[0].data[2 + 8 :])  # strip "0x" + 4-byte selector
    ds, acct, start, end = abi_decode(["address", "address", "uint256", "uint256"], args)
    assert to_checksum_address(ds) == data_store
    assert to_checksum_address(acct) == _ACCOUNT
    assert (start, end) == (0, gmx_perps._MAX_POSITION_RANGE)


def test_build_calls_empty_when_a_target_role_is_unresolved():
    query = PerpsPositionQuery(chain="arbitrum", wallet_address=_ACCOUNT, targets={"reader": "0xR"})
    assert gmx_perps._build_gmx_calls(query) == []  # data_store missing -> fail closed


# --------------------------------------------------------------------------- #
# market_metadata parity
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("market,symbol,decimals", [(_ETH_MARKET, "ETH", 18), (_BTC_MARKET, "BTC", 8)])
def test_market_metadata_resolves_symbol_and_decimals(market, symbol, decimals):
    # The framework's pre-refactor ``_resolve_perps_index_token`` /
    # ``_get_perps_index_decimals`` helpers (the PR-2 oracle) were deleted in PR-3;
    # the relocated lookup is pinned directly against the known GMX market values.
    meta = gmx_perps._gmx_market_metadata(market, "arbitrum")
    assert meta is not None
    assert meta.index_token_symbol == symbol
    assert meta.index_token_decimals == decimals
    # Case-insensitive on the market address.
    assert gmx_perps._gmx_market_metadata(market.lower(), "arbitrum") == meta


def test_market_metadata_resolves_previously_uncatalogued_market_decimals():
    # DOGE is explicitly catalogued now so the adapter never falls back to 18 decimals.
    meta = gmx_perps._gmx_market_metadata(_DOGE_MARKET, "arbitrum")
    assert meta is not None
    assert meta.index_token_symbol == "DOGE"
    assert meta.index_token_decimals == 8


def test_market_metadata_none_for_unknown_market_or_chain():
    # Unknown market / unknown chain -> None.
    assert gmx_perps._gmx_market_metadata("0x" + "ab" * 20, "arbitrum") is None
    assert gmx_perps._gmx_market_metadata(_ETH_MARKET, "ethereum") is None


# --------------------------------------------------------------------------- #
# value_position parity — frozen known-good vectors
# --------------------------------------------------------------------------- #

# Before PR-3 these were cross-checked against the framework's
# ``perps_valuer.value_perps_position`` (PR-2 oracle). That legacy fn was removed
# in PR-3 when the math moved into the connector, so the math is now pinned by the
# expected ``PerpsPositionValue`` field values (computed once from the relocated
# ``value_perps_position`` and asserted as literals). Decimal equality is
# value-based, so the normalised literals below compare equal to the raw quotients.
# Keyed by (is_long, index_decimals); ``size_in_tokens`` per decimals is in
# ``_FROZEN_SIZE_TOK``. Common inputs: size_in_usd=1500e30, collateral=300 USDC,
# mark=2517.5, collateral_price=1.0, collateral_decimals=6, market="0xMarket".
_FROZEN_SIZE_TOK = {18: 7 * 10**17, 8: 3 * 10**7, 9: 5 * 10**8}
_FROZEN_VALUE_VECTORS: dict[tuple[bool, int], PerpsPositionValue] = {
    (True, 18): PerpsPositionValue(
        market="0xMarket",
        is_long=True,
        size_usd=Decimal("1500"),
        collateral_value_usd=Decimal("300.0"),
        entry_price_usd=Decimal("2142.857142857142857142857143"),
        mark_price_usd=Decimal("2517.5"),
        unrealized_pnl_usd=Decimal("262.2499999999999999999999999"),
        pending_fees_usd=Decimal("0"),
        net_value_usd=Decimal("562.2499999999999999999999999"),
        leverage=Decimal("5"),
    ),
    (True, 8): PerpsPositionValue(
        market="0xMarket",
        is_long=True,
        size_usd=Decimal("1500"),
        collateral_value_usd=Decimal("300.0"),
        entry_price_usd=Decimal("5000"),
        mark_price_usd=Decimal("2517.5"),
        unrealized_pnl_usd=Decimal("-744.75"),
        pending_fees_usd=Decimal("0"),
        net_value_usd=Decimal("-444.75"),
        leverage=Decimal("5"),
    ),
    (True, 9): PerpsPositionValue(
        market="0xMarket",
        is_long=True,
        size_usd=Decimal("1500"),
        collateral_value_usd=Decimal("300.0"),
        entry_price_usd=Decimal("3000"),
        mark_price_usd=Decimal("2517.5"),
        unrealized_pnl_usd=Decimal("-241.25"),
        pending_fees_usd=Decimal("0"),
        net_value_usd=Decimal("58.75"),
        leverage=Decimal("5"),
    ),
    (False, 18): PerpsPositionValue(
        market="0xMarket",
        is_long=False,
        size_usd=Decimal("1500"),
        collateral_value_usd=Decimal("300.0"),
        entry_price_usd=Decimal("2142.857142857142857142857143"),
        mark_price_usd=Decimal("2517.5"),
        unrealized_pnl_usd=Decimal("-262.2499999999999999999999999"),
        pending_fees_usd=Decimal("0"),
        net_value_usd=Decimal("37.7500000000000000000000001"),
        leverage=Decimal("5"),
    ),
    (False, 8): PerpsPositionValue(
        market="0xMarket",
        is_long=False,
        size_usd=Decimal("1500"),
        collateral_value_usd=Decimal("300.0"),
        entry_price_usd=Decimal("5000"),
        mark_price_usd=Decimal("2517.5"),
        unrealized_pnl_usd=Decimal("744.75"),
        pending_fees_usd=Decimal("0"),
        net_value_usd=Decimal("1044.75"),
        leverage=Decimal("5"),
    ),
    (False, 9): PerpsPositionValue(
        market="0xMarket",
        is_long=False,
        size_usd=Decimal("1500"),
        collateral_value_usd=Decimal("300.0"),
        entry_price_usd=Decimal("3000"),
        mark_price_usd=Decimal("2517.5"),
        unrealized_pnl_usd=Decimal("241.25"),
        pending_fees_usd=Decimal("0"),
        net_value_usd=Decimal("541.25"),
        leverage=Decimal("5"),
    ),
}


@pytest.mark.parametrize("is_long", [True, False])
@pytest.mark.parametrize("index_dec", [18, 8, 9])
def test_value_matches_frozen_vectors(is_long, index_dec):
    value = value_perps_position(
        size_in_usd=1500 * 10**30,
        size_in_tokens=_FROZEN_SIZE_TOK[index_dec],
        collateral_amount=300 * 10**6,
        is_long=is_long,
        mark_price_usd=Decimal("2517.5"),
        collateral_token_price_usd=Decimal("1.0"),
        collateral_token_decimals=6,
        index_token_decimals=index_dec,
        market="0xMarket",
    )
    assert value == _FROZEN_VALUE_VECTORS[(is_long, index_dec)]


# --------------------------------------------------------------------------- #
# registry wiring
# --------------------------------------------------------------------------- #


def test_registry_routes_to_gmx_spec():
    assert "gmx_v2" in PerpsReadRegistry.supported_protocols()
    meta = PerpsReadRegistry.market_metadata("gmx_v2", _ETH_MARKET, "arbitrum")
    assert meta is not None and meta.index_token_symbol == "ETH"
    val = PerpsReadRegistry.value_position(
        "gmx_v2",
        size_in_usd=10**30,
        size_in_tokens=10**18,
        collateral_amount=10**6,
        is_long=True,
        mark_price_usd=Decimal("1"),
        collateral_token_price_usd=Decimal("1"),
        collateral_token_decimals=6,
        index_token_decimals=18,
        market="0xM",
    )
    assert val is not None


# --------------------------------------------------------------------------- #
# Relocated GMX mark-to-market math (moved from tests/unit/test_perps_valuation.py
# in PR-3; repointed at the connector's value_perps_position / _GMX_USD_DECIMALS).
# --------------------------------------------------------------------------- #


class TestValuePerpsPosition:
    """Test the mark-to-market math for GMX V2 positions."""

    def _make_long_eth(
        self,
        *,
        size_usd: int = 10_000,
        tokens: float = 5.0,
        collateral: int = 2000,
        mark_price: Decimal = Decimal("2000"),
        collateral_price: Decimal = Decimal("1"),
        collateral_decimals: int = 6,
        index_decimals: int = 18,
        funding: Decimal = Decimal("0"),
        borrowing: Decimal = Decimal("0"),
    ) -> PerpsPositionValue:
        """Helper: create a long ETH/USD position valued at given mark price."""
        return value_perps_position(
            size_in_usd=size_usd * 10**_GMX_USD_DECIMALS,
            size_in_tokens=int(tokens * 10**index_decimals),
            collateral_amount=collateral * 10**collateral_decimals,
            is_long=True,
            mark_price_usd=mark_price,
            collateral_token_price_usd=collateral_price,
            collateral_token_decimals=collateral_decimals,
            index_token_decimals=index_decimals,
            pending_funding_fees_usd=funding,
            pending_borrowing_fees_usd=borrowing,
            market="ETH/USD",
        )

    def test_long_breakeven(self):
        """Long at entry = mark: PnL should be ~0."""
        result = self._make_long_eth(size_usd=10_000, tokens=5.0, mark_price=Decimal("2000"))
        assert result.is_long is True
        assert result.market == "ETH/USD"
        assert result.size_usd == Decimal("10000")
        # Entry = 10000/5 = 2000, mark = 2000 => pnl ≈ 0
        assert abs(result.unrealized_pnl_usd) < Decimal("0.01")

    def test_long_profit(self):
        """Long with price increase: positive PnL."""
        result = self._make_long_eth(size_usd=10_000, tokens=5.0, mark_price=Decimal("2200"))
        # Entry = 2000, mark = 2200, tokens = 5
        # PnL = 5 * (2200 - 2000) = 1000
        assert result.unrealized_pnl_usd == Decimal("1000")
        assert result.net_value_usd == Decimal("3000")  # 2000 collateral + 1000 pnl

    def test_long_loss(self):
        """Long with price decrease: negative PnL."""
        result = self._make_long_eth(size_usd=10_000, tokens=5.0, mark_price=Decimal("1800"))
        # PnL = 5 * (1800 - 2000) = -1000
        assert result.unrealized_pnl_usd == Decimal("-1000")
        assert result.net_value_usd == Decimal("1000")  # 2000 - 1000

    def test_short_breakeven(self):
        """Short at entry = mark: PnL should be ~0."""
        result = value_perps_position(
            size_in_usd=10_000 * 10**_GMX_USD_DECIMALS,
            size_in_tokens=5 * 10**18,
            collateral_amount=2000 * 10**6,
            is_long=False,
            mark_price_usd=Decimal("2000"),
            collateral_token_price_usd=Decimal("1"),
            collateral_token_decimals=6,
            index_token_decimals=18,
        )
        assert result.is_long is False
        assert abs(result.unrealized_pnl_usd) < Decimal("0.01")

    def test_short_profit(self):
        """Short with price decrease: positive PnL."""
        result = value_perps_position(
            size_in_usd=10_000 * 10**_GMX_USD_DECIMALS,
            size_in_tokens=5 * 10**18,
            collateral_amount=2000 * 10**6,
            is_long=False,
            mark_price_usd=Decimal("1800"),
            collateral_token_price_usd=Decimal("1"),
            collateral_token_decimals=6,
            index_token_decimals=18,
        )
        # PnL = 5 * (2000 - 1800) = 1000
        assert result.unrealized_pnl_usd == Decimal("1000")

    def test_short_loss(self):
        """Short with price increase: negative PnL."""
        result = value_perps_position(
            size_in_usd=10_000 * 10**_GMX_USD_DECIMALS,
            size_in_tokens=5 * 10**18,
            collateral_amount=2000 * 10**6,
            is_long=False,
            mark_price_usd=Decimal("2200"),
            collateral_token_price_usd=Decimal("1"),
            collateral_token_decimals=6,
            index_token_decimals=18,
        )
        # PnL = 5 * (2000 - 2200) = -1000
        assert result.unrealized_pnl_usd == Decimal("-1000")

    def test_fees_reduce_net_value(self):
        """Pending fees reduce net value."""
        result = self._make_long_eth(
            size_usd=10_000,
            tokens=5.0,
            mark_price=Decimal("2000"),
            funding=Decimal("50"),
            borrowing=Decimal("30"),
        )
        assert result.pending_fees_usd == Decimal("80")
        # Collateral (2000) + PnL (0) - fees (80) = 1920
        assert result.net_value_usd == Decimal("1920")

    def test_leverage_calculation(self):
        """Leverage = notional / collateral value."""
        result = self._make_long_eth(size_usd=10_000, tokens=5.0, collateral=2000)
        # Size = 10000, collateral = 2000 * $1 = 2000 => leverage = 5
        assert result.leverage == Decimal("5")

    def test_non_usd_collateral(self):
        """Collateral in ETH (non-stablecoin) valued at market price."""
        result = value_perps_position(
            size_in_usd=10_000 * 10**_GMX_USD_DECIMALS,
            size_in_tokens=5 * 10**18,
            collateral_amount=1 * 10**18,  # 1 ETH as collateral
            is_long=True,
            mark_price_usd=Decimal("2000"),
            collateral_token_price_usd=Decimal("2000"),  # ETH price
            collateral_token_decimals=18,
            index_token_decimals=18,
        )
        assert result.collateral_value_usd == Decimal("2000")
        assert result.leverage == Decimal("5")

    def test_btc_position_8_decimals(self):
        """BTC market uses 8 decimals for index token."""
        result = value_perps_position(
            size_in_usd=100_000 * 10**_GMX_USD_DECIMALS,
            size_in_tokens=int(1.0 * 10**8),  # 1 BTC (8 decimals)
            collateral_amount=10_000 * 10**6,  # 10k USDC
            is_long=True,
            mark_price_usd=Decimal("100000"),
            collateral_token_price_usd=Decimal("1"),
            collateral_token_decimals=6,
            index_token_decimals=8,
            market="BTC/USD",
        )
        assert result.size_usd == Decimal("100000")
        assert result.entry_price_usd == Decimal("100000")
        assert abs(result.unrealized_pnl_usd) < Decimal("0.01")

    def test_zero_size_returns_zero_pnl(self):
        """Position with zero size has zero PnL."""
        result = value_perps_position(
            size_in_usd=0,
            size_in_tokens=0,
            collateral_amount=1000 * 10**6,
            is_long=True,
            mark_price_usd=Decimal("2000"),
            collateral_token_price_usd=Decimal("1"),
            collateral_token_decimals=6,
            index_token_decimals=18,
        )
        assert result.unrealized_pnl_usd == Decimal("0")
        assert result.leverage == Decimal("0")

    def test_zero_collateral_zero_leverage(self):
        """Zero collateral results in zero leverage (not division by zero)."""
        result = value_perps_position(
            size_in_usd=10_000 * 10**_GMX_USD_DECIMALS,
            size_in_tokens=5 * 10**18,
            collateral_amount=0,
            is_long=True,
            mark_price_usd=Decimal("2000"),
            collateral_token_price_usd=Decimal("1"),
            collateral_token_decimals=6,
            index_token_decimals=18,
        )
        assert result.leverage == Decimal("0")
