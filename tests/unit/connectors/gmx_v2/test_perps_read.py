"""GMX V2 perps-read parity pins + relocated valuation math (VIB-4930).

The pure spec (``build_calls`` / ``reduce_calls`` / ``market_metadata`` /
``value_position``) reproduces the legacy GMX read+value path byte-for-byte. The
surviving cross-checkable oracles (the framework's own pre-refactor helpers were
deleted in PR-3) are:

* decode      -> ``GMXV2SDK._parse_raw_positions`` (the web3-decoded tuple mapping)
* metadata    -> ``_gmx_market_metadata`` against the process's venue-verified
  catalog (address-first: there is no static market table any more, so the
  tests prime ``market_catalog`` from the audited fixture rows and pin the
  None-when-unverified contract)
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
from tests.unit.connectors.gmx_v2.market_fixtures import market_record, prime_catalog

# Real GMX arbitrum market addresses (checksummed) the verified catalog keys on.
_ETH_MARKET = "0x70d95587d40A2caf56bd97485aB3Eec10Bee6336"  # ETH/USD, 18 dec
_BTC_MARKET = "0x47c031236e19d024b42f8AE6780E44A573170703"  # BTC/USD, 8 dec
_DOGE_MARKET = "0x6853EA96FF216fAb11D2d930CE3C508556A4bdc4"  # DOGE/USD, 8 dec (synthetic index)
_ACCOUNT = to_checksum_address("0x" + "11" * 20)
_USDC = to_checksum_address("0x" + "cc" * 20)


def _props(market, *, size_usd, size_tok, col_amt, is_long, bf=7, ffaps=9, pia=-5):
    """A synthetic Position.Props tuple in the Reader's (addresses, numbers, flags) shape.

    Numbers follows the CURRENT 10-field GMX struct (VIB-5289): index 3 is the
    signed ``int256 pendingImpactAmount``; the legacy block fields are gone.
    """
    addresses = (_ACCOUNT, to_checksum_address(market), _USDC)
    # numbers[0..9]: size_usd, size_tok, col_amt, pendingImpactAmount[int256],
    # borrowingFactor, fundingFeeAmountPerSize, longTokClaimable, shortTokClaimable,
    # increasedAtTime, decreasedAtTime
    numbers = (size_usd, size_tok, col_amt, pia, bf, ffaps, 11, 13, 1_700_000_000, 1_700_000_001)
    flags = (is_long,)
    return (addresses, numbers, flags)


def _encode(props_list):
    return "0x" + abi_encode([gmx_perps._GET_ACCOUNT_POSITIONS_OUTPUT], [props_list]).hex()


# --------------------------------------------------------------------------- #
# reduce_calls parity
# --------------------------------------------------------------------------- #


def test_reduce_decodes_current_10_field_numbers_struct():
    """VIB-5289: decode the CURRENT 10-field ``Position.Numbers`` (``int256``
    ``pendingImpactAmount`` at index 3). The prior 11-``uint256`` ABI ran
    ``eth_abi`` out of bytes on the real return → ``ok=False`` for EVERY live
    position. Field-shift vs the stale layout: ``borrowing_factor`` 3→4,
    ``funding_fee_amount_per_size`` 4→5, ``increased_at_time`` 9→8,
    ``decreased_at_time`` 10→9; the valuation-critical size/collateral/is_long
    stay at 0/1/2.

    Validated against ground truth directly. (The ``GMXV2SDK`` /
    ``GMXv2Adapter`` ``_parse_raw_positions`` oracles + the vendored
    ``reader.json`` were on the stale 11-field layout until VIB-5950 completed the
    fix; they now share this 10-field layout — see
    ``test_reader_json_position_decode.py``.)
    """
    props_list = [
        _props(_ETH_MARKET, size_usd=10**31, size_tok=10**18, col_amt=10**6, is_long=True, bf=71, ffaps=91),
        _props(_BTC_MARKET, size_usd=0, size_tok=0, col_amt=5 * 10**6, is_long=False),  # inactive
        _props(_DOGE_MARKET, size_usd=2 * 10**31, size_tok=3 * 10**9, col_amt=10**18, is_long=False),
    ]
    result = gmx_perps._reduce_gmx_positions(
        PerpsPositionQuery(chain="arbitrum", wallet_address=_ACCOUNT), [_encode(props_list)]
    )
    assert result.ok is True
    # Inactive (size_in_usd == 0) is filtered; ETH + DOGE remain.
    assert len(result.positions) == 2
    eth, doge = result.positions
    # Valuation-critical fields (indices 0/1/2 + flags) decode correctly.
    assert eth.account == _ACCOUNT
    assert eth.market == to_checksum_address(_ETH_MARKET)
    assert eth.collateral_token == _USDC
    assert (eth.size_in_usd, eth.size_in_tokens, eth.collateral_amount) == (10**31, 10**18, 10**6)
    assert eth.is_long is True
    # Shifted fields land on their NEW indices (4/5/8/9), not the old (3/4/9/10)
    # which under the 11-field layout would have read pendingImpactAmount as
    # borrowing_factor.
    assert eth.borrowing_factor == 71
    assert eth.funding_fee_amount_per_size == 91
    assert eth.increased_at_time == 1_700_000_000
    assert eth.decreased_at_time == 1_700_000_001
    assert doge.is_long is False and doge.size_in_usd == 2 * 10**31


def test_numbers_abi_pins_current_10_field_struct():
    """VIB-5289 regression pin: the ``Position.Numbers`` ABI must be the current
    10-field struct with the signed ``int256 pendingImpactAmount`` at index 3 —
    NOT the stale 11-``uint256`` layout that decoded ZERO live GMX positions
    (every read failed → ``ok=False``). A precise structural assertion so a
    revert (or a copy-paste back to ``["uint256"] * 11``) trips here."""
    fields = gmx_perps._POSITION_NUMBERS.strip("()").split(",")
    assert len(fields) == 10, "GMX Position.Numbers is 10 fields, not 11"
    assert fields[3] == "int256", "index 3 is the signed pendingImpactAmount"
    assert fields.count("int256") == 1
    assert fields.count("uint256") == 9


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


@pytest.mark.parametrize(
    ("market", "label", "symbol", "decimals"), [(_ETH_MARKET, "ETH/USD", "ETH", 18), (_BTC_MARKET, "BTC/USD", "BTC", 8)]
)
def test_market_metadata_resolves_symbol_and_decimals(market, label, symbol, decimals):
    # The framework's pre-refactor ``_resolve_perps_index_token`` /
    # ``_get_perps_index_decimals`` helpers (the PR-2 oracle) were deleted in PR-3.
    # Address-first: metadata is served ONLY from the process's venue-verified
    # catalog (VIB-6561), so the test primes it exactly as dynamic market
    # resolution would during a live compile, then pins the known GMX values.
    prime_catalog(market_record("arbitrum", label), chain="arbitrum")
    meta = gmx_perps._gmx_market_metadata(market, "arbitrum")
    assert meta is not None
    assert meta.index_token_symbol == symbol
    assert meta.index_token_decimals == decimals
    # Case-insensitive on the market address.
    assert gmx_perps._gmx_market_metadata(market.lower(), "arbitrum") == meta


def test_market_metadata_resolves_any_venue_verified_market():
    # DOGE (a synthetic-index market with 8 decimals) resolves once venue-verified
    # in this process — no curated decimals row exists to fall back to, so a
    # default-18 misread (the 2026-08-07 XMR class) can never come from here.
    prime_catalog(market_record("arbitrum", "DOGE/USD"), chain="arbitrum")
    meta = gmx_perps._gmx_market_metadata(_DOGE_MARKET, "arbitrum")
    assert meta is not None
    assert meta.index_token_symbol == "DOGE"
    assert meta.index_token_decimals == 8


def test_market_metadata_none_for_unknown_market_or_chain():
    prime_catalog(market_record("arbitrum", "ETH/USD"), chain="arbitrum")
    # Unknown market / unknown chain -> None.
    assert gmx_perps._gmx_market_metadata("0x" + "ab" * 20, "arbitrum") is None
    assert gmx_perps._gmx_market_metadata(_ETH_MARKET, "ethereum") is None


def test_market_metadata_none_until_venue_verified():
    """``None`` means "not verified in this process", never "unsupported".

    A real market this process has not verified yields ``None`` — the framework
    falls back to the strategy-provided value rather than guessing decimals.
    This is the address-first replacement for the deleted static table: absence
    is UNMEASURED, not a rejection (the misread that motivated deleting
    ``GMX_V2_MARKETS``).
    """
    assert gmx_perps._gmx_market_metadata(_ETH_MARKET, "arbitrum") is None


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
    prime_catalog(market_record("arbitrum", "ETH/USD"), chain="arbitrum")
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


# ---------------------------------------------------------------------------
# Backtest observation parity: _gmx_simulate_position
# ---------------------------------------------------------------------------


def _simulated(**overrides):
    from almanak.connectors._strategy_base.perps_read_base import SimulatedPerpPosition

    base = {
        "chain": "arbitrum",
        "account": _ACCOUNT,
        "market": "ETH/USD",
        "is_long": True,
        "size_usd": Decimal("10"),
        "size_tokens": Decimal("0.005"),
        "collateral_token": "USDC",
        "collateral_token_address": "0xaf88d065e77c8cc2239327c5edb3a432268e5831",
        "collateral_token_decimals": 6,
        "collateral_amount": Decimal("5"),
        "opened_at": 1_700_000_000,
    }
    base.update(overrides)
    return SimulatedPerpPosition(**base)


class TestSimulatePosition:
    """Connector-owned projection of an engine-simulated position (backtest
    observation parity, address-first): the market identity must be a
    venue-verified catalog row — reached by the authored market-token address
    or an unambiguous remembered label — and GMX fixed-point scaling is applied
    exactly (USD at 30 decimals, sizes at index decimals, collateral at
    collateral-token decimals). A catalog miss is "not verified in this
    process": the projection refuses (None) rather than guess an address or a
    scale, and the framework serves an unmeasured read."""

    def test_projects_verified_label_with_gmx_scaling(self):
        prime_catalog(market_record("arbitrum", "ETH/USD"), chain="arbitrum")

        row = gmx_perps._gmx_simulate_position(_simulated())

        assert row is not None
        assert row.market == _ETH_MARKET
        assert row.collateral_token == "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
        assert row.account == _ACCOUNT
        assert row.is_long is True
        assert row.size_in_usd == 10 * 10**_GMX_USD_DECIMALS
        assert row.size_in_tokens == int(Decimal("0.005") * 10**18)  # ETH index: 18 dec
        assert row.collateral_amount == 5 * 10**6
        assert row.increased_at_time == 1_700_000_000
        assert row.is_active is True
        assert row.key_prefix == "gmx"

    def test_accepts_verified_market_address_verbatim(self):
        # The address-first contract: the demo authors the market-token
        # address, so the stamped identity arrives as an address; the served
        # row carries the verified record's canonical checksummed form.
        prime_catalog(market_record("arbitrum", "ETH/USD"), chain="arbitrum")

        row = gmx_perps._gmx_simulate_position(_simulated(market=_ETH_MARKET.lower()))

        assert row is not None
        assert row.market == _ETH_MARKET

    def test_refuses_unverified_market(self):
        # Empty catalog: a miss means "not verified in this process", never
        # "does not exist" — the projection refuses for BOTH identity forms.
        assert gmx_perps._gmx_simulate_position(_simulated()) is None
        assert gmx_perps._gmx_simulate_position(_simulated(market=_ETH_MARKET)) is None

    def test_refuses_unknown_label_and_foreign_address_when_catalog_is_primed(self):
        prime_catalog(market_record("arbitrum", "ETH/USD"), chain="arbitrum")
        assert gmx_perps._gmx_simulate_position(_simulated(market="NOTLISTED/USD")) is None
        assert gmx_perps._gmx_simulate_position(_simulated(market="0x" + "99" * 20)) is None

    def test_refuses_unresolved_collateral_identity(self):
        prime_catalog(market_record("arbitrum", "ETH/USD"), chain="arbitrum")
        assert gmx_perps._gmx_simulate_position(_simulated(collateral_token_address=None)) is None
        assert gmx_perps._gmx_simulate_position(_simulated(collateral_token_decimals=None)) is None
        assert gmx_perps._gmx_simulate_position(_simulated(collateral_amount=None)) is None

    def test_registry_dispatches_simulate_position(self):
        prime_catalog(market_record("arbitrum", "ETH/USD"), chain="arbitrum")
        row = PerpsReadRegistry.simulate_position("gmx_v2", _simulated(market=_ETH_MARKET))
        assert row is not None
        assert row.market == _ETH_MARKET
        # An unregistered venue refuses through the registry too.
        assert PerpsReadRegistry.simulate_position("uniswap_v3", _simulated()) is None
