"""Unit tests for the paper-engine position query functions.

The six ``query_*_positions`` entry points (uniswap / gmx / aave, async +
sync) resolve connector-owned addresses through ``AddressRegistry`` and then
read on-chain positions. Uniswap / Aave enumerate via per-item ``_query_*``
RPC helpers, which these tests patch (never the registry). GMX is
address-first: ONE ``Reader.getAccountPositions`` range read through the
production ``PERPS_READ_SPEC`` returns the wallet's whole book, so its tests
stub at the ``web3.eth.call`` boundary with a real ABI-encoded return blob —
the production call planner, decoder, filters, and decimal resolution all run
for real without a node.
"""

from unittest.mock import AsyncMock, patch

import pytest
from eth_abi import encode as abi_encode
from web3 import Web3

import almanak.framework.backtesting.paper.position_queries as pq
from almanak.connectors.gmx_v2 import market_catalog
from almanak.connectors.gmx_v2.market_metadata import ResolvedGmxMarket
from almanak.connectors.gmx_v2.perps_read import (
    _GET_ACCOUNT_POSITIONS_OUTPUT,
    _GET_ACCOUNT_POSITIONS_SELECTOR,
)

_WALLET = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
_MODULE = "almanak.framework.backtesting.paper.position_queries"


class _FakeWeb3:
    """Just enough web3 surface for the query functions."""

    @staticmethod
    def to_checksum_address(value: str) -> str:
        return Web3.to_checksum_address(value)


@pytest.fixture
def web3() -> _FakeWeb3:
    return _FakeWeb3()


def _univ3_position(*, token_id: int, fee: int, liquidity: int) -> "pq.UniswapV3Position":
    return pq.UniswapV3Position(
        token_id=token_id,
        nonce=0,
        operator="0x" + "00" * 20,
        token0="0x" + "11" * 20,
        token1="0x" + "22" * 20,
        fee=fee,
        tick_lower=-100,
        tick_upper=100,
        liquidity=liquidity,
        fee_growth_inside0_last_x128=0,
        fee_growth_inside1_last_x128=0,
        tokens_owed0=0,
        tokens_owed1=0,
    )


class TestUniswapV3PositionQueries:
    @pytest.mark.asyncio
    async def test_async_no_positions(self, web3):
        """Default position-manager resolution + zero-balance early exit."""
        with patch(f"{_MODULE}._query_balance_of", AsyncMock(return_value=0)) as balance_mock:
            positions = await pq.query_uniswap_v3_positions(_WALLET, web3, chain="ethereum")
        assert positions == []
        contract = balance_mock.call_args.args[1]
        assert contract.startswith("0x")  # registry-resolved position manager

    @pytest.mark.asyncio
    async def test_async_enumerates_and_skips_failed_token_ids(self, web3):
        """The token-id loop appends found positions and warns on None ids."""
        position = _univ3_position(token_id=101, fee=500, liquidity=1_000)
        with (
            patch(f"{_MODULE}._query_balance_of", AsyncMock(return_value=2)),
            patch(
                f"{_MODULE}._query_token_of_owner_by_index",
                AsyncMock(side_effect=[101, None]),
            ),
            patch(f"{_MODULE}._query_position", AsyncMock(return_value=position)),
        ):
            positions = await pq.query_uniswap_v3_positions(_WALLET, web3, chain="ethereum")
        assert positions == [position]

    @pytest.mark.asyncio
    async def test_async_unsupported_chain_raises(self, web3):
        with pytest.raises(ValueError, match="Unsupported chain: notachain"):
            await pq.query_uniswap_v3_positions(_WALLET, web3, chain="notachain")

    def test_sync_no_positions(self, web3):
        with patch(f"{_MODULE}._query_balance_of_sync", return_value=0):
            assert pq.query_uniswap_v3_positions_sync(_WALLET, web3, chain="ethereum") == []

    def test_sync_enumerates_positions(self, web3):
        position = _univ3_position(token_id=7, fee=3000, liquidity=5)
        with (
            patch(f"{_MODULE}._query_balance_of_sync", return_value=1),
            patch(f"{_MODULE}._query_token_of_owner_by_index_sync", return_value=7),
            patch(f"{_MODULE}._query_position_sync", return_value=position),
        ):
            assert pq.query_uniswap_v3_positions_sync(_WALLET, web3, chain="ethereum") == [position]


# ---------------------------------------------------------------------------
# GMX V2 — the address-first range read
# ---------------------------------------------------------------------------

# Real Arbitrum addresses so the position-key derivation and the entry-price
# decimal lookups run against production-shaped inputs.
_ETH_MARKET = "0x70d95587d40A2caf56bd97485aB3Eec10Bee6336"  # ETH/USD, 18 dec (static table)
_BTC_MARKET = "0x47c031236e19d024b42f8AE6780E44A573170703"  # BTC/USD, 8 dec (static table)
_USDC = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
# A market NOT in the static decimals table — the XMR class the range read
# exists to serve (the venue can list markets no catalogue ever named).
_UNCATALOGUED_MARKET = Web3.to_checksum_address("0x" + "77" * 20)


def _props(market: str, collateral: str, *, size_usd: int, size_tok: int, col_amt: int, is_long: bool):
    """One ``Position.Props`` tuple in the Reader's (addresses, numbers, flags) shape."""
    addresses = (Web3.to_checksum_address(_WALLET), market, collateral)
    # Current 10-field Position.Numbers (index 3 = signed pendingImpactAmount).
    numbers = (size_usd, size_tok, col_amt, -5, 7, 9, 11, 13, 1_700_000_000, 0)
    return (addresses, numbers, (is_long,))


def _encode_book(props_list) -> bytes:
    """ABI-encode a ``getAccountPositions`` return exactly as the chain would."""
    return abi_encode([_GET_ACCOUNT_POSITIONS_OUTPUT], [list(props_list)])


_TWO_POSITION_BOOK = (
    _props(_ETH_MARKET, _USDC, size_usd=5000 * 10**30, size_tok=int(2.5 * 10**18), col_amt=1000 * 10**6, is_long=True),
    _props(
        _BTC_MARKET, _USDC, size_usd=10000 * 10**30, size_tok=int(0.25 * 10**8), col_amt=2000 * 10**6, is_long=False
    ),
    # Inactive (size 0) — the production reducer must filter it out.
    _props(_ETH_MARKET, _USDC, size_usd=0, size_tok=0, col_amt=5 * 10**6, is_long=False),
)


class _GmxEth:
    """A ``web3.eth`` stand-in answering the single range read."""

    def __init__(self, blob: bytes | None = None, exc: Exception | None = None):
        self.blob = blob
        self.exc = exc
        self.calls: list[dict] = []

    def call(self, params: dict) -> bytes:
        self.calls.append(params)
        if self.exc is not None:
            raise self.exc
        return self.blob


class _AsyncGmxEth(_GmxEth):
    async def call(self, params: dict) -> bytes:  # type: ignore[override]
        return _GmxEth.call(self, params)


class _GmxWeb3(_FakeWeb3):
    def __init__(self, eth):
        self.eth = eth


class TestGmxUndeployedChain:
    def test_unsupported_chain_raises_the_documented_valueerror(self):
        """Parity pin: ``PerpsReadRegistry.resolve_plan`` returns ``None`` for a
        chain GMX V2 is not deployed on, and this function's documented public
        contract (unchanged from the pre-range-read implementation, which raised
        from the address lookup) converts that into ``ValueError`` — callers
        that probe arbitrary chains guard with try/except, exactly as before.
        """
        with pytest.raises(ValueError, match="Unsupported chain"):
            pq.query_gmx_positions_sync(_WALLET, _GmxWeb3(_GmxEth(blob="0x")), chain="base")


class TestGmxPositionQueries:
    """The GMX paper reader is ONE production-spec range read, post-filtered.

    Address-first: there is no market catalogue to enumerate, so the old
    market × collateral × direction brute-force (which structurally missed
    every uncatalogued market) is gone. ``markets`` / ``collateral_tokens``
    are optional POST-filters over the decoded book; ``None`` means the whole
    book.
    """

    def test_sync_whole_book_default(self):
        """No filters -> every active position, from ONE Reader call."""
        from tests.unit.connectors.gmx_v2.market_fixtures import prime_catalog

        # Entry price scales by the venue-verified decimals (address-first: no
        # static table) — prime the audited snapshot as a live compile would.
        prime_catalog()
        eth = _GmxEth(blob=_encode_book(_TWO_POSITION_BOOK))
        positions = pq.query_gmx_positions_sync(_WALLET, _GmxWeb3(eth), chain="arbitrum")

        assert [p.market for p in positions] == [_ETH_MARKET, _BTC_MARKET]
        assert [p.is_long for p in positions] == [True, False]

        # Exactly one call, to the registry-resolved Reader, with the
        # production selector — the PERPS_READ_SPEC plan, not a bespoke one.
        assert len(eth.calls) == 1
        assert eth.calls[0]["to"] == pq._contract_address("gmx_v2", "arbitrum", "reader")
        assert eth.calls[0]["data"].startswith("0x" + _GET_ACCOUNT_POSITIONS_SELECTOR.hex())

        eth_pos = positions[0]
        assert eth_pos.position_key == pq._compute_position_key(eth_pos.account, _ETH_MARKET, _USDC, True)
        assert eth_pos.size_in_usd == 5000 * 10**30
        assert eth_pos.collateral_amount == 1000 * 10**6
        # Verified-catalog decimals (18 for ETH): 5000e30 * 1e18 / 2.5e18.
        assert eth_pos.entry_price == 2000 * 10**30

        btc_pos = positions[1]
        assert btc_pos.position_key == pq._compute_position_key(btc_pos.account, _BTC_MARKET, _USDC, False)
        # Verified-catalog decimals (8 for BTC): 10000e30 * 1e8 / 0.25e8.
        assert btc_pos.entry_price == 40000 * 10**30

    @pytest.mark.asyncio
    async def test_async_whole_book_default(self):
        """The async variant shares the plan/decode path via an async eth.call."""
        eth = _AsyncGmxEth(blob=_encode_book(_TWO_POSITION_BOOK))
        positions = await pq.query_gmx_positions(_WALLET, _GmxWeb3(eth), chain="arbitrum")
        assert [p.market for p in positions] == [_ETH_MARKET, _BTC_MARKET]
        assert len(eth.calls) == 1

    def test_sync_market_and_collateral_post_filters(self):
        """``markets`` / ``collateral_tokens`` are POST-filters, case-insensitive."""
        blob = _encode_book(_TWO_POSITION_BOOK)

        only_eth = pq.query_gmx_positions_sync(
            _WALLET, _GmxWeb3(_GmxEth(blob=blob)), chain="arbitrum", markets=[_ETH_MARKET.lower()]
        )
        assert [p.market for p in only_eth] == [_ETH_MARKET]

        both = pq.query_gmx_positions_sync(
            _WALLET,
            _GmxWeb3(_GmxEth(blob=blob)),
            chain="arbitrum",
            collateral_tokens=[_USDC.upper().replace("0X", "0x")],
        )
        assert len(both) == 2

        none = pq.query_gmx_positions_sync(
            _WALLET, _GmxWeb3(_GmxEth(blob=blob)), chain="arbitrum", collateral_tokens=["0x" + "99" * 20]
        )
        assert none == []

    def test_sync_empty_book_is_empty(self):
        """A successful decode of an empty array is a measured empty book."""
        eth = _GmxEth(blob=_encode_book([]))
        assert pq.query_gmx_positions_sync(_WALLET, _GmxWeb3(eth), chain="arbitrum") == []
        assert len(eth.calls) == 1

    def test_sync_read_failure_raises_unavailable(self):
        """Empty ≠ Zero: a failed eth_call is UNMEASURED and must RAISE.

        The single range read is all-or-nothing — silently returning [] would
        make an outage indistinguishable from a flat book, and the paper
        reconciler could treat every tracked position as gone (review finding
        on this PR). Callers that can proceed unmeasured (engine adoption)
        catch ``GmxPositionReadUnavailable`` explicitly.
        """
        eth = _GmxEth(exc=RuntimeError("node down"))
        with pytest.raises(pq.GmxPositionReadUnavailable, match="range read call failed"):
            pq.query_gmx_positions_sync(_WALLET, _GmxWeb3(eth), chain="arbitrum")

    @pytest.mark.asyncio
    async def test_async_read_failure_raises_unavailable(self):
        class _Boom(_GmxEth):
            async def call(self, params):  # type: ignore[override]
                raise RuntimeError("node down")

        with pytest.raises(pq.GmxPositionReadUnavailable, match="range read call failed"):
            await pq.query_gmx_positions(_WALLET, _GmxWeb3(_Boom()), chain="arbitrum")

    def test_sync_full_page_raises_unavailable_at_the_truncation_boundary(self):
        """A full [0, 100) page is UNMEASURED, not a complete book (review P1).

        The reducer flags a raw page at _MAX_POSITION_RANGE as truncated;
        treating it as the whole book lets the reconciler mark positions
        beyond the page MISSING_ON_CHAIN and rebaseline them away. One
        ACTIVE row among 100 keeps the raw-page count at the boundary while
        the filtered result is small — exactly the shape that must refuse.
        """
        from almanak.connectors.gmx_v2.perps_read import _MAX_POSITION_RANGE

        one_active = _TWO_POSITION_BOOK[0]
        zero_size = _props(_ETH_MARKET, _USDC, size_usd=0, size_tok=0, col_amt=0, is_long=False)
        book = [one_active] + [zero_size] * (_MAX_POSITION_RANGE - 1)
        eth = _GmxEth(blob=_encode_book(book))
        with pytest.raises(pq.GmxPositionReadUnavailable, match="truncated"):
            pq.query_gmx_positions_sync(_WALLET, _GmxWeb3(eth), chain="arbitrum")

    def test_sync_undecodable_blob_raises_unavailable(self):
        """A garbage return is UNMEASURED — raised, never reported as flat."""
        eth = _GmxEth(blob=b"\xde\xad\xbe\xef")
        with pytest.raises(pq.GmxPositionReadUnavailable, match="undecodable"):
            pq.query_gmx_positions_sync(_WALLET, _GmxWeb3(eth), chain="arbitrum")

    @pytest.mark.asyncio
    async def test_async_unsupported_chain_raises(self, web3):
        with pytest.raises(ValueError, match="Unsupported chain: notachain"):
            await pq.query_gmx_positions(_WALLET, web3, chain="notachain")

    def test_entry_price_uses_catalog_decimals_when_primed(self):
        """Index decimals resolve catalog-first (address-first, VIB-6561).

        For a market absent from BOTH the verified catalog and the static
        legacy table, decimals are UNMEASURED and the entry price degrades to 0
        rather than defaulting to 18 (the XMR-class misread). Once this process
        venue-verifies the market, the same read prices it with the catalog's
        decimals.
        """
        blob = _encode_book(
            [
                _props(
                    _UNCATALOGUED_MARKET,
                    _USDC,
                    size_usd=5000 * 10**30,
                    size_tok=25 * 10**11,  # 2.5 tokens at 12 decimals
                    col_amt=1000 * 10**6,
                    is_long=True,
                )
            ]
        )

        unprimed = pq.query_gmx_positions_sync(_WALLET, _GmxWeb3(_GmxEth(blob=blob)), chain="arbitrum")
        assert len(unprimed) == 1
        assert unprimed[0].entry_price == 0, "unmeasured decimals must degrade, not default to 18"

        market_catalog.remember(
            "arbitrum",
            ResolvedGmxMarket(
                label="XMR/USD",
                market_token=_UNCATALOGUED_MARKET,
                index_token="0x" + "01" * 20,
                index_symbol="XMR",
                index_token_decimals=12,
                long_token="0x" + "02" * 20,
                long_token_symbol="WETH",
                long_token_decimals=18,
                short_token=_USDC,
                short_token_symbol="USDC",
                short_token_decimals=6,
            ),
        )
        primed = pq.query_gmx_positions_sync(_WALLET, _GmxWeb3(_GmxEth(blob=blob)), chain="arbitrum")
        assert primed[0].entry_price == (5000 * 10**30 * 10**12) // (25 * 10**11)
        assert primed[0].entry_price == 2000 * 10**30


class TestAavePositionQueries:
    @pytest.mark.asyncio
    async def test_async_default_assets_no_positions(self, web3):
        """Default per-chain token table drives the asset loop."""
        with patch(f"{_MODULE}._query_aave_user_reserve_data", AsyncMock(return_value=None)) as query_mock:
            positions = await pq.query_aave_positions(_WALLET, web3, chain="arbitrum")
        assert positions == []
        assert query_mock.await_count == len(pq.AAVE_V3_TOKENS.get("arbitrum", {}))

    @pytest.mark.asyncio
    async def test_async_unsupported_chain_raises(self, web3):
        with pytest.raises(ValueError, match="Unsupported chain: notachain"):
            await pq.query_aave_positions(_WALLET, web3, chain="notachain")

    def test_sync_collects_active_positions(self, web3):
        asset = "0x" + "55" * 20
        active = pq.AaveV3LendingPosition(
            asset="USDC",
            asset_address=asset,
            current_atoken_balance=1_000_000,
            current_stable_debt=0,
            current_variable_debt=0,
            principal_stable_debt=0,
            scaled_variable_debt=0,
            stable_borrow_rate=0,
            liquidity_rate=0,
            usage_as_collateral_enabled=True,
            decimals=6,
        )
        with patch(f"{_MODULE}._query_aave_user_reserve_data_sync", return_value=active):
            positions = pq.query_aave_positions_sync(_WALLET, web3, chain="arbitrum", assets=[asset])
        assert positions == [active]


class TestEngineAdoptionSkipsUnmeasuredBook:
    @pytest.mark.asyncio
    async def test_adoption_is_skipped_when_the_book_is_unmeasured(self, caplog):
        """Review pin: an unmeasured book must SKIP adoption loudly — never seed
        the reconciler baseline from a false-flat read (Empty≠Zero)."""
        import logging
        from unittest.mock import patch

        from almanak.framework.backtesting.paper.engine import PaperTrader

        engine = PaperTrader.__new__(PaperTrader)
        engine._backtest_id = "bt-test"
        engine.config = type("Cfg", (), {"chain": "arbitrum"})()

        class _Recon:
            # A pre-existing tracked position: the skip must PRESERVE state,
            # not merely add nothing (review pin).
            positions = {"existing-position": object()}

            def track_perp_open(self, **kwargs):  # pragma: no cover - must not run
                raise AssertionError("track_perp_open must not be called for an unmeasured book")

        engine._position_reconciler = _Recon()

        with (
            patch(
                "almanak.framework.backtesting.paper.position_queries.query_gmx_positions",
                side_effect=pq.GmxPositionReadUnavailable("node down"),
            ),
            caplog.at_level(logging.WARNING),
        ):
            await engine._adopt_onchain_perp_positions(object(), _WALLET)

        assert set(engine._position_reconciler.positions) == {"existing-position"}
        assert any("book unmeasured" in r.message for r in caplog.records)
