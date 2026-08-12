"""The three-valued venue probe that teardown enumeration now depends on.

Every test here pins one leg of the property the probe exists to establish:
``get_open_positions()`` must report what the VENUE holds, and must never turn an
unmeasured read into a flat account (ALM-3109 / VIB-6159 / VIB-6497).

The GMX and Hyperliquid registries are exercised for real (offline: address and
market catalogues are static), so a metadata or plan-resolution regression fails
these tests rather than being mocked away.
"""

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.connectors._strategy_base.perps_read_base import (
    PerpsPositionOnChain,
    PerpsReadResult,
)
from almanak.framework.strategies import PerpProbeState, probe_perp_position

# Real GMX V2 arbitrum catalogue entries — the probe resolves the venue's market
# key through the connector's own metadata, so these must be genuine.
# Symbol-keyed probes resolve through the connector's venue-verified catalog
# (address-first) — prime the audited fixture snapshot, standing in for the
# dynamic verification a live compile performs before any probe runs.
from tests.unit.connectors.gmx_v2.market_fixtures import prime_catalog


@pytest.fixture(autouse=True)
def _verified_markets():
    prime_catalog()


ETH_USD_MARKET = "0x70d95587d40A2caf56bd97485aB3Eec10Bee6336"
BTC_USD_MARKET = "0x47c031236e19d024b42f8AE6780E44A573170703"
USDC_ARBITRUM = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
WETH_ARBITRUM = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"


def _gmx_position(
    *,
    market: str = ETH_USD_MARKET,
    is_long: bool = True,
    size_in_usd: int = 100 * 10**30,
    size_in_tokens: int = 10**17,  # 0.1 ETH (18 decimals)
) -> PerpsPositionOnChain:
    return PerpsPositionOnChain(
        account="0x" + "1" * 40,
        market=market,
        collateral_token=USDC_ARBITRUM,
        size_in_usd=size_in_usd,
        size_in_tokens=size_in_tokens,
        collateral_amount=50 * 10**6,
        is_long=is_long,
        borrowing_factor=0,
        funding_fee_amount_per_size=0,
        increased_at_time=0,
        decreased_at_time=0,
    )


def _hyperliquid_eth_position() -> PerpsPositionOnChain:
    return PerpsPositionOnChain(
        account="0x" + "1" * 40,
        market="ETH",
        collateral_token="USDC",
        # Deliberately differs from 1.2345 * $3,000 so the test proves the
        # notional comes from the trusted venue mark, not this stored value.
        size_in_usd=2_000_000_000,
        size_in_tokens=12_345,  # 1.2345 ETH at Hyperliquid's 4 szDecimals
        collateral_amount=740_700_000,
        is_long=True,
        borrowing_factor=0,
        funding_fee_amount_per_size=0,
        increased_at_time=0,
        decreased_at_time=0,
        key_prefix="hyperliquid",
    )


def _snapshot(result: PerpsReadResult | Exception, price: str = "3000") -> MagicMock:
    market = MagicMock()
    if isinstance(result, Exception):
        market.perp_positions.side_effect = result
    else:
        market.perp_positions.return_value = result
    market.price.return_value = Decimal(price)
    return market


def _probe(result, **kwargs):
    return probe_perp_position(
        _snapshot(result),
        protocol=kwargs.pop("protocol", "gmx_v2"),
        chain=kwargs.pop("chain", "arbitrum"),
        market_symbol=kwargs.pop("market_symbol", "ETH/USD"),
        index_token_address=kwargs.pop("index_token_address", WETH_ARBITRUM),
        **kwargs,
    )


class TestVenueTruthBeatsCache:
    """The three transitions the migration exists to make."""

    def test_venue_position_is_reported_even_though_no_cache_input_exists(self):
        """A position the strategy cache missed is still found (ALM-3109 / VIB-6159).

        The probe takes NO cached state as input — the side comes from the venue,
        which is precisely what a cache that never recorded the fill cannot supply.
        """
        probe = _probe(PerpsReadResult(positions=(_gmx_position(is_long=False),), ok=True))

        assert probe.state is PerpProbeState.OPEN
        assert probe.is_open
        assert len(probe.positions) == 1
        assert probe.positions[0].is_long is False

    def test_measured_empty_book_is_flat(self):
        """``ok=True`` with no positions is a MEASURED flat — the cache is stale."""
        probe = _probe(PerpsReadResult(positions=(), ok=True))

        assert probe.state is PerpProbeState.FLAT
        assert probe.is_flat
        assert probe.positions == ()

    def test_failed_read_is_unmeasured_not_flat(self):
        """``ok=False`` must NEVER read as flat (VIB-6497's false certification)."""
        probe = _probe(PerpsReadResult(positions=(), ok=False))

        assert probe.state is PerpProbeState.UNMEASURED
        assert not probe.is_flat
        assert not probe.is_measured
        assert probe.reason == "read_unavailable"

    def test_absent_snapshot_is_unmeasured_not_flat(self):
        probe = probe_perp_position(None, protocol="gmx_v2", chain="arbitrum", market_symbol="ETH/USD")

        assert probe.state is PerpProbeState.UNMEASURED
        assert probe.reason == "no_market_snapshot"

    def test_raising_read_is_unmeasured_not_flat(self):
        """A probe never propagates: it would fail enumeration for every position."""
        probe = _probe(RuntimeError("gateway down"))

        assert probe.state is PerpProbeState.UNMEASURED
        assert probe.reason == "read_raised:RuntimeError"


class TestNegativeClaimsAreHardToMake:
    """Every way an empty match set fails to prove the account is flat."""

    def test_truncated_page_cannot_prove_absence(self):
        probe = _probe(PerpsReadResult(positions=(), ok=True, truncated=True))

        assert probe.state is PerpProbeState.UNMEASURED
        assert probe.reason == "read_truncated"

    def test_truncated_page_still_proves_presence(self):
        """Presence is positive evidence — a partial read cannot weaken it."""
        probe = _probe(PerpsReadResult(positions=(_gmx_position(),), ok=True, truncated=True))

        assert probe.state is PerpProbeState.OPEN

    def test_unidentifiable_live_position_cannot_prove_absence(self):
        """A live position whose market we cannot name might be ours."""
        probe = _probe(PerpsReadResult(positions=(_gmx_position(market="0x" + "e" * 40),), ok=True))

        assert probe.state is PerpProbeState.UNMEASURED
        assert probe.reason == "unidentified_venue_position"

    def test_other_market_is_identified_and_excluded(self):
        """A NAMED position on another market does not block the flat claim."""
        probe = _probe(PerpsReadResult(positions=(_gmx_position(market=BTC_USD_MARKET),), ok=True))

        assert probe.state is PerpProbeState.FLAT

    def test_market_outside_a_per_market_read_universe_is_unmeasured(self):
        """Hyperliquid plans one call per SEEDED symbol.

        A market outside that set is never read, so an empty book says nothing
        about it. Reporting flat here would strand the position (VIB-6392 leaves
        perps no registry backstop to recover from).
        """
        probe = _probe(
            PerpsReadResult(positions=(), ok=True),
            protocol="hyperliquid",
            chain="hyperevm",
            market_symbol="NOTASEEDEDMARKET",
        )

        assert probe.state is PerpProbeState.UNMEASURED
        assert probe.reason == "market_outside_read_universe"

    def test_seeded_per_market_venue_can_still_be_measured_flat(self):
        """Liveness control: the guard above must not refuse EVERY flat claim."""
        probe = _probe(
            PerpsReadResult(positions=(), ok=True),
            protocol="hyperliquid",
            chain="hyperevm",
            market_symbol="ETH",
        )

        assert probe.state is PerpProbeState.FLAT

    def test_zero_size_position_is_not_open(self):
        probe = _probe(PerpsReadResult(positions=(_gmx_position(size_in_usd=0, size_in_tokens=0),), ok=True))

        assert probe.state is PerpProbeState.FLAT


class TestNotional:
    """A truthful row valued at $0 is dropped as dust by the teardown harness."""

    def test_notional_is_mark_valued_from_venue_size(self):
        market = _snapshot(PerpsReadResult(positions=(_gmx_position(),), ok=True))
        probe = probe_perp_position(
            market,
            protocol="gmx_v2",
            chain="arbitrum",
            market_symbol="ETH/USD",
            index_token_address=WETH_ARBITRUM,
        )

        # 0.1 ETH (1e17 raw / 1e18) at $3000.
        assert probe.positions[0].notional_usd == Decimal("300.0")
        market.price.assert_called_once_with(WETH_ARBITRUM, chain="arbitrum")

    def test_gmx_without_index_address_stays_unmeasured(self):
        market = _snapshot(PerpsReadResult(positions=(_gmx_position(),), ok=True))

        probe = probe_perp_position(
            market,
            protocol="gmx_v2",
            chain="arbitrum",
            market_symbol="ETH/USD",
        )

        assert probe.positions[0].notional_usd is None
        market.funding_rate.assert_not_called()
        market.perp_mark_price.assert_not_called()
        market.price.assert_not_called()

    def test_hyperliquid_addressless_position_uses_venue_mark(self):
        market = _snapshot(PerpsReadResult(positions=(_hyperliquid_eth_position(),), ok=True))
        market.perp_mark_price.return_value = Decimal("3000")

        probe = probe_perp_position(
            market,
            protocol="hyperliquid",
            chain="hyperevm",
            market_symbol="ETH",
            index_token_address=None,
        )

        assert probe.positions[0].notional_usd == Decimal("3703.5000")
        market.perp_mark_price.assert_called_once_with("hyperliquid", "ETH", chain="hyperevm")
        market.price.assert_not_called()

    def test_unpriceable_notional_is_none_not_zero(self):
        market = _snapshot(PerpsReadResult(positions=(_gmx_position(),), ok=True))
        market.price.side_effect = ValueError("no oracle path")

        probe = probe_perp_position(
            market,
            protocol="gmx_v2",
            chain="arbitrum",
            market_symbol="ETH/USD",
            index_token_address=WETH_ARBITRUM,
        )

        assert probe.state is PerpProbeState.OPEN
        assert probe.positions[0].notional_usd is None

    def test_non_positive_price_is_unmeasured_not_a_zero_notional(self):
        market = _snapshot(PerpsReadResult(positions=(_gmx_position(),), ok=True), price="0")

        probe = probe_perp_position(
            market,
            protocol="gmx_v2",
            chain="arbitrum",
            market_symbol="ETH/USD",
            index_token_address=WETH_ARBITRUM,
        )

        assert probe.positions[0].notional_usd is None


class TestSideFilter:
    @pytest.mark.parametrize("wanted,expected", [(True, PerpProbeState.OPEN), (False, PerpProbeState.FLAT)])
    def test_is_long_filter_selects_the_side(self, wanted, expected):
        probe = _probe(PerpsReadResult(positions=(_gmx_position(is_long=True),), ok=True), is_long=wanted)

        assert probe.state is expected


class TestHyperliquidPartialReadCannotAssertFlat:
    """End-to-end: the REAL hyperliquid reducer feeding the REAL probe.

    Every guard in ``TestNegativeClaimsAreHardToMake`` is exercised against a
    hand-built ``PerpsReadResult``. That proves the probe honours ``truncated`` —
    it proves nothing about whether any producer ever SETS it. Hyperliquid plans
    one call per seeded market and, before this was fixed, dropped a failed
    market silently while returning ``ok=True, truncated=False``: a book with a
    hole in it, indistinguishable from a measured-flat account.

    So this test drives the connector's own reducer rather than a stand-in. If
    the reducer stops reporting incompleteness, the probe goes back to
    certifying FLAT for a market it never actually read, and teardown closes
    nothing over a live HyperCore position (VIB-6497).
    """

    @staticmethod
    def _reduce(*position_blobs):
        from almanak.connectors._strategy_base.perps_read_base import PerpsPositionQuery
        from almanak.connectors.hyperliquid import perps_read as pr
        from tests.unit.connectors.hyperliquid.test_perps_read import _margin_blob

        query = PerpsPositionQuery(
            chain="hyperevm",
            wallet_address="0x" + "1" * 40,
            targets={},
            markets=("BTC", "ETH"),
        )
        return pr._reduce_hyperliquid_positions(query, [*position_blobs, _margin_blob()])

    def test_a_failed_eth_call_is_unmeasured_not_flat(self) -> None:
        # BTC succeeds and holds nothing; the ETH call failed outright. ETH was
        # never read, so its absence is not evidence of closure.
        from tests.unit.connectors.hyperliquid.test_perps_read import _position_blob

        result = self._reduce(_position_blob(0, 0), None)
        probe = _probe(result, protocol="hyperliquid", chain="hyperevm", market_symbol="ETH")
        assert probe.state is PerpProbeState.UNMEASURED
        assert probe.is_flat is False

    def test_a_live_btc_position_does_not_launder_a_failed_eth_read(self) -> None:
        # The dangerous shape: another market returns a REAL position, so the read
        # looks healthy and `saw_unidentified` never trips (BTC is identified, and
        # identified-as-not-ours). Only truncation stands between this and a FLAT.
        from tests.unit.connectors.hyperliquid.test_perps_read import _position_blob

        result = self._reduce(_position_blob(1000, 600_000_000), None)
        probe = _probe(result, protocol="hyperliquid", chain="hyperevm", market_symbol="ETH")
        assert probe.state is PerpProbeState.UNMEASURED

    def test_a_complete_read_still_measures_flat(self) -> None:
        """Liveness control: a fail-closed guard that can never PROCEED is as
        broken as one that never fires. A fully measured empty book must still
        reach FLAT, or teardown would publish a phantom residual forever."""
        from tests.unit.connectors.hyperliquid.test_perps_read import _position_blob

        result = self._reduce(_position_blob(0, 0), _position_blob(0, 0))
        probe = _probe(result, protocol="hyperliquid", chain="hyperevm", market_symbol="ETH")
        assert probe.state is PerpProbeState.FLAT
