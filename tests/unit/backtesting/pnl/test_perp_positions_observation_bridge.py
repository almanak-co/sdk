"""Simulated perp book served through ``MarketSnapshot.perp_positions``.

Backtest observation parity for perps (the last serve-from-sim-state gap after
ALM-2943/ALM-2951 closed health, LP value, gas, OHLCV, pool price, slippage,
and lending rates): the engine books PERP_OPEN fills into its own
``SimulatedPortfolio`` but the strategy-facing ``perp_positions`` read went
straight to the (absent) gateway and returned ``ok=False`` forever, so every
observation-gated perp strategy — the recommended pattern — held forever in
backtests while the same code traded live.

Pins:

- The sim's own book is served as a MEASURED read (``ok=True``), projected into
  the venue's raw on-chain shape (real market / collateral addresses, venue
  fixed-point scaling) via the connector-owned ``simulate_position`` hook.
- Empty≠Zero at every seam: a flat book is ``ok=True`` + empty (measured flat);
  an unregistered venue or an unprojectable position is ``ok=False`` plus a
  decision-input ledger entry — and never a partially served book.
- ``_perp_open_delta`` stamps the authoring-surface identity (market key,
  collateral token / address / decimals / amount) the projection needs.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from almanak.connectors.gmx_v2 import market_catalog
from almanak.framework.backtesting.pnl.data_provider import MarketState
from almanak.framework.backtesting.pnl.engine import PnLBacktester, SimulatedPositionView
from almanak.framework.backtesting.pnl.portfolio import SimulatedPortfolio
from almanak.framework.backtesting.pnl.position_models import SimulatedPosition
from almanak.framework.market import MarketSnapshot

D = Decimal
TS = datetime(2026, 6, 20, 12, tzinfo=UTC)
WALLET = "0x" + "ab" * 20
GMX_ETH_USD_ARBITRUM = "0x70d95587d40A2caf56bd97485aB3Eec10Bee6336"
USDC_ARBITRUM = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"


@pytest.fixture(autouse=True)
def _verified_eth_market():
    """Prime the process catalog with the venue-verified ETH/USD row.

    Address-first: the projection resolves identity ONLY through the
    remembered catalog (a live run populates it via dynamic verification;
    tests prime it from the audit-pinned fixture rows), and the module-global
    catalog must not leak rows across tests.
    """
    from tests.unit.connectors.gmx_v2.market_fixtures import market_record

    market_catalog.clear()
    market_catalog.remember("arbitrum", market_record("arbitrum", "ETH/USD"))
    yield
    market_catalog.clear()


def _snapshot() -> MarketSnapshot:
    from almanak.framework.market.builders import MarketSnapshotBuilder

    return MarketSnapshotBuilder.seeded(chain="arbitrum", wallet_address=WALLET)


def _market_state() -> MarketState:
    return MarketState(timestamp=TS, prices={"ETH": D("2000"), "USDC": D("1")}, chain="arbitrum")


def _perp(protocol: str = "gmx_v2", *, stamped: bool = True) -> SimulatedPosition:
    position = SimulatedPosition.perp_long(
        token="ETH",
        collateral_usd=D("5"),
        leverage=D("2"),
        entry_price=D("2000"),
        entry_time=TS,
        protocol=protocol,
    )
    if stamped:
        position.metadata.update(
            {
                "perp_market": GMX_ETH_USD_ARBITRUM,
                "perp_collateral_token": "USDC",
                "perp_collateral_address": USDC_ARBITRUM,
                "perp_collateral_decimals": 6,
                "perp_collateral_amount": "5",
            }
        )
    return position


def _portfolio(*positions: SimulatedPosition) -> SimulatedPortfolio:
    portfolio = SimulatedPortfolio(initial_capital_usd=D("10000"), chain="arbitrum")
    portfolio.positions.extend(positions)
    return portfolio


def _view_snapshot(portfolio: SimulatedPortfolio) -> MarketSnapshot:
    snapshot = _snapshot()
    view = SimulatedPositionView(portfolio)
    view.bind(_market_state(), TS)
    snapshot._simulated_position_view = view
    return snapshot


class TestSimulatedBookServed:
    def test_open_position_is_a_measured_venue_read(self):
        snapshot = _view_snapshot(_portfolio(_perp()))

        result = snapshot.perp_positions("gmx_v2", chain="arbitrum")

        assert result.ok is True
        assert result.truncated is False
        assert len(result.positions) == 1
        row = result.positions[0]
        # The strategy-side matchers compare ADDRESSES — a symbol here would
        # silently never match (the demo's _target_position_is_open shape).
        assert row.market.lower() == GMX_ETH_USD_ARBITRUM.lower()
        assert row.collateral_token.lower() == USDC_ARBITRUM.lower()
        assert row.account == WALLET
        assert row.is_long is True
        assert row.is_active is True
        # GMX fixed-point scaling: USD at 30 decimals, collateral at token
        # decimals, sizes at index-token decimals.
        assert row.size_in_usd == 10 * 10**30
        assert row.collateral_amount == 5 * 10**6
        assert row.size_in_tokens == int(D("0.005") * 10**18)

    def test_flat_book_is_measured_flat(self):
        snapshot = _view_snapshot(_portfolio())

        result = snapshot.perp_positions("gmx_v2", chain="arbitrum")

        assert result.ok is True
        assert result.positions == ()

    def test_liquidated_position_reads_as_flat(self):
        # A liquidated position no longer exists on-venue; serving it back
        # would let a strategy "close" exposure the venue already seized.
        position = _perp()
        position.is_liquidated = True
        snapshot = _view_snapshot(_portfolio(position))

        result = snapshot.perp_positions("gmx_v2", chain="arbitrum")

        assert result.ok is True
        assert result.positions == ()

    def test_other_venue_positions_are_not_served(self):
        snapshot = _view_snapshot(_portfolio(_perp(protocol="hyperliquid")))

        result = snapshot.perp_positions("gmx_v2", chain="arbitrum")

        assert result.ok is True
        assert result.positions == ()


class TestHonestRefusals:
    def test_unregistered_venue_is_unmeasured_with_ledger_entry(self):
        snapshot = _view_snapshot(_portfolio(_perp()))

        result = snapshot.perp_positions("uniswap_v3", chain="arbitrum")

        assert result.ok is False
        assert result.positions == ()
        assert ("perp_positions", "simulation") in snapshot._critical_data_failures

    def test_unstamped_position_is_unmeasured_never_partial(self):
        # One projectable and one identity-less position: serving only the
        # projectable one would report the other as closed exposure. The whole
        # read refuses instead.
        snapshot = _view_snapshot(_portfolio(_perp(), _perp(stamped=False)))

        result = snapshot.perp_positions("gmx_v2", chain="arbitrum")

        assert result.ok is False
        assert result.positions == ()
        assert ("perp_positions", "simulation") in snapshot._critical_data_failures

    def test_unknown_market_is_unmeasured_with_ledger_entry(self):
        position = _perp()
        position.metadata["perp_market"] = "NOTLISTED/USD"
        snapshot = _view_snapshot(_portfolio(position))

        result = snapshot.perp_positions("gmx_v2", chain="arbitrum")

        assert result.ok is False
        assert ("perp_positions", "simulation") in snapshot._critical_data_failures


class TestOpenStampsObservationIdentity:
    class _StampHost:
        """Minimal PnLBacktester stand-in: only the registered-token map."""

        @staticmethod
        def _registered_token_addresses() -> dict[str, tuple[str, str]]:
            return {"USDC": ("arbitrum", USDC_ARBITRUM)}

    def test_perp_open_intent_identity_is_stamped(self):
        from almanak.framework.intents import Intent

        intent = Intent.perp_open(
            market=GMX_ETH_USD_ARBITRUM,
            collateral_token="USDC",
            collateral_amount=D("5"),
            size_usd=D("10"),
            is_long=True,
            leverage=D("2"),
            protocol="gmx_v2",
        )
        position = _perp(stamped=False)

        PnLBacktester._stamp_perp_observation_identity(self._StampHost(), position, intent, _market_state())

        metadata = position.metadata
        assert metadata["perp_market"] == GMX_ETH_USD_ARBITRUM
        assert metadata["perp_collateral_token"] == "USDC"
        assert metadata["perp_collateral_address"].lower() == USDC_ARBITRUM.lower()
        assert metadata["perp_collateral_decimals"] == 6
        assert D(metadata["perp_collateral_amount"]) == D("5")

    def test_identityless_intent_stamps_nothing_and_does_not_raise(self):
        # Duck-typed intents without market/collateral fields still open fine;
        # only the observation surface later refuses (with a ledger entry).
        position = _perp(stamped=False)

        PnLBacktester._stamp_perp_observation_identity(self._StampHost(), position, object(), _market_state())

        assert "perp_market" not in position.metadata
        assert "perp_collateral_token" not in position.metadata

    def test_stamped_open_round_trips_through_the_snapshot_read(self):
        from almanak.framework.intents import Intent

        intent = Intent.perp_open(
            market=GMX_ETH_USD_ARBITRUM,
            collateral_token="USDC",
            collateral_amount=D("5"),
            size_usd=D("10"),
            is_long=True,
            leverage=D("2"),
            protocol="gmx_v2",
        )
        position = _perp(stamped=False)
        PnLBacktester._stamp_perp_observation_identity(self._StampHost(), position, intent, _market_state())
        snapshot = _view_snapshot(_portfolio(position))

        result = snapshot.perp_positions("gmx_v2", chain="arbitrum")

        assert result.ok is True
        assert len(result.positions) == 1
        assert result.positions[0].market.lower() == GMX_ETH_USD_ARBITRUM.lower()
