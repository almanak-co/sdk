"""VIB-5252 — perp NAV is net equity, never gross notional (end-to-end).

These drive the full ``PortfolioValuer.value()`` path (discovery → merge →
reprice → snapshot), the contract the prior fix (#2937) failed to exercise. The
defect class: a strategy reports a perp as a SYMBOL stub whose ``value_usd`` is
gross NOTIONAL (collateral × leverage) and which carries no on-chain market
address/wallet, so it can never reprice to §7.4 net equity
(``collateral + uPnL − fees``). Only the framework-discovered position carries
the address + wallet the repricer needs. The merge must let discovery win.

Four scenarios pin the class so it cannot silently return:

1. ``filled``      — discovery returns a live perp → ONE leg at net equity, not notional.
2. ``flat``        — discovery scanned ok but the book is empty → the notional stub vanishes.
3. ``errored``     — discovery could not scan → stub survives but degrades to UNAVAILABLE
                     (never books notional at HIGH; the runner fallback then excludes it).
4. ``multi``       — two live perps both reprice; both stubs are dropped.

The merge fix is venue-agnostic (keys on ``PositionType.PERP``), so it covers
gmx_v2 + aster_perps (and any future ``perps_read`` venue) in one place.
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

from almanak.connectors._strategy_base.perps_read_base import PerpsReadResult
from almanak.framework.portfolio.models import ValueConfidence
from almanak.framework.teardown.models import PositionInfo, PositionType
from almanak.framework.valuation.perps_position_reader import PerpsPositionOnChain
from almanak.framework.valuation.portfolio_valuer import PortfolioValuer
from almanak.framework.valuation.position_discovery import DiscoveryResult

# Real GMX arbitrum ETH/USD market: symbol "ETH", 18-decimal index token — the
# real PerpsReadRegistry resolves its metadata + valuation.
_ETH_MARKET = "0x70d95587d40A2caf56bd97485aB3Eec10Bee6336"
_USDC = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"

# At ETH=$2200, _on_chain_eth_long values to: collateral $2000 + uPnL
# (5 ETH × ($2200 − $2000 entry)) $1000 = $3000 net equity. Gross notional
# (size_in_usd) is $10000 — a 3.3× overstatement if booked as the stub reports.
_NET_EQUITY_LONG = Decimal("3000")
_NOTIONAL = Decimal("10000")


def _on_chain_long() -> PerpsPositionOnChain:
    return PerpsPositionOnChain(
        account="0xWallet",
        market=_ETH_MARKET,
        collateral_token=_USDC,
        size_in_usd=10_000 * 10**30,
        size_in_tokens=5 * 10**18,
        collateral_amount=2000 * 10**6,
        is_long=True,
        borrowing_factor=0,
        funding_fee_amount_per_size=0,
        increased_at_time=0,
        decreased_at_time=0,
    )


def _on_chain_short() -> PerpsPositionOnChain:
    # Same market, opposite side. At ETH=$2200 uPnL = 5 × ($2000 − $2200) =
    # −$1000 → net equity $2000 − $1000 = $1000.
    return PerpsPositionOnChain(
        account="0xWallet",
        market=_ETH_MARKET,
        collateral_token=_USDC,
        size_in_usd=10_000 * 10**30,
        size_in_tokens=5 * 10**18,
        collateral_amount=2000 * 10**6,
        is_long=False,
        borrowing_factor=0,
        funding_fee_amount_per_size=0,
        increased_at_time=0,
        decreased_at_time=0,
    )


def _strategy_perp_stub(*, is_long: bool = True) -> PositionInfo:
    """How production strategies report a perp: SYMBOL market, NOTIONAL value,
    no wallet hint — cannot reprice."""
    return PositionInfo(
        position_type=PositionType.PERP,
        position_id=f"gmx-ETH/USD-{'long' if is_long else 'short'}",
        chain="arbitrum",
        protocol="gmx_v2",
        value_usd=_NOTIONAL,  # gross notional — the bug under test
        details={"market": "ETH/USD", "is_long": is_long},  # symbol, no wallet
    )


def _discovered_perp(*, is_long: bool = True) -> PositionInfo:
    """How framework discovery emits a perp: on-chain market ADDRESS + wallet,
    value 0 (repriced downstream)."""
    return PositionInfo(
        position_type=PositionType.PERP,
        position_id=f"gmx-{_ETH_MARKET}-{'long' if is_long else 'short'}",
        chain="arbitrum",
        protocol="gmx_v2",
        value_usd=Decimal("0"),
        details={
            "market": _ETH_MARKET,
            "collateral_token": _USDC,
            "is_long": is_long,
            "wallet_address": "0xWallet",
        },
    )


def _market_at(eth_price: Decimal) -> MagicMock:
    market = MagicMock()
    market.price.side_effect = lambda token, *a, **k: {"ETH": eth_price, "USDC": Decimal("1")}.get(token, Decimal("0"))
    market.balance.return_value = MagicMock(balance=Decimal("0"))
    return market


def _run_value(
    *,
    strategy_positions: list[PositionInfo],
    discovered: list[PositionInfo],
    perp_protocols_ok: set[str],
    errors: list[str] | None = None,
    on_chain: tuple[PerpsPositionOnChain, ...] = (),
    on_chain_ok: bool = True,
    eth_price: Decimal = Decimal("2200"),
):
    """Drive the full value() path with controllable discovery + on-chain read."""
    valuer = PortfolioValuer(gateway_client=None)

    discovery_result = DiscoveryResult(
        positions=list(discovered),
        perp_protocols_ok=set(perp_protocols_ok),
        errors=list(errors or []),
    )
    mock_discovery = MagicMock()
    mock_discovery.discover.return_value = discovery_result
    valuer._discovery = mock_discovery

    # The repricer reads the wallet's on-chain book through the valuer's perps reader.
    valuer._perps_reader = MagicMock()
    valuer._perps_reader.read_positions.return_value = PerpsReadResult(positions=on_chain, ok=on_chain_ok)

    strategy = MagicMock()
    strategy.deployment_id = "test-strat"
    strategy.chain = "arbitrum"
    strategy.wallet_address = "0xWallet"
    strategy._get_tracked_tokens.return_value = []

    market = _market_at(eth_price)

    with (
        patch.object(valuer, "_get_strategy_positions", return_value=(strategy_positions, False)),
        patch.object(valuer, "_build_discovery_config", return_value=MagicMock()),
        patch.object(PortfolioValuer, "_resolve_token_symbol", lambda self, addr, p, k: "USDC"),
        patch.object(PortfolioValuer, "_get_token_decimals", lambda self, sym, chain: 6),
    ):
        return valuer.value(strategy, market)


def _perp_legs(snapshot) -> list:
    return [p for p in snapshot.positions if p.position_type == PositionType.PERP]


class TestPerpNetEquityVib5252:
    def test_filled_books_net_equity_not_notional(self):
        """A live perp values at net equity ($3000), not the stub's notional ($10000),
        and exactly one perp leg survives (the discovered one — stub dropped)."""
        snapshot = _run_value(
            strategy_positions=[_strategy_perp_stub()],
            discovered=[_discovered_perp()],
            perp_protocols_ok={"gmx_v2"},
            on_chain=(_on_chain_long(),),
        )

        legs = _perp_legs(snapshot)
        assert len(legs) == 1, "stub + discovery must collapse to one perp leg (no double-count)"
        assert legs[0].value_usd == _NET_EQUITY_LONG
        assert legs[0].value_usd != _NOTIONAL
        # No notional inflation anywhere in the total.
        assert snapshot.total_value_usd == _NET_EQUITY_LONG
        assert snapshot.value_confidence == ValueConfidence.HIGH

    def test_flat_book_drops_notional_stub(self):
        """Discovery scanned ok but the book is empty (perp is flat/unfilled): the
        strategy's notional stub vanishes — it never inflates NAV."""
        snapshot = _run_value(
            strategy_positions=[_strategy_perp_stub()],
            discovered=[],  # ok-but-empty book
            perp_protocols_ok={"gmx_v2"},
            on_chain=(),
        )

        assert _perp_legs(snapshot) == []
        assert snapshot.total_value_usd == Decimal("0")  # no notional
        assert snapshot.value_confidence == ValueConfidence.HIGH

    def test_discovery_errored_degrades_not_notional(self):
        """Discovery could NOT scan the venue (read failed): the stub survives the
        merge but must degrade to UNAVAILABLE, never book notional at HIGH. This
        is the path that routes to the runner fallback (which excludes the perp)."""
        snapshot = _run_value(
            strategy_positions=[_strategy_perp_stub()],
            discovered=[],
            perp_protocols_ok=set(),  # nothing scanned ok
            errors=["Perps read failed for gmx_v2 on arbitrum"],
            on_chain=(),
            on_chain_ok=False,  # repricer cannot match either
        )

        legs = _perp_legs(snapshot)
        # The unrepriceable strategy stub SURVIVES the merge (nothing scanned ok,
        # so we cannot confirm it is flat and must not silently drop it), but it
        # contributes 0 (no_path) — never its notional. Assert exactly one leg of
        # the stub's shape so an empty-leg list can't vacuously pass.
        assert len(legs) == 1
        assert legs[0].details.get("market") == "ETH/USD"
        assert legs[0].value_usd == Decimal("0")
        assert snapshot.total_value_usd == Decimal("0")
        assert snapshot.value_confidence == ValueConfidence.UNAVAILABLE

    def test_multi_position_both_reprice_stubs_dropped(self):
        """Two live perps (long + short) both reprice to net equity; both notional
        stubs are dropped. Exactly two perp legs, summing net equity."""
        snapshot = _run_value(
            strategy_positions=[_strategy_perp_stub(is_long=True), _strategy_perp_stub(is_long=False)],
            discovered=[_discovered_perp(is_long=True), _discovered_perp(is_long=False)],
            perp_protocols_ok={"gmx_v2"},
            on_chain=(_on_chain_long(), _on_chain_short()),
        )

        legs = _perp_legs(snapshot)
        assert len(legs) == 2, "two discovered perps survive; two stubs dropped"
        values = sorted(leg.value_usd for leg in legs)
        # short net $1000 (uPnL −$1000), long net $3000 (uPnL +$1000).
        assert values == [Decimal("1000"), Decimal("3000")]
        assert _NOTIONAL not in values
        assert snapshot.value_confidence == ValueConfidence.HIGH


class TestFallbackSnapshotExcludesPerpNotional:
    """Site D: the framework default ``get_portfolio_snapshot`` (the runner's
    substitution when the valuer is UNAVAILABLE) must not re-book perp notional —
    the trap that made #2937 inert."""

    def _make_strategy(self, positions, total):
        from almanak.framework.strategies.intent_strategy import IntentStrategy

        strat = MagicMock(spec=IntentStrategy)
        strat.deployment_id = "test-strat"
        strat._chain = "arbitrum"
        strat.chain = "arbitrum"
        strat.deployment_id = "test-strat"
        summary = MagicMock()
        summary.positions = positions
        summary.total_value_usd = total
        strat.get_open_positions.return_value = summary
        strat._get_tracked_tokens.return_value = []
        strat._append_native_gas_to_wallet.return_value = ("unknown_chain", Decimal("0"))
        return strat

    def test_perp_leg_excluded_from_fallback_total(self):
        from almanak.framework.strategies.intent_strategy import IntentStrategy

        perp = MagicMock()
        perp.position_type = PositionType.PERP
        perp.protocol = "gmx_v2"
        perp.chain = "arbitrum"
        perp.value_usd = _NOTIONAL  # notional — must be excluded
        perp.details = {}
        supply = MagicMock()
        supply.position_type = PositionType.SUPPLY
        supply.protocol = "aave_v3"
        supply.chain = "arbitrum"
        supply.value_usd = Decimal("500")
        supply.details = {}

        strat = self._make_strategy([perp, supply], total=_NOTIONAL + Decimal("500"))
        market = _market_at(Decimal("2200"))

        snapshot = IntentStrategy.get_portfolio_snapshot(strat, market)

        # Only the non-perp leg's value is booked; the perp notional is excluded
        # and confidence degrades.
        assert snapshot.total_value_usd == Decimal("500")
        assert snapshot.value_confidence == ValueConfidence.ESTIMATED
        assert all(p.position_type != PositionType.PERP for p in snapshot.positions)
