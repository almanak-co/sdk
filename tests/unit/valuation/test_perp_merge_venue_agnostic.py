"""VIB-5252: the perp merge is venue-agnostic (proven for a non-gmx venue).

``_merge_position_sources`` keys its discovery-wins drop on ``PositionType.PERP``,
not a protocol name, so it must work for ANY perp venue. These exercise the
merge directly with ``aster_perps`` (the repo's second perp venue) to prove the
strategy's notional SYMBOL stub is dropped and the on-chain discovered position
wins — exactly as for gmx_v2 — including alias normalisation, the flat-scan
case, and a negative control. This isolates the venue-agnostic MERGE from a
venue's on-chain DECODE (a separate connector concern, e.g. VIB-5289 for gmx).

Authored from an independent adversarial verification sweep of VIB-5252.
"""

from decimal import Decimal

from almanak.framework.teardown.models import PositionInfo, PositionType
from almanak.framework.valuation.portfolio_valuer import PortfolioValuer


def _aster_stub(*, protocol: str = "aster_perps") -> PositionInfo:
    """Strategy-reported aster perp: SYMBOL market, NOTIONAL value, no wallet."""
    return PositionInfo(
        position_type=PositionType.PERP,
        position_id=f"{protocol}-BTC/USD-long",
        chain="bnb",
        protocol=protocol,
        value_usd=Decimal("10000"),  # gross notional stub
        details={"market": "BTC/USD", "is_long": True},  # symbol, no wallet hint
    )


def _aster_discovered() -> PositionInfo:
    """Discovery-emitted aster perp: on-chain market ADDRESS + wallet."""
    return PositionInfo(
        position_type=PositionType.PERP,
        position_id="aster_perps-0xMarketAddr-long",
        chain="bnb",
        protocol="aster_perps",
        value_usd=Decimal("0"),  # repriced downstream
        details={
            "market": "0x00000000000000000000000000000000000000aa",
            "collateral_token": "0x00000000000000000000000000000000000000bb",
            "is_long": True,
            "wallet_address": "0xWallet",
        },
    )


def _perps(positions: list[PositionInfo]) -> list[PositionInfo]:
    return [p for p in positions if p.position_type == PositionType.PERP]


def _merge(strategy_positions, discovered, perp_protocols_ok):
    valuer = PortfolioValuer(gateway_client=None)
    # VIB-5722: perp_protocols_ok is now (chain, protocol)-scoped. These cases are
    # all single-chain "bnb", so scope each venue to "bnb".
    scoped = None if perp_protocols_ok is None else {("bnb", p) for p in perp_protocols_ok}
    return valuer._merge_position_sources(strategy_positions, discovered, "bnb", scoped)


class TestAsterMergeVenueAgnostic:
    def test_stub_dropped_discovery_wins_via_protocols_ok(self):
        """Path (a): discovery scanned aster_perps ok → drop the notional stub,
        keep ONLY the discovered position. Exactly one perp leg survives."""
        merged = _merge([_aster_stub()], [_aster_discovered()], {"aster_perps"})

        legs = _perps(merged)
        assert len(legs) == 1, "stub + discovery must collapse to one perp leg"
        # The survivor is the discovered (address-keyed) leg, not the notional stub.
        assert legs[0].details["market"].startswith("0x")
        assert legs[0].value_usd == Decimal("0")  # repriced downstream, not notional
        assert Decimal("10000") not in [p.value_usd for p in legs]

    def test_stub_dropped_via_discovered_perp_without_protocols_ok(self):
        """Path (b): even if the caller does NOT thread perp_protocols_ok, a
        discovered aster perp for the same venue still drops the stub — the
        anti-double-count guarantee is independent of the ok-set."""
        merged = _merge([_aster_stub()], [_aster_discovered()], None)

        legs = _perps(merged)
        assert len(legs) == 1, "discovered aster perp must drop the same-venue stub"
        assert legs[0].details["market"].startswith("0x")

    def test_alias_stub_dropped_against_canonical_discovery(self):
        """Cross-alias venue-agnosticism: strategy reports the alias
        ``pancakeswap_perps`` while discovery scanned canonical ``aster_perps``.
        Normalisation must still drop the alias stub."""
        merged = _merge([_aster_stub(protocol="pancakeswap_perps")], [], {"aster_perps"})

        assert _perps(merged) == [], "alias stub must normalise to aster_perps and drop"

    def test_flat_ok_scan_drops_aster_stub(self):
        """Discovery scanned aster ok but the book was empty (flat/unfilled):
        the notional stub must vanish, not stand."""
        merged = _merge([_aster_stub()], [], {"aster_perps"})

        assert _perps(merged) == [], "ok-but-empty aster scan drops the notional stub"

    def test_negative_control_unconfirmed_stub_survives(self):
        """Adversarial negative control: discovery did NOT scan aster (read
        failed → not in perp_protocols_ok) and returned no aster perp. The stub
        must be PRESERVED (we cannot confirm flat) so a downstream degraded
        path can handle it — proving the drop is gated on confirmation, not
        blanket perp deletion."""
        merged = _merge([_aster_stub()], [], set())

        legs = _perps(merged)
        assert len(legs) == 1, "unconfirmed stub must survive the merge"
        assert legs[0].value_usd == Decimal("10000")  # the original stub, untouched
