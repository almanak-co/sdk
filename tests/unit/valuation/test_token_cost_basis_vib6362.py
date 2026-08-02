"""VIB-6362 — a held ``PositionType.TOKEN`` leg must carry a real cost basis.

``_enrich_position_pnl`` dispatches on position type (SUPPLY/BORROW, LP, PERP,
VAULT) and had NO ``TOKEN`` branch, so a discovered token holding was never
enriched. ``PositionValue.cost_basis_usd`` defaults to ``Decimal("0")`` and
``_position_to_dict`` drops it as falsy, so the leg reached every reader with no
basis, permanently.

Strategy PnL is ``open_position_nav - deployed_capital_usd``: the NAV side
counts the leg's full mark, the cost side skips it, so the whole holding books
as profit. On BSC mainnet the ``pancakeswap_aave_carry_bsc`` carry rendered
**+$0.87 (+42.07%)** on an economically flat position for its entire life
(``tests/reports/vib6308-pnl-basis-coverage-mainnet-proof-20260801.md``).

VIB-6308 shipped the display guard (``nav_basis_coverage`` → the tile suppresses
rather than renders a number it cannot trust). This suite pins the fix that
produces the truth: the leg is backed from the SAME FIFO swap-acquisition lots
the VIB-5057 classifier reads.

The numbers below are the frozen mainnet run
``docs/internal/quant-user-runs/20260801-0045-noneth6-pcs-aave-carry-bsc``
(deployment ``deployment:81b812bf5b74``, iteration 3): SUPPLY 0.005 WBNB,
BORROW 0.88 USDC, SWAP → 0.88064637978994422 USDT held.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.dashboard.quant_aggregations import compute_inventory_unrealized
from almanak.framework.portfolio.models import PositionValue
from almanak.framework.teardown.models import (
    PositionInfo,
    PositionType,
    TeardownPositionSummary,
)
from almanak.framework.valuation.net_debt import net_debt_from_snapshot
from almanak.framework.valuation.portfolio_valuer import (
    PortfolioValuer,
    _build_wallet_match_index,
)
from almanak.gateway.proto import gateway_pb2

_DEPLOYMENT = "deployment:81b812bf5b74"
_WALLET = "0x6461179daf3B088f2f7811c55ED0cBA6e51661E8"
_CHAIN = "bsc"

_COLLATERAL_USD = Decimal("2.92708581880608260091687")  # SUPPLY WBNB, on-chain mark
_COLLATERAL_COST_USD = Decimal("2.944407655")
_DEBT_USD = Decimal("0.88002047955141935600")  # BORROW USDC (negative leg)
_DEBT_COST_USD = Decimal("0.88")
_HELD_USDT = Decimal("0.88064637978994422")  # SWAP token_out, still held
_SWAP_COST_USD = Decimal("0.8806463797899442200")  # SWAP amount_out_usd
_USDT_PRICE = Decimal("1.00")


def _event(event_type: str, payload: dict[str, object], *, position_key: str = "") -> dict[str, object]:
    return {
        "deployment_id": _DEPLOYMENT,
        "chain": _CHAIN,
        "wallet_address": _WALLET,
        "event_type": event_type,
        "position_key": position_key,
        "timestamp": "2026-08-01T04:14:37+00:00",
        "payload_json": json.dumps(payload),
    }


def _carry_events(*, swap_cost_usd: str | None = str(_SWAP_COST_USD)) -> list[dict[str, object]]:
    """SUPPLY → BORROW → SWAP, the shape the frozen mainnet DB holds.

    ``swap_cost_usd=None`` drops ``amount_out_usd`` so the lot's basis replays
    as unmeasured — the Empty≠Zero path.
    """
    swap_payload: dict[str, object] = {
        "event_type": "SWAP",
        "token_in": "USDC",
        "amount_in": "0.88",
        "amount_in_usd": "0.8800",
        "token_out": "USDT",
        "amount_out": str(_HELD_USDT),
        "swap_position_key": f"swap:{_CHAIN}:{_WALLET.lower()}",
    }
    if swap_cost_usd is not None:
        swap_payload["amount_out_usd"] = swap_cost_usd
    return [
        _event(
            "SUPPLY",
            {
                "event_type": "SUPPLY",
                "asset": "WBNB",
                "amount_token": "0.005",
                "principal_delta_usd": str(_COLLATERAL_COST_USD),
            },
            position_key=f"lending:{_CHAIN}:aave_v3:{_WALLET.lower()}:wbnb",
        ),
        _event(
            "BORROW",
            {
                "event_type": "BORROW",
                "asset": "USDC",
                "amount_token": "0.88",
                "principal_delta_usd": str(_DEBT_COST_USD),
            },
            position_key=f"lending:{_CHAIN}:aave_v3:{_WALLET.lower()}:usdc",
        ),
        _event("SWAP", swap_payload),
    ]


def _accounting_store(events: list[dict[str, object]]) -> MagicMock:
    store = MagicMock()
    store.get_accounting_events_sync.return_value = events
    return store


def _carry_strategy() -> MagicMock:
    strategy = MagicMock()
    strategy.deployment_id = _DEPLOYMENT
    strategy.chain = _CHAIN
    strategy.wallet_address = _WALLET
    # USDT is deliberately NOT tracked — matching the frozen DB, whose
    # wallet_balances_json held only native BNB. That absence is precisely why
    # the VIB-5057 classifier skipped the lot ``capped_to_zero`` and emitted no
    # backed synthetic row, leaving the discovered leg unbacked.
    strategy._get_tracked_tokens.return_value = ["WBNB", "USDC", "BNB"]
    strategy.get_open_positions.return_value = TeardownPositionSummary(
        deployment_id=_DEPLOYMENT,
        timestamp=datetime.now(UTC),
        positions=[
            PositionInfo(
                position_type=PositionType.SUPPLY,
                position_id="aave-v3-supply-WBNB-bsc",
                chain=_CHAIN,
                protocol="aave_v3",
                value_usd=_COLLATERAL_USD,
                details={"asset": "WBNB", "amount": "0.005", "wallet_address": _WALLET},
            ),
            PositionInfo(
                position_type=PositionType.BORROW,
                position_id="aave-v3-borrow-USDC-bsc",
                chain=_CHAIN,
                protocol="aave_v3",
                value_usd=-_DEBT_USD,
                details={"asset": "USDC", "amount": "0.88", "wallet_address": _WALLET},
            ),
            PositionInfo(
                position_type=PositionType.TOKEN,
                position_id="pancakeswap-swap-USDT-bsc",
                chain=_CHAIN,
                protocol="pancakeswap_v3",
                value_usd=Decimal("0"),
                details={"asset": "USDT", "amount": str(_HELD_USDT), "origin": "swapped_from_borrow"},
            ),
        ],
    )
    return strategy


def _carry_market(*, usdt_balance: Decimal = _HELD_USDT) -> MagicMock:
    market = MagicMock()
    prices = {"USDT": _USDT_PRICE, "USDC": Decimal("1"), "BNB": Decimal("585.29"), "WBNB": Decimal("585.41713")}
    balances = {"BNB": Decimal("0.00106147495"), "USDT": usdt_balance}

    def mock_price(token: str, quote: str = "USD", *, chain: str | None = None):
        if token in prices:
            return prices[token]
        raise ValueError(f"No price for {token}")

    def mock_balance(token: str, protocol: str | None = None, *, chain: str | None = None, price=None):
        if token in balances:
            result = MagicMock()
            result.balance = balances[token]
            return result
        raise ValueError(f"No balance for {token}")

    market.price = mock_price
    market.balance = mock_balance
    return market


def _snapshot(
    *,
    events: list[dict[str, object]] | None = None,
    usdt_balance: Decimal = _HELD_USDT,
    drain_incomplete: bool = False,
):
    valuer = PortfolioValuer()
    valuer.set_accounting_context(_accounting_store(_carry_events() if events is None else events), _DEPLOYMENT)
    if drain_incomplete:
        valuer._drain_barrier_incomplete = True
    return valuer.value(_carry_strategy(), _carry_market(usdt_balance=usdt_balance))


def _usdt_leg(snapshot):
    return next(
        p for p in snapshot.positions if p.position_type == PositionType.TOKEN and p.details.get("asset") == "USDT"
    )


class TestTokenLegIsBackedBySwapLots:
    def test_the_held_token_leg_carries_the_swap_acquisition_cost(self):
        """The core defect: the leg's basis was permanently absent."""
        leg = _usdt_leg(_snapshot())

        assert leg.cost_basis_usd == _SWAP_COST_USD
        assert leg.unrealized_pnl_usd == leg.value_usd - _SWAP_COST_USD
        # Data-shape marker (VIB-4636 discipline) — how the reader recognises a
        # leg whose lot MTM is already folded into the position sums.
        assert leg.details.get("cost_basis_source") == "swap_inventory_lots"

    def test_deployed_capital_includes_the_token_leg(self):
        """``deployed_capital_usd`` is Σ abs(cost_basis) — the leg must reach it."""
        snapshot = _snapshot()

        assert snapshot.deployed_capital_usd == (_COLLATERAL_COST_USD + _DEBT_COST_USD + _SWAP_COST_USD)

    def test_nav_basis_coverage_is_not_stamped_once_every_leg_is_backed(self):
        """VIB-6308's guard must stand down — there is nothing left to suppress.

        The stamp is written only when a NAV leg is unbacked, so its ABSENCE is
        the assertion that the carry now has full coverage.
        """
        snapshot = _snapshot()

        assert "nav_basis_coverage" not in (snapshot.snapshot_metadata or {})

    def test_strategy_pnl_on_a_flat_carry_is_near_zero_not_the_whole_holding(self):
        """Acceptance: not ``—`` and not ``+42%``.

        Reproduces the tile arithmetic
        (``_detail_header._strategy_pnl_usd``: ``open_position_nav -
        deployed_capital_usd``, where the leveraged read path replaces the gross
        column with the net equity cost — ``quant_aggregations`` VIB-4983
        follow-up).
        """
        snapshot = _snapshot()
        _count, debt_mark, debt_cost, net_cost = net_debt_from_snapshot(snapshot)

        open_position_nav = snapshot.total_value_usd - debt_mark
        assert debt_cost > Decimal("0")  # the leveraged branch is the live one
        unrealized = open_position_nav - net_cost

        # A flat carry: the only real PnL is Aave interest accrual (cents), NOT
        # the $0.88 holding. Pre-fix this was the full held value.
        assert abs(unrealized) < Decimal("0.05"), unrealized
        assert abs(unrealized) < _SWAP_COST_USD / 2


def _assert_leg_is_unmeasured(snapshot) -> None:
    """The leg carries NO basis, and nothing downstream believes it does.

    Asserting ``not leg.cost_basis_usd`` alone CANNOT discriminate here, and a
    first draft of this suite that did so let a real mutation through: booking a
    fabricated ``Decimal("0")`` basis produces a falsy value that the serializer
    drops and the coverage stamp reads as missing, so the leg looks identical on
    that axis. The observable difference is everything else the enrichment does
    on its way to that value — it stamps ``cost_basis_source`` and the writer
    then names the token in ``folded_into_positions``, which SUPPRESSES the
    additive inventory-MTM term for a lot nobody actually measured. Assert those
    too, or the Empty≠Zero contract this class is named for is untested.
    """
    leg = _usdt_leg(snapshot)
    metadata = snapshot.snapshot_metadata or {}

    assert not leg.cost_basis_usd
    assert "cost_basis_source" not in (leg.details or {})
    assert "folded_into_positions" not in (metadata.get("swap_inventory") or {})
    # The VIB-6308 guard must stay armed for an unbacked NAV leg.
    coverage = metadata.get("nav_basis_coverage")
    assert coverage is not None
    assert coverage["legs_missing_basis"] >= 1


class TestEmptyIsNotZero:
    """Every refusal must leave the leg UNMEASURED, never a fabricated zero.

    A fabricated basis is worse than none: it makes an unbacked leg look
    measured and silently disarms the VIB-6308 coverage guard.
    """

    def test_a_holding_larger_than_the_swap_lots_stays_unmeasured(self):
        """The wallet can hold more than the SWAP lots account for.

        A USDT BORROW lands in the same fungible wallet pool but is minted
        ``source="BORROW"``, which ``iter_open_swap_lots`` excludes (VIB-3964).
        Booking the swap cost against the larger holding would understate the
        basis — this bug again, smaller, while reading as fully backed.
        """
        surplus = _HELD_USDT * 2
        snapshot = _snapshot(usdt_balance=surplus)

        assert _usdt_leg(snapshot).value_usd == surplus * _USDT_PRICE  # the mark still counts
        _assert_leg_is_unmeasured(snapshot)

    def test_an_unmeasured_lot_cost_leaves_the_leg_unmeasured(self):
        """A SWAP with no ``amount_out_usd`` replays to a ``None`` basis."""
        _assert_leg_is_unmeasured(_snapshot(events=_carry_events(swap_cost_usd=None)))

    def test_no_swap_lot_for_the_token_leaves_the_leg_unmeasured(self):
        """A token acquired by something other than a SWAP has no lot to read."""
        events = [e for e in _carry_events() if e["event_type"] != "SWAP"]

        _assert_leg_is_unmeasured(_snapshot(events=events))

    def test_a_drain_incomplete_snapshot_leaves_the_leg_unmeasured(self):
        """VIB-5406: the event stream may be missing this unit's disposals, so a
        lot that reads as still-held may already be sold. Same fail-closed rule
        the swap/PT inventory classifiers use."""
        _assert_leg_is_unmeasured(_snapshot(drain_incomplete=True))

    def test_a_partial_holding_prorates_the_basis(self):
        """Holding LESS than the lots (partial external transfer) pro-rates."""
        half = _HELD_USDT / 2
        leg = _usdt_leg(_snapshot(usdt_balance=half))

        expected = _SWAP_COST_USD * (half / _HELD_USDT)
        assert leg.cost_basis_usd == expected
        assert leg.cost_basis_usd < _SWAP_COST_USD

    @pytest.mark.parametrize("poison", ["NaN", "-NaN", "sNaN", "Infinity", "-Infinity", "-1", "0", "not-a-number"])
    def test_a_poisoned_quantity_is_refused_rather_than_defeating_every_guard(self, poison):
        """``details`` is strategy-supplied, and ``Decimal("NaN")`` parses fine.

        A NaN quantity defeats EVERY guard in ``_enrich_token_pnl`` at once,
        because a NaN ordering comparison is False in both directions:
        ``quantity <= 0`` passes AND ``quantity > remaining`` passes. The leg
        would then be stamped ``cost_basis_usd = NaN`` — which is **truthy**, so
        it reads as measured: the VIB-6308 guard stands down and the token is
        named in ``folded_into_positions``, suppressing the reader's inventory
        term for a lot nobody measured. Fail-open on the exact property this
        change exists to protect (CodeRabbit, 2026-08-02).

        Exercised directly on the enricher rather than through ``value()``: the
        spot repricer sanitises its own output, so routing a poisoned amount
        through the full path would test the repricer's guard, not this one.
        The contract is that the enricher refuses a poisoned quantity no matter
        who hands it over.
        """
        valuer = PortfolioValuer()
        valuer.set_accounting_context(_accounting_store(_carry_events()), _DEPLOYMENT)
        valuer._snapshot_events_flat = _carry_events()

        leg = PositionValue(
            position_type=PositionType.TOKEN,
            protocol="pancakeswap_v3",
            chain=_CHAIN,
            value_usd=_HELD_USDT * _USDT_PRICE,
            label="pancakeswap_v3 TOKEN",
            details={"asset": "USDT", "amount": poison},
        )
        valuer._enrich_token_pnl(leg)

        assert not leg.cost_basis_usd
        assert leg.cost_basis_usd.is_finite()
        assert "cost_basis_source" not in leg.details


class TestOnlyNavCountingLegsAreBacked:
    """A leg's cost must land on the SAME side of the NAV partition as its mark.

    ``total_value_usd`` excludes a VIB-4909 wallet pseudo-position (its value is
    already counted once inside ``wallet_balances``). ``deployed_capital_usd``
    excludes nothing — it is Σ abs(cost_basis_usd) over every position. So
    backing a wallet-overlapping leg adds cost with NO matching open-NAV mark,
    and Strategy PnL (``open_position_nav − deployed_capital_usd``) moves
    negative by the whole basis: a phantom LOSS.

    Reachable in exactly the classifier skips this path otherwise targets — a
    declared ``quote_token`` (``numeraire_cash``, portfolio-scale after a
    de-risk) or a ``dust_residual``, both of which require the wallet to hold the
    token. The carry escapes only because its USDT is absent from the wallet map,
    which is a property of that data, not a guard. (Grok, high-risk panel
    2026-08-02.)
    """

    def test_a_wallet_overlapping_token_leg_is_not_backed(self):
        """USDT tracked into the wallet ⇒ pseudo-position ⇒ no basis, no fold."""
        strategy = _carry_strategy()
        strategy._get_tracked_tokens.return_value = ["WBNB", "USDC", "BNB", "USDT"]

        valuer = PortfolioValuer()
        valuer.set_accounting_context(_accounting_store(_carry_events()), _DEPLOYMENT)
        snapshot = valuer.value(strategy, _carry_market())

        leg = _usdt_leg(snapshot)
        assert not leg.cost_basis_usd
        assert "cost_basis_source" not in (leg.details or {})
        stamp = (snapshot.snapshot_metadata or {}).get("swap_inventory") or {}
        assert "folded_into_positions" not in stamp

    def test_deployed_capital_does_not_gain_a_cost_whose_mark_left_nav(self):
        """The phantom loss, stated as the number it would have been."""
        strategy = _carry_strategy()
        strategy._get_tracked_tokens.return_value = ["WBNB", "USDC", "BNB", "USDT"]

        valuer = PortfolioValuer()
        valuer.set_accounting_context(_accounting_store(_carry_events()), _DEPLOYMENT)
        snapshot = valuer.value(strategy, _carry_market())

        # The USDT mark is NOT in open-position NAV (it is wallet cash)...
        assert _usdt_leg(snapshot).value_usd > Decimal("0")
        assert snapshot.total_value_usd == _COLLATERAL_USD
        # ...so its cost must not be in deployed capital either.
        assert snapshot.deployed_capital_usd == (_COLLATERAL_COST_USD + _DEBT_COST_USD)

    def test_an_unknown_wallet_partition_refuses_rather_than_assumes(self):
        """A caller that bypassed ``value()`` has no index — that is not a licence."""
        valuer = PortfolioValuer()
        valuer.set_accounting_context(_accounting_store(_carry_events()), _DEPLOYMENT)
        valuer._snapshot_events_flat = _carry_events()
        valuer._snapshot_wallet_index = None

        leg = PositionValue(
            position_type=PositionType.TOKEN,
            protocol="pancakeswap_v3",
            chain=_CHAIN,
            value_usd=_HELD_USDT * _USDT_PRICE,
            label="pancakeswap_v3 TOKEN",
            details={"asset": "USDT", "amount": str(_HELD_USDT)},
        )
        valuer._enrich_token_pnl(leg)

        assert not leg.cost_basis_usd


class TestChainScope:
    """Lots are matched per ``(chain, token)``, never per token alone.

    A multi-chain deployment (VIB-5722) can swap the same symbol on two chains.
    A symbol-only lookup hands a chain-A leg the combined cross-chain quantity
    and cost, and a symbol-only exclusion stamp suppresses the reader's inventory
    term for that symbol on EVERY chain. Accounting identity is chain-scoped
    (blueprint 27). (Codex, high-risk panel 2026-08-02.)
    """

    def test_a_leg_does_not_read_another_chains_lot(self):
        valuer = PortfolioValuer()
        valuer.set_accounting_context(_accounting_store(_carry_events()), _DEPLOYMENT)
        valuer._snapshot_events_flat = _carry_events()
        valuer._snapshot_wallet_index = _build_wallet_match_index([])

        # Same symbol, same quantity — but the lots were minted on bsc.
        leg = PositionValue(
            position_type=PositionType.TOKEN,
            protocol="pancakeswap_v3",
            chain="arbitrum",
            value_usd=_HELD_USDT * _USDT_PRICE,
            label="TOKEN",
            details={"asset": "USDT", "amount": str(_HELD_USDT)},
        )
        valuer._enrich_token_pnl(leg)

        assert not leg.cost_basis_usd

        # ...and the same leg on the lots' own chain IS backed, so the refusal
        # above is chain scoping and not a dead lookup.
        leg.chain = _CHAIN
        valuer._enrich_token_pnl(leg)
        assert leg.cost_basis_usd == _SWAP_COST_USD

    def test_the_stamp_is_chain_qualified(self):
        stamp = (_snapshot().snapshot_metadata or {}).get("swap_inventory") or {}

        assert stamp.get("folded_into_positions") == [f"{_CHAIN}:usdt"]

    def test_the_exclusion_only_matches_the_stamped_chain(self):
        prices = {f"{_CHAIN}:0x55d3": {"symbol": "USDT", "price_usd": str(_USDT_PRICE)}}

        # Another chain's stamp must not silence this chain's lot.
        assert (
            compute_inventory_unrealized(
                _carry_events(), _DEPLOYMENT, prices, exclude_tokens=frozenset({"arbitrum:usdt"})
            )
            is not None
        )


class TestNoDoubleCountWithTheAdditiveInventoryTerm:
    """The folded lot must not ALSO be marked by the legacy VIB-4984 term.

    That term fires whenever the snapshot is not stamped
    ``swap_inventory.status == "applied"`` — which is exactly the
    ``capped_to_zero`` case this enrichment targets. Without the exclusion the
    token's ``mark - cost`` would enter Strategy PnL twice.
    """

    def test_the_writer_names_the_folded_token(self):
        snapshot = _snapshot()
        stamp = (snapshot.snapshot_metadata or {}).get("swap_inventory") or {}

        assert stamp.get("folded_into_positions") == [f"{_CHAIN}:usdt"]
        # The classifier itself produced no row for it — the fold is the ONLY
        # thing backing this leg.
        assert stamp.get("status") != "applied"
        assert (stamp.get("skipped") or {}).get("usdt") == "capped_to_zero"

    def test_a_folded_token_is_excluded_from_the_additive_term(self):
        prices = {f"{_CHAIN}:0x55d3": {"symbol": "USDT", "price_usd": str(_USDT_PRICE)}}

        assert (
            compute_inventory_unrealized(
                _carry_events(), _DEPLOYMENT, prices, exclude_tokens=frozenset({f"{_CHAIN}:usdt"})
            )
            is None
        )

    def test_a_co_held_token_still_in_cash_keeps_its_term(self):
        """The exclusion is per-token, not whole-snapshot.

        Folding USDT must not silence an unrelated open WETH lot — that would
        turn a double-count fix into a silent DROP of inventory MTM.
        """
        events = [
            *_carry_events(),
            _event(
                "SWAP",
                {
                    "event_type": "SWAP",
                    "token_in": "USDC",
                    "amount_in": "10",
                    "token_out": "WETH",
                    "amount_out": "0.004",
                    "amount_out_usd": "10.00",
                    "swap_position_key": f"swap:{_CHAIN}:{_WALLET.lower()}",
                },
            ),
        ]
        prices = {
            f"{_CHAIN}:0x55d3": {"symbol": "USDT", "price_usd": str(_USDT_PRICE)},
            f"{_CHAIN}:0x2170": {"symbol": "WETH", "price_usd": "3000"},
        }

        measured = compute_inventory_unrealized(
            events, _DEPLOYMENT, prices, exclude_tokens=frozenset({f"{_CHAIN}:usdt"})
        )

        # WETH survives the exclusion and is marked on its own: 0.004 × 3000 − 10.
        assert measured == (Decimal("0.004") * Decimal("3000")) - Decimal("10.00")

    def test_a_pre_vib6362_snapshot_keeps_the_additive_term(self):
        """Version tolerance: no stamp ⇒ byte-identical to the old behaviour."""
        prices = {f"{_CHAIN}:0x55d3": {"symbol": "USDT", "price_usd": str(_USDT_PRICE)}}

        measured = compute_inventory_unrealized(_carry_events(), _DEPLOYMENT, prices)

        assert measured is not None
        assert measured == (_HELD_USDT * _USDT_PRICE) - _SWAP_COST_USD

    @pytest.mark.parametrize(
        "stamp",
        [
            None,
            {},
            {"folded_into_positions": "bsc:usdt"},  # not a list
            {"folded_into_positions": [None, 7]},  # non-string members
        ],
    )
    def test_a_malformed_stamp_never_widens_the_exclusion(self, stamp):
        """A malformed stamp must not delete a legitimate inventory term."""
        from almanak.gateway.services.dashboard_service import _folded_inventory_tokens

        assert _folded_inventory_tokens(stamp) == frozenset()


@pytest.mark.asyncio
async def test_getcoststack_actually_passes_the_stamp_to_the_exclusion():
    """The WIRE: snapshot stamp → ``_folded_inventory_tokens`` → ``exclude_tokens``.

    Asserting the writer stamps, and separately that the function honours a
    hand-built ``exclude_tokens``, leaves the call site itself untested — drop
    ``exclude_tokens=`` at the gateway and every other test in this file stays
    green while the double-count returns. This drives the real ``GetCostStack``
    and captures what it passed. (Grok, high-risk panel 2026-08-02.)
    """
    from almanak.gateway.services import dashboard_service as ds

    captured: dict[str, object] = {}

    def _spy(events, deployment_id, prices, exclude_tokens=frozenset()):
        captured["exclude_tokens"] = exclude_tokens
        return None

    svc = ds.DashboardServiceServicer.__new__(ds.DashboardServiceServicer)
    svc.settings = SimpleNamespace()
    svc._state_manager = MagicMock()
    svc._initialized = True
    svc._strategies_root = None
    svc._cached_positions = {}

    snapshot = SimpleNamespace(
        token_prices={},
        snapshot_metadata={"swap_inventory": {"status": "unmeasured", "folded_into_positions": [f"{_CHAIN}:usdt"]}},
    )

    async def _inputs(_deployment_id):
        return (MagicMock(), [snapshot], MagicMock(), _carry_events(), None)

    async def _ensure():
        return None

    svc._get_quant_inputs = _inputs
    svc._ensure_initialized = _ensure

    with patch.object(ds, "_folded_inventory_tokens", wraps=ds._folded_inventory_tokens) as parser:
        with patch(
            "almanak.framework.dashboard.quant_aggregations.compute_inventory_unrealized",
            _spy,
        ):
            await svc.GetCostStack(gateway_pb2.GetCostStackRequest(deployment_id=_DEPLOYMENT), MagicMock())

    parser.assert_called_once()
    assert captured["exclude_tokens"] == frozenset({f"{_CHAIN}:usdt"})
