"""VIB-6283 — the LP earn / IL buckets across the gateway wire.

``fees_earned_usd`` and ``il_usd`` became presence-aware in three places at
once: the producer (``gateway/services/dashboard_service.py`` — measured-meter
⇒ ``""`` sentinel), the wire (``gateway.proto`` — string fields), and the
client (``framework/dashboard/gateway_client.py::_convert_cost_stack`` —
``_safe_optional_decimal``).

A slip anywhere in that chain **fails CLOSED**: the value arrives as ``""``,
decodes to ``None``, and every LP money tile renders "—". That reads as
correct Empty≠Zero behaviour, so nothing looks broken — it is the failure mode
most likely to ship silently, and no single-layer test can see it. These tests
drive the REAL servicer expression and the REAL client converter over one
round trip and assert both ends.

The pair that matters most is the last one: a careless ``""``-sentinel
implementation (``str(x) if x else ""``) turns a MEASURED ``Decimal("0")``
into unmeasured, because ``Decimal("0")`` is falsey. The measured meters exist
precisely so presence is decided by the meter, not by truthiness.

Rows come from ``tests/fixtures/lp_dashboard_vib6283/
anvil_fee_induction_run.json`` — the real Anvil Arbitrum fork run — so the
values crossing the wire here are the ones a real deployment produced.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest

from almanak.framework.dashboard.gateway_client import (
    CostStackInfo,
    PnLSummary,
    _convert_cost_stack,
    _convert_pnl_summary,
)
from almanak.framework.dashboard.pages._detail_header import _net_realized_pnl_usd, _strategy_apr_pct
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.dashboard_service import DashboardServiceServicer

_FIXTURE = Path(__file__).resolve().parents[2] / "fixtures" / "lp_dashboard_vib6283" / "anvil_fee_induction_run.json"
DEPLOYMENT_ID = "deployment:b3816ff5ddb8"


def _fixture_events() -> list[dict[str, Any]]:
    """The run's accounting_events rows, in the exact shape the gateway reads."""
    return json.loads(_FIXTURE.read_text())["accounting_events"]


def _event(event_type: str) -> dict[str, Any]:
    return next(r for r in _fixture_events() if r["event_type"] == event_type)


def _with_payload(row: dict[str, Any], **overrides: Any) -> dict[str, Any]:
    """Copy a fixture row with payload keys overridden (or removed via ``None``
    for the key itself — pass ``_drop=("key",)``)."""
    payload = json.loads(row["payload_json"])
    for key in overrides.pop("_drop", ()):
        payload.pop(key, None)
    payload.update(overrides)
    return {**row, "payload_json": json.dumps(payload)}


_CLOSE_PAYLOAD = json.loads(_event("LP_CLOSE")["payload_json"])
FEES_EARNED_USD = _CLOSE_PAYLOAD["fees_total_usd"]  # "88.50577177198135678775255617"
IL_USD = _CLOSE_PAYLOAD["il_usd"]  # "0.041188405836821282508"


def _snapshot() -> SimpleNamespace:
    return SimpleNamespace(
        total_value_usd="593170.69194600",
        available_cash_usd="593170.69194600",
        value_confidence="HIGH",
        deployed_capital_usd="0",
        positions_json="[]",
        token_prices={},
        snapshot_metadata=None,
        timestamp=datetime.now(tz=UTC),
    )


def _metrics() -> SimpleNamespace:
    row = json.loads(_FIXTURE.read_text())["portfolio_metrics"][0]
    return SimpleNamespace(
        deposits_usd="0",
        withdrawals_usd="0",
        initial_value_usd=row["initial_value_usd"],
        timestamp=datetime.now(tz=UTC),
    )


def _servicer(events: list[dict[str, Any]]) -> DashboardServiceServicer:
    """A servicer whose input load is stubbed, so the test exercises the REAL
    ``GetCostStack`` / ``GetPnLSummary`` serialization expressions."""
    svc = DashboardServiceServicer.__new__(DashboardServiceServicer)
    svc.settings = SimpleNamespace()
    svc._state_manager = MagicMock()
    svc._initialized = True
    svc._strategies_root = None
    svc._cached_positions = {}

    async def _inputs(_deployment_id: str) -> tuple[Any, list[Any], list[Any], list[Any], Any]:
        return (_metrics(), [_snapshot()], [], events, None)

    async def _drawdown(_deployment_id: str) -> None:
        return None

    async def _ensure() -> None:
        return None

    svc._get_quant_inputs = _inputs  # type: ignore[method-assign]
    svc._get_lifetime_drawdown = _drawdown  # type: ignore[method-assign]
    svc._ensure_initialized = _ensure  # type: ignore[method-assign]
    return svc


async def _round_trip(events: list[dict[str, Any]]) -> tuple[gateway_pb2.CostStackInfo, CostStackInfo]:
    """Producer → wire → client, with both ends returned for assertion."""
    proto = await _servicer(events).GetCostStack(
        gateway_pb2.GetCostStackRequest(deployment_id=DEPLOYMENT_ID),
        MagicMock(),
    )
    return proto, _convert_cost_stack(proto)


# ── Measured: the real fork numbers survive the full loop ────────────────


@pytest.mark.asyncio
async def test_measured_lp_buckets_cross_the_wire_at_full_precision() -> None:
    """A real closed LP position: $88.5057… of fees and $0.0411… of IL must
    reach the client as Decimals, not as the "" that renders "—". Full
    precision, not a 2-dp string: the wire is the accounting lane, and rounding
    here would silently cap what any consumer can ever show.

    The second half is RC4 on the wire. ``LP_COLLECT_FEES`` appeared ZERO times
    in ``quant_aggregations.py`` despite being a first-class accounting event
    that CREDITS the wallet, so a strategy that harvests without closing had
    real USD income in wallet PnL and none in Σ component PnL (a G6 gap). The
    harvest row is the fixture's own LP_CLOSE payload re-typed as a collect
    (fees only, no realized PnL, no IL — a harvest does not realise impermanent
    loss), so its shape is a real one from the same run.
    """
    proto, client = await _round_trip([_event("LP_OPEN"), _event("LP_CLOSE"), _event("SWAP")])

    assert proto.fees_earned_usd == FEES_EARNED_USD
    assert proto.il_usd == IL_USD
    assert client.fees_earned_usd == Decimal(FEES_EARNED_USD)
    assert client.il_usd == Decimal(IL_USD)

    harvest = _with_payload(
        {**_event("LP_CLOSE"), "event_type": "LP_COLLECT_FEES"},
        event_type="LP_COLLECT_FEES",
        fees_total_usd="1.234567890123456789",
        realized_pnl_usd="0",
        _drop=("il_usd",),
    )
    proto, client = await _round_trip([_event("LP_OPEN"), harvest, _event("LP_CLOSE")])

    expected = Decimal(FEES_EARNED_USD) + Decimal("1.234567890123456789")
    assert Decimal(proto.fees_earned_usd) == expected
    assert client.fees_earned_usd == expected
    # A harvest is not an IL observation, so it must not mark the IL bucket
    # measured-or-missing either way — the close's IL still travels intact.
    assert client.il_usd == Decimal(IL_USD)


# ── Unmeasured: "" on the wire, None on the client ───────────────────────


@pytest.mark.asyncio
async def test_open_lp_position_is_unmeasured_end_to_end() -> None:
    """An OPEN position has no close/collect row, so nothing has MEASURED its
    fees. The honest wire value is "" and the honest client value is ``None``
    — fees are accruing on-chain (~$88 on this very position) and "$0.00"
    would contradict the chain."""
    proto, client = await _round_trip([_event("LP_OPEN")])

    assert proto.fees_earned_usd == ""
    assert proto.il_usd == ""
    assert client.fees_earned_usd is None
    assert client.il_usd is None


@pytest.mark.asyncio
async def test_close_without_a_fee_field_is_unmeasured_not_zero() -> None:
    """18 of 66 LP_CLOSE rows in the run corpus carry no ``fees_total_usd``
    (a bundled-fee close the parser could not attribute). Those were reaching
    dashboards as a confident ``$0.00``."""
    close = _with_payload(_event("LP_CLOSE"), _drop=("fees_total_usd", "il_usd"))

    proto, client = await _round_trip([_event("LP_OPEN"), close])

    assert proto.fees_earned_usd == ""
    assert proto.il_usd == ""
    assert client.fees_earned_usd is None
    assert client.il_usd is None
    # The rest of the same row still travels — suppression is per-bucket.
    assert client.realized_pnl_usd == Decimal(_CLOSE_PAYLOAD["realized_pnl_usd"])


@pytest.mark.asyncio
async def test_explicit_null_fee_is_unmeasured_not_zero() -> None:
    """``lp_accounting`` writes ``fees_total_usd: null`` when the quantity is
    genuinely unmeasurable. A present-but-null key must be treated exactly like
    an absent one — Empty ≠ Zero, and ``None`` is not "0"."""
    close = _with_payload(_event("LP_CLOSE"), fees_total_usd=None, il_usd=None)

    proto, client = await _round_trip([close])

    assert proto.fees_earned_usd == ""
    assert client.fees_earned_usd is None
    assert proto.il_usd == ""
    assert client.il_usd is None


# ── The measured zero — the bug a careless ""-sentinel introduces ────────


@pytest.mark.asyncio
async def test_measured_zero_survives_the_wire_while_an_open_position_stays_unmeasured() -> None:
    """``Decimal("0")`` is FALSEY. Any presence test written as truthiness
    (``str(x) if x else ""``) silently reclassifies a close that genuinely
    earned nothing as "we never measured this" — trading one wrong render for
    another. Presence must come from the measured meter, not from the value.

    Asserted as a CONTRAST, because that is the whole point of the sentinel:
    a measured-zero close and an open position used to be byte-identical on
    the wire, and must now differ.
    """
    _, measured = await _round_trip([_with_payload(_event("LP_CLOSE"), fees_total_usd="0", il_usd="0")])
    proto_measured, _ = await _round_trip([_with_payload(_event("LP_CLOSE"), fees_total_usd="0", il_usd="0")])
    _, unmeasured = await _round_trip([_event("LP_OPEN")])

    # A measured zero is a VALUE, not a sentinel.
    assert proto_measured.fees_earned_usd != "", "a measured zero was serialised as the unmeasured sentinel"
    assert proto_measured.il_usd != "", "a measured zero was serialised as the unmeasured sentinel"
    assert Decimal(proto_measured.fees_earned_usd) == Decimal("0")
    assert measured.fees_earned_usd == Decimal("0")
    assert measured.fees_earned_usd is not None
    assert measured.il_usd == Decimal("0")
    assert measured.il_usd is not None

    # ...and it is distinguishable from "never measured".
    assert unmeasured.fees_earned_usd is None
    assert measured.fees_earned_usd != unmeasured.fees_earned_usd


# ── Client decode in isolation ───────────────────────────────────────────


def test_client_decodes_the_lp_sentinels_without_the_server() -> None:
    """Pin the client half on its own, so a producer change cannot mask a
    decoder regression (or the reverse)."""
    unmeasured = _convert_cost_stack(gateway_pb2.CostStackInfo(fees_earned_usd="", il_usd=""))
    assert unmeasured.fees_earned_usd is None
    assert unmeasured.il_usd is None

    measured = _convert_cost_stack(gateway_pb2.CostStackInfo(fees_earned_usd=FEES_EARNED_USD, il_usd=IL_USD))
    assert measured.fees_earned_usd == Decimal(FEES_EARNED_USD)
    assert measured.il_usd == Decimal(IL_USD)

    measured_zero = _convert_cost_stack(gateway_pb2.CostStackInfo(fees_earned_usd="0", il_usd="0"))
    assert measured_zero.fees_earned_usd == Decimal("0")
    assert measured_zero.il_usd == Decimal("0")


# ── age_days_exact across the same wire ──────────────────────────────────


@pytest.mark.asyncio
async def test_age_days_exact_is_produced_and_decoded() -> None:
    """The annualisation denominator is a NEW proto field; if it does not
    serialise, APR silently reverts to the whole-day behaviour that made it
    uncomputable on every sub-24h run."""
    proto = await _servicer([]).GetPnLSummary(
        gateway_pb2.GetPnLSummaryRequest(deployment_id=DEPLOYMENT_ID),
        MagicMock(),
    )

    assert proto.age_days_exact != "", "the gateway did not emit age_days_exact"
    client = _convert_pnl_summary(proto)
    assert client.age_days_exact == Decimal(proto.age_days_exact)


# ── Backward compatibility with a gateway that predates the field ────────


def test_old_gateway_leaves_age_days_exact_unmeasured() -> None:
    """An old gateway never sets the field, so it arrives as proto3's default
    ``""``. That must decode to ``None`` (unmeasured), never ``Decimal("0")``
    — a zero denominator is not "instantaneous", it is "unknown"."""
    client = _convert_pnl_summary(
        gateway_pb2.PnLSummary(
            deployed_usd="1000",
            nav_usd="1000",
            value_confidence="HIGH",
            age_days=3,
        )
    )

    assert client.age_days_exact is None
    assert client.age_days == 3


def test_pnl_summary_constructed_without_the_new_field_is_unmeasured() -> None:
    """The field is additive + defaulted, so every existing constructor in the
    tree (and every third-party one) keeps working and reports "unknown"."""
    p = PnLSummary(
        deployed_usd=Decimal("1000"),
        nav_usd=Decimal("1000"),
        lifetime_pnl_usd=None,
        lifetime_pnl_pct=None,
        net_apr_pct=None,
        max_drawdown_pct=Decimal("0"),
        current_drawdown_pct=Decimal("0"),
        value_confidence="HIGH",
        age_days=3,
        deployed_capital_usd=Decimal("1000"),
        available_cash_usd=Decimal("0"),
        open_position_count=1,
        primary_risk_kind="lp",
        primary_risk_label="Range",
        primary_risk_value="in-range",
        primary_risk_color="green",
    )

    assert p.age_days_exact is None


def test_apr_against_an_old_gateway_reproduces_whole_day_behaviour_exactly() -> None:
    """With ``age_days_exact`` unmeasured, APR must fall back to whole days and
    return byte-for-byte what the pre-change formula returned. A "safer"
    fallback (suppressing to ``None``, or flooring the denominator) would be a
    silent behaviour change on every deployment still running an old gateway.
    """
    strategy_pnl = Decimal("771.86")
    capital = Decimal("1261091.44")

    pre_change = (strategy_pnl / capital) * Decimal("365") / Decimal("3") * Decimal("100")

    assert _strategy_apr_pct(strategy_pnl, capital, 3, None) == pre_change
    # Same call without the new argument at all (the pre-change call shape).
    assert _strategy_apr_pct(strategy_pnl, capital, 3) == pre_change
    # And the new field, when present, wins over the whole-day label.
    assert _strategy_apr_pct(strategy_pnl, capital, 3, Decimal("3.5")) != pre_change


def test_widening_the_lp_buckets_to_optional_breaks_no_existing_consumer() -> None:
    """``CostStackInfo.fees_earned_usd`` / ``il_usd`` widened from ``Decimal``
    to ``Decimal | None``. Two halves of one compatibility claim:

    * the pre-VIB-6283 constructor call still builds a valid object (no new
      REQUIRED field), and
    * every consumer now has to survive ``None``. ``_net_realized_pnl_usd`` is
      the one on the Strategy PnL path — it must treat unmeasured as a zero
      CONTRIBUTION (the only arithmetic available to a sum that cannot express
      "unknown") and produce exactly what it produced before: not raise, and
      not shift the headline.
    """
    base: dict[str, Any] = {
        "cost_gas_usd": Decimal("1"),
        "cost_protocol_fees_usd": None,
        "cost_slippage_usd": None,
        "interest_paid_usd": Decimal("0"),
        "interest_earned_usd": Decimal("0"),
        "funding_paid_usd": Decimal("0"),
        "funding_earned_usd": Decimal("0"),
        "realized_pnl_usd": Decimal("10"),
    }

    pre_change = CostStackInfo(fees_earned_usd=Decimal("5"), il_usd=Decimal("0"), **base)
    assert pre_change.fees_earned_usd == Decimal("5")
    assert pre_change.il_usd == Decimal("0")
    assert _net_realized_pnl_usd(pre_change) == Decimal("14")  # 10 + 5 − 1 gas

    unmeasured = _net_realized_pnl_usd(CostStackInfo(fees_earned_usd=None, il_usd=None, **base))
    measured_zero = _net_realized_pnl_usd(CostStackInfo(fees_earned_usd=Decimal("0"), il_usd=Decimal("0"), **base))

    assert unmeasured == Decimal("9")  # 10 realized − 1 gas
    assert unmeasured == measured_zero, "the headline must not move when a bucket is unmeasured"
