"""VIB-4977 — lending CLOSE attribution carries a signed realized net_pnl_usd.

Before this fix, the lending CLOSE PositionEvent's ``attribution_json`` carried
a populated ``lending_v1`` payload (collateral/debt/HF after-state) but NO
``net_pnl_usd`` key, so ``almanak strat pnl`` scored every leveraged-lending
close as unattributed → ``Win rate: — (0/0 scored closes)``.

The fix stamps the signed realized PnL (the FIFO interest split the Layer-5
lending handler already computes) onto the matching lending PositionEvent during
the AccountingProcessor drain. These tests pin:

* the pure sign convention (matches ``position_pnl.compute_position_pnl``),
* that the seed-time payload still omits the key (back-filled, not seeded),
* that ``strat_pnl._pnl_from_attribution`` now scores a lending close,
* that the drain back-fill joins deterministically on
  ``(position_key, ledger_entry_id)`` so a partial DECREASE and the final
  CLOSE each receive ONLY their own action's realized PnL (no double-count),
* that an UNAVAILABLE interest delta leaves the payload untouched (Empty ≠ Zero).
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.accounting.basis import FIFOBasisStore
from almanak.framework.accounting.models import (
    AccountingConfidence,
    AccountingIdentity,
    LendingAccountingEvent,
    LendingEventType,
)
from almanak.framework.accounting.processor import (
    AccountingProcessor,
    _find_position_event_for_ledger,
    _is_lending_event,
    _merge_net_pnl,
)
from almanak.framework.observability.position_events import (
    PositionEvent,
    _build_lending_attribution,
    lending_realized_net_pnl_usd,
)

# ──────────────────────────────────────────────────────────────────────────────
# Pure helper — signed realized PnL
# ──────────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("intent_type", "interest", "expected"),
    [
        # Debt-side interest paid is a cost (negative).
        ("REPAY", Decimal("0.0002370"), Decimal("-0.0002370")),
        ("DELEVERAGE", Decimal("0.5"), Decimal("-0.5")),
        # Supply-side yield received is a gain (positive).
        ("WITHDRAW", Decimal("0.0006340"), Decimal("0.0006340")),
        # Measured zero interest → measured-zero (break-even) PnL, NOT None.
        ("WITHDRAW", Decimal("0"), Decimal("0")),
        ("REPAY", Decimal("0"), Decimal("0")),
        # Case-insensitive + string/float tolerant.
        ("repay", "0.25", Decimal("-0.25")),
        ("withdraw", 0.125, Decimal("0.125")),
    ],
)
def test_signed_realized_pnl(intent_type: str, interest: Any, expected: Decimal) -> None:
    assert lending_realized_net_pnl_usd(intent_type, interest) == expected


@pytest.mark.parametrize(
    ("intent_type", "interest"),
    [
        # UNAVAILABLE interest (no matching FIFO lots) → unattributed, never 0.
        ("REPAY", None),
        ("WITHDRAW", None),
        # Open-side legs realize no interest.
        ("SUPPLY", Decimal("1")),
        ("BORROW", Decimal("1")),
        # Non-lending / unknown intent.
        ("SWAP", Decimal("1")),
        ("", Decimal("1")),
        # Non-finite is rejected (cannot score).
        ("REPAY", Decimal("NaN")),
        ("WITHDRAW", "not-a-number"),
    ],
)
def test_unattributed_returns_none(intent_type: str, interest: Any) -> None:
    assert lending_realized_net_pnl_usd(intent_type, interest) is None


def test_measured_zero_is_distinct_from_none() -> None:
    """Empty ≠ Zero: measured-zero interest scores break-even; None stays unscored."""
    assert lending_realized_net_pnl_usd("WITHDRAW", Decimal("0")) == Decimal("0")
    assert lending_realized_net_pnl_usd("WITHDRAW", None) is None


# ──────────────────────────────────────────────────────────────────────────────
# Seed-time payload: net_pnl_usd absent unless explicitly supplied
# ──────────────────────────────────────────────────────────────────────────────


def _post_state() -> dict[str, Any]:
    return {
        "collateral_value_usd": "0",
        "debt_value_usd": "0",
        "health_factor": "999999",
        "liquidation_threshold": "0.78",
        "supply_apr_bps": 95,
        "borrow_apr_bps": 385,
    }


def test_seed_payload_omits_net_pnl() -> None:
    event = PositionEvent(position_type="LENDING_DEBT", event_type="CLOSE")
    _build_lending_attribution(event, _post_state(), asset="USDT", intent_type="REPAY")
    payload = json.loads(event.attribution_json)
    assert payload["schema"] == "lending_v1"
    assert "net_pnl_usd" not in payload, "seed-time payload must NOT carry net_pnl_usd (back-filled by drain)"


def test_build_attribution_stamps_net_pnl_when_supplied() -> None:
    event = PositionEvent(position_type="LENDING_DEBT", event_type="CLOSE")
    _build_lending_attribution(
        event, _post_state(), asset="USDT", intent_type="REPAY", net_pnl_usd=Decimal("-0.0002370")
    )
    payload = json.loads(event.attribution_json)
    assert payload["net_pnl_usd"] == "-0.0002370"
    # Schema version is unchanged — additive optional field.
    assert payload["version"] == 1
    assert payload["schema"] == "lending_v1"


# ──────────────────────────────────────────────────────────────────────────────
# Win-rate scoring read path (strat_pnl._pnl_from_attribution) — no edit to that file
# ──────────────────────────────────────────────────────────────────────────────


def test_strat_pnl_scores_lending_close() -> None:
    """A lending close carrying net_pnl_usd is now scored (no longer 0/0)."""
    from almanak.framework.cli.strat_pnl import _pnl_from_attribution

    # Loss close (debt interest paid).
    loss = {"attribution_json": json.dumps({"schema": "lending_v1", "net_pnl_usd": "-0.0002370"})}
    assert _pnl_from_attribution(loss) == Decimal("-0.0002370")

    # Profit close (supply yield received).
    win = {"attribution_json": json.dumps({"schema": "lending_v1", "net_pnl_usd": "0.0006340"})}
    assert _pnl_from_attribution(win) == Decimal("0.0006340")

    # A seed-time payload without net_pnl_usd stays UNSCORED (None), not 0.
    unscored = {"attribution_json": json.dumps({"schema": "lending_v1"})}
    assert _pnl_from_attribution(unscored) is None


# ──────────────────────────────────────────────────────────────────────────────
# Module-level join helpers
# ──────────────────────────────────────────────────────────────────────────────


def test_is_lending_event() -> None:
    ev = _make_lending_event("REPAY", interest=Decimal("0.001"))
    assert _is_lending_event(ev) is True
    assert _is_lending_event(MagicMock()) is False


def test_find_position_event_for_ledger_isolates_by_ledger() -> None:
    history = [
        {"id": "pe-decrease", "ledger_entry_id": "led-decrease", "event_type": "DECREASE"},
        {"id": "pe-close", "ledger_entry_id": "led-close", "event_type": "CLOSE"},
    ]
    assert _find_position_event_for_ledger(history, "led-close")["id"] == "pe-close"
    assert _find_position_event_for_ledger(history, "led-decrease")["id"] == "pe-decrease"
    assert _find_position_event_for_ledger(history, "missing") is None


def test_merge_net_pnl_preserves_existing_keys() -> None:
    raw = json.dumps({"schema": "lending_v1", "health_factor_after": "1.4"})
    merged = json.loads(_merge_net_pnl(raw, Decimal("-0.5")))
    assert merged["net_pnl_usd"] == "-0.5"
    assert merged["health_factor_after"] == "1.4"
    assert merged["schema"] == "lending_v1"


def test_merge_net_pnl_rejects_non_object() -> None:
    assert _merge_net_pnl("[]", Decimal("1")) is None
    assert _merge_net_pnl("not json", Decimal("1")) is None
    # A non-dict, non-empty container is rejected (json.loads → TypeError, or
    # a JSON-array string decodes to a list — neither is a JSON object).
    assert _merge_net_pnl([1, 2], Decimal("1")) is None  # type: ignore[arg-type]
    assert _merge_net_pnl('["a"]', Decimal("1")) is None


def test_merge_net_pnl_accepts_dict_attribution() -> None:
    """Some backends auto-deserialize attribution_json into a dict — merging
    must stamp (not silently no-op via a TypeError-on-json.loads)."""
    raw = {"schema": "lending_v1", "health_factor_after": "1.4"}
    merged = json.loads(_merge_net_pnl(raw, Decimal("-0.5")))
    assert merged["net_pnl_usd"] == "-0.5"
    assert merged["health_factor_after"] == "1.4"
    assert merged["schema"] == "lending_v1"
    # Input dict must NOT be mutated in place (copied before merge).
    assert "net_pnl_usd" not in raw


def test_merge_net_pnl_empty_dict_stamps() -> None:
    merged = json.loads(_merge_net_pnl({}, Decimal("0.25")))
    assert merged == {"net_pnl_usd": "0.25"}


# ──────────────────────────────────────────────────────────────────────────────
# Drain back-fill — end-to-end with a mock store
# ──────────────────────────────────────────────────────────────────────────────

_DEPLOYMENT = "dep-1"
_POSITION_KEY = "lending:arbitrum:aave_v3:0xwallet:usdt"


def _make_lending_event(
    event_type: str,
    *,
    interest: Decimal | None,
    ledger_entry_id: str = "led-close",
    position_key: str = _POSITION_KEY,
) -> LendingAccountingEvent:
    identity = AccountingIdentity(
        id=str(uuid.uuid4()),
        deployment_id=_DEPLOYMENT,
        cycle_id="cycle-1",
        execution_mode="live",
        timestamp=datetime.now(UTC),
        chain="arbitrum",
        protocol="aave_v3",
        wallet_address="0xwallet",
        tx_hash="0xdead",
        ledger_entry_id=ledger_entry_id,
    )
    return LendingAccountingEvent(
        identity=identity,
        event_type=LendingEventType(event_type),
        position_key=position_key,
        market_id="",
        asset="USDT",
        collateral_value_before_usd=None,
        collateral_value_after_usd=None,
        debt_value_before_usd=None,
        debt_value_after_usd=None,
        net_equity_before_usd=None,
        net_equity_after_usd=None,
        health_factor_before=None,
        health_factor_after=None,
        liquidation_threshold=None,
        lltv=None,
        supply_apr_bps=None,
        borrow_apr_bps=None,
        principal_delta_usd=Decimal("2.4"),
        interest_delta_usd=interest,
        gas_usd=None,
        confidence=AccountingConfidence.HIGH,
    )


def _seed_close_attr(intent_type: str) -> str:
    ev = PositionEvent(position_type="LENDING_DEBT", event_type="CLOSE")
    _build_lending_attribution(ev, _post_state(), asset="USDT", intent_type=intent_type)
    return ev.attribution_json


def _backfill_store(history: list[dict[str, Any]]) -> MagicMock:
    store = MagicMock()
    store.get_position_history = AsyncMock(return_value=history)
    store.update_position_attribution = AsyncMock(return_value=True)
    return store


@pytest.mark.asyncio
async def test_backfill_stamps_signed_net_pnl_on_close() -> None:
    history = [
        {
            "id": "pe-close",
            "ledger_entry_id": "led-close",
            "position_id": _POSITION_KEY,
            "event_type": "CLOSE",
            "attribution_json": _seed_close_attr("REPAY"),
            "attribution_version": 1,
        },
    ]
    store = _backfill_store(history)
    proc = AccountingProcessor(state_manager=store, basis_store=FIFOBasisStore(), deployment_id=_DEPLOYMENT)

    event = _make_lending_event("REPAY", interest=Decimal("0.0002370"), ledger_entry_id="led-close")
    await proc._backfill_lending_position_pnl(event)

    store.update_position_attribution.assert_awaited_once()
    args = store.update_position_attribution.await_args.args
    event_id, attribution_json = args[0], args[1]
    assert event_id == "pe-close"
    payload = json.loads(attribution_json)
    # REPAY interest paid → negative realized PnL.
    assert payload["net_pnl_usd"] == "-0.0002370"
    # After-state fields preserved.
    assert payload["schema"] == "lending_v1"


@pytest.mark.asyncio
async def test_backfill_stamps_when_attribution_is_dict() -> None:
    """A backend that auto-deserializes attribution_json into a dict must
    still get the net_pnl_usd stamped — not a silent skip (Gemini review)."""
    history = [
        {
            "id": "pe-close",
            "ledger_entry_id": "led-close",
            "position_id": _POSITION_KEY,
            # attribution_json delivered as a Python dict, not a JSON string.
            "attribution_json": {"schema": "lending_v1", "health_factor_after": "1.4"},
            "attribution_version": 1,
        },
    ]
    store = _backfill_store(history)
    proc = AccountingProcessor(state_manager=store, basis_store=FIFOBasisStore(), deployment_id=_DEPLOYMENT)

    event = _make_lending_event("REPAY", interest=Decimal("0.0002370"), ledger_entry_id="led-close")
    await proc._backfill_lending_position_pnl(event)

    store.update_position_attribution.assert_awaited_once()
    payload = json.loads(store.update_position_attribution.await_args.args[1])
    assert payload["net_pnl_usd"] == "-0.0002370"
    assert payload["health_factor_after"] == "1.4"
    assert payload["schema"] == "lending_v1"


@pytest.mark.asyncio
async def test_backfill_withdraw_close_is_positive() -> None:
    history = [
        {
            "id": "pe-close",
            "ledger_entry_id": "led-close",
            "position_id": _POSITION_KEY,
            "event_type": "CLOSE",
            "attribution_json": _seed_close_attr("WITHDRAW"),
            "attribution_version": 1,
        },
    ]
    store = _backfill_store(history)
    proc = AccountingProcessor(state_manager=store, basis_store=FIFOBasisStore(), deployment_id=_DEPLOYMENT)

    event = _make_lending_event("WITHDRAW", interest=Decimal("0.0006340"), ledger_entry_id="led-close")
    await proc._backfill_lending_position_pnl(event)

    payload = json.loads(store.update_position_attribution.await_args.args[1])
    assert payload["net_pnl_usd"] == "0.0006340"


@pytest.mark.asyncio
async def test_backfill_partial_decrease_and_final_close_do_not_double_count() -> None:
    """Each action's accounting event stamps ONLY its own position event row
    (joined by ledger_entry_id) — a partial DECREASE never picks up the
    CLOSE's PnL and vice versa."""
    history = [
        {
            "id": "pe-decrease",
            "ledger_entry_id": "led-decrease",
            "position_id": _POSITION_KEY,
            "event_type": "DECREASE",
            "attribution_json": _seed_close_attr("REPAY"),
            "attribution_version": 1,
        },
        {
            "id": "pe-close",
            "ledger_entry_id": "led-close",
            "position_id": _POSITION_KEY,
            "event_type": "CLOSE",
            "attribution_json": _seed_close_attr("REPAY"),
            "attribution_version": 1,
        },
    ]
    store = _backfill_store(history)
    proc = AccountingProcessor(state_manager=store, basis_store=FIFOBasisStore(), deployment_id=_DEPLOYMENT)

    # Drain the partial DECREASE's accounting event (interest 0.0001).
    await proc._backfill_lending_position_pnl(
        _make_lending_event("REPAY", interest=Decimal("0.0001"), ledger_entry_id="led-decrease")
    )
    # Drain the final CLOSE's accounting event (interest 0.0002370).
    await proc._backfill_lending_position_pnl(
        _make_lending_event("REPAY", interest=Decimal("0.0002370"), ledger_entry_id="led-close")
    )

    assert store.update_position_attribution.await_count == 2
    by_event_id = {
        c.args[0]: json.loads(c.args[1])["net_pnl_usd"] for c in store.update_position_attribution.await_args_list
    }
    # The DECREASE row carries ONLY its own action's PnL; CLOSE carries ONLY its own.
    assert by_event_id["pe-decrease"] == "-0.0001"
    assert by_event_id["pe-close"] == "-0.0002370"


@pytest.mark.asyncio
async def test_backfill_unattributed_interest_leaves_payload_untouched() -> None:
    """interest_delta_usd is None (no FIFO lots) ⇒ no stamp (Empty ≠ Zero)."""
    history = [
        {
            "id": "pe-close",
            "ledger_entry_id": "led-close",
            "position_id": _POSITION_KEY,
            "event_type": "CLOSE",
            "attribution_json": _seed_close_attr("REPAY"),
            "attribution_version": 1,
        },
    ]
    store = _backfill_store(history)
    proc = AccountingProcessor(state_manager=store, basis_store=FIFOBasisStore(), deployment_id=_DEPLOYMENT)

    event = _make_lending_event("REPAY", interest=None, ledger_entry_id="led-close")
    await proc._backfill_lending_position_pnl(event)

    store.update_position_attribution.assert_not_awaited()


@pytest.mark.asyncio
async def test_backfill_no_matching_position_event_warns_not_silent(caplog: pytest.LogCaptureFixture) -> None:
    """When the L5 position_key finds no L3 position event (the prime cause
    is the market-scoped lending L3/L5 key divergence, VIB-4981), the
    back-fill must emit a WARNING — not a silent debug skip — so the gap is
    visible in prod log pipelines (matches run_attribution_on_close's level)."""
    store = _backfill_store(history=[])
    proc = AccountingProcessor(state_manager=store, basis_store=FIFOBasisStore(), deployment_id=_DEPLOYMENT)

    event = _make_lending_event("REPAY", interest=Decimal("0.001"), ledger_entry_id="led-close")
    with caplog.at_level("WARNING", logger="almanak.framework.accounting.processor"):
        await proc._backfill_lending_position_pnl(event)

    store.update_position_attribution.assert_not_awaited()
    warnings = [r for r in caplog.records if r.levelname == "WARNING"]
    assert len(warnings) == 1, f"expected exactly one WARNING, got {[r.message for r in warnings]}"
    msg = warnings[0].getMessage()
    assert "no Layer-3 position event" in msg
    assert _POSITION_KEY in msg  # the mismatched position_key is included
    assert "led-close" in msg  # the ledger_entry_id is included
    assert "VIB-4981" in msg


@pytest.mark.asyncio
async def test_backfill_market_scoped_key_divergence_warns(caplog: pytest.LogCaptureFixture) -> None:
    """Concrete Morpho-style reproduction of the L3/L5 divergence (VIB-4981).

    The L5 accounting event's ``position_key`` carries ``market_id``
    (``lending:...:<market_id>:<asset>``) while the L3 position event was
    seeded under the market-id-free ``lending:...:<asset>`` id, so the join
    cannot find the row. The fix must WARN (not silently skip) and never
    fabricate a stamp under the wrong key."""
    # L3 row keyed WITHOUT market_id (lending_position_id shape).
    l3_position_id = "lending:base:morpho_blue:0xwallet:usdc"
    history = [
        {
            "id": "pe-close",
            "ledger_entry_id": "led-close",
            "position_id": l3_position_id,
            "event_type": "CLOSE",
            "attribution_json": _seed_close_attr("WITHDRAW"),
            "attribution_version": 1,
        },
    ]
    store = _backfill_store(history)
    proc = AccountingProcessor(state_manager=store, basis_store=FIFOBasisStore(), deployment_id=_DEPLOYMENT)

    # L5 event keyed WITH market_id (_derive_position_key shape) — diverges.
    l5_position_key = "lending:base:morpho_blue:0xwallet:0xmarketid:usdc"
    event = _make_lending_event(
        "WITHDRAW",
        interest=Decimal("0.0006340"),
        ledger_entry_id="led-close",
        position_key=l5_position_key,
    )
    # get_position_history is keyed on the L5 position_key, which has no L3 row.
    store.get_position_history = AsyncMock(return_value=[])

    with caplog.at_level("WARNING", logger="almanak.framework.accounting.processor"):
        await proc._backfill_lending_position_pnl(event)

    store.update_position_attribution.assert_not_awaited()
    assert any("VIB-4981" in r.getMessage() for r in caplog.records if r.levelname == "WARNING")


@pytest.mark.asyncio
async def test_drain_one_repay_wires_backfill_end_to_end() -> None:
    """Full drain_one path: a REPAY that FIFO-matches a prior BORROW lot
    computes interest and back-fills the CLOSE position event's net_pnl_usd."""
    from unittest.mock import patch

    led_borrow = "led-borrow"
    led_repay = "led-close"
    position_key = _POSITION_KEY

    basis = FIFOBasisStore()
    # Pre-seed a BORROW lot at $1.00 basis so the REPAY matches and any excess
    # is interest. We record the borrow lot directly to keep the test focused.
    basis.record_borrow(
        deployment_id=_DEPLOYMENT,
        position_key=position_key,
        token="USDT",
        principal_amount=Decimal("1.0"),
        principal_usd=Decimal("1.0"),
        timestamp=datetime.now(UTC),
        lot_id="lot-1",
        source_ledger_entry_id=led_borrow,
    )

    close_attr = _seed_close_attr("REPAY")
    history = [
        {
            "id": "pe-close",
            "ledger_entry_id": led_repay,
            "position_id": position_key,
            "event_type": "CLOSE",
            "attribution_json": close_attr,
            "attribution_version": 1,
        }
    ]

    outbox_row = {
        "id": str(uuid.uuid4()),
        "ledger_entry_id": led_repay,
        "deployment_id": _DEPLOYMENT,
        "cycle_id": "cycle-1",
        "intent_type": "REPAY",
        "wallet_address": "0xwallet",
        "position_key": position_key,
        "market_id": "",
        "status": "pending",
        "attempts": 0,
    }
    # Repay 1.0 principal (matches the lot exactly → interest 0; a measured
    # break-even close that still SCORES, distinct from unattributed None).
    ledger_row = {
        "id": led_repay,
        "deployment_id": _DEPLOYMENT,
        "cycle_id": "cycle-1",
        "execution_mode": "live",
        "timestamp": datetime.now(UTC).isoformat(),
        "intent_type": "REPAY",
        "token_in": "USDT",
        "chain": "arbitrum",
        "protocol": "aave_v3",
        "tx_hash": "0xrepay",
        "gas_usd": "0.0",
        "extracted_data_json": json.dumps({"repay_amount": 1_000_000}),  # 1.0 USDT (6 dec)
        "price_inputs_json": json.dumps({"USDT": "1.0"}),
        "pre_state_json": "",
        "post_state_json": "",
    }

    store = MagicMock()
    store.get_outbox_by_ledger_id = MagicMock(return_value=outbox_row)
    store.update_outbox_entry = MagicMock()
    store.has_accounting_events_for_ledger = MagicMock(return_value=False)
    store.get_ledger_entry_by_id = MagicMock(return_value=ledger_row)
    store.save_accounting_event = AsyncMock(return_value=True)
    store.get_position_history = AsyncMock(return_value=history)
    store.update_position_attribution = AsyncMock(return_value=True)

    proc = AccountingProcessor(state_manager=store, basis_store=basis, deployment_id=_DEPLOYMENT)

    mock_token_info = MagicMock()
    mock_token_info.decimals = 6
    mock_resolver = MagicMock(resolve=MagicMock(return_value=mock_token_info))
    with patch("almanak.framework.data.tokens.resolver.get_token_resolver", return_value=mock_resolver):
        result = await proc.drain_one(led_repay)

    assert result is True
    store.save_accounting_event.assert_awaited_once()
    store.update_position_attribution.assert_awaited_once()
    payload = json.loads(store.update_position_attribution.await_args.args[1])
    # Exact principal match ⇒ interest 0 ⇒ measured-zero (scored) net PnL.
    # (A measured zero is a SCORED break-even close, not the unattributed None.)
    assert "net_pnl_usd" in payload
    assert Decimal(payload["net_pnl_usd"]) == Decimal("0")


@pytest.mark.asyncio
async def test_backfill_is_best_effort_on_store_error() -> None:
    """A store read failure degrades attribution but never raises."""
    store = MagicMock()
    store.get_position_history = AsyncMock(side_effect=RuntimeError("db down"))
    store.update_position_attribution = AsyncMock(return_value=True)
    proc = AccountingProcessor(state_manager=store, basis_store=FIFOBasisStore(), deployment_id=_DEPLOYMENT)

    # Must not raise.
    await proc._backfill_lending_position_pnl(
        _make_lending_event("REPAY", interest=Decimal("0.001"), ledger_entry_id="led-close")
    )
    store.update_position_attribution.assert_not_awaited()
