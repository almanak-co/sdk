"""Unit tests for Hyperliquid fill-economics accounting (VIB-5595).

Three layers, mirroring the shape of the accounting seam:

1. Gateway reader — the connector's ``fetch_user_fills`` / ``fetch_user_funding``
   decode fee / closedPnl / funding correctly from a mocked HL Info-API response
   (and honour Empty ≠ Zero on absent fields).
2. Correlation — ``build_perp_data_from_fills`` matches fills to the intent by
   the deterministic cloid and aggregates fee / realized-PnL / funding.
3. End-to-end wiring — a serialized ``PerpData`` (as the runner hook stamps it)
   flows through ``handle_perp`` into a ``PerpAccountingEvent`` carrying measured
   realized-PnL + funding, and the AccountingProcessor writes it via
   AccountingWriter.
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.connectors.hyperliquid.compiler import HyperliquidCompiler
from almanak.connectors.hyperliquid.gateway.provider import (
    HyperliquidGatewayConnector,
    _parse_user_fill,
    _parse_user_funding,
)

# A deterministic cloid for a known intent id — the exact correlation key the
# CoreWriter order carries (compiler._cloid → sdk encode → receipt decode).
_INTENT_ID = "intent-abc-123"
_CLOID_INT = HyperliquidCompiler._cloid(_INTENT_ID)
_CLOID_HEX = hex(_CLOID_INT)


# ──────────────────────────────────────────────────────────────────────────────
# Layer 1 — gateway reader decodes fee / closedPnl / funding
# ──────────────────────────────────────────────────────────────────────────────


def _patch_session_post(svc: Any, *, json_payload: object) -> None:
    """Patch ``servicer._get_http_session`` so one ``session.post(...)`` is mocked."""
    mock_response = MagicMock()
    mock_response.status = 200
    mock_response.json = AsyncMock(return_value=json_payload)
    post_cm = MagicMock()
    post_cm.__aenter__ = AsyncMock(return_value=mock_response)
    post_cm.__aexit__ = AsyncMock(return_value=None)
    mock_session = MagicMock()
    mock_session.post = MagicMock(return_value=post_cm)
    svc._get_http_session = AsyncMock(return_value=mock_session)


def test_parse_user_fill_decodes_economics() -> None:
    """A userFills entry maps fee / closedPnl / px / sz / cloid faithfully."""
    fill = _parse_user_fill(
        {
            "coin": "BTC",
            "px": "60000.5",
            "sz": "0.001",
            "dir": "Close Long",
            "fee": "0.027",
            "closedPnl": "1.23",
            "oid": "999",
            "cloid": _CLOID_HEX,
            "time": 1720000000000,
            "crossed": True,
            "feeToken": "USDC",
        }
    )
    assert fill.coin == "BTC"
    assert fill.fee == "0.027"
    assert fill.closed_pnl == "1.23"
    assert fill.px == "60000.5"
    assert fill.cloid == _CLOID_HEX
    assert fill.time_ms == 1720000000000
    assert fill.crossed is True


def test_parse_user_fill_empty_not_zero() -> None:
    """A fill missing fee / closedPnl leaves them empty (unmeasured), never '0'."""
    fill = _parse_user_fill({"coin": "ETH", "px": "3000", "sz": "1"})
    assert fill.fee == ""  # unmeasured, NOT "0"
    assert fill.closed_pnl == ""
    assert fill.coin == "ETH"


def test_parse_user_funding_decodes_delta() -> None:
    """A userFunding row maps the signed usdc delta from the nested ``delta``."""
    delta = _parse_user_funding(
        {"time": 1720000001000, "delta": {"coin": "BTC", "usdc": "-0.0001", "fundingRate": "0.0000125"}}
    )
    assert delta.coin == "BTC"
    assert delta.usdc == "-0.0001"
    assert delta.funding_rate == "0.0000125"
    assert delta.time_ms == 1720000001000


@pytest.mark.asyncio
async def test_fetch_user_fills_reads_and_filters() -> None:
    """fetch_user_fills posts userFills, decodes rows, filters by coin + start_ts."""
    conn = HyperliquidGatewayConnector()
    servicer = MagicMock()
    _patch_session_post(
        servicer,
        json_payload=[
            {"coin": "BTC", "px": "60000", "sz": "0.001", "fee": "0.027", "closedPnl": "1.5",
             "cloid": _CLOID_HEX, "time": 1720000000000},
            {"coin": "ETH", "px": "3000", "sz": "1", "fee": "0.1", "closedPnl": "0",
             "cloid": "0xdead", "time": 1720000000000},  # filtered out by coin
            {"coin": "BTC", "px": "59000", "sz": "0.001", "fee": "0.02", "closedPnl": "0",
             "cloid": _CLOID_HEX, "time": 1710000000000},  # filtered out by start_ts
        ],
    )
    result = await conn.fetch_user_fills(servicer, wallet_address="0xabc", coin="BTC", start_ts=1715000000000)
    assert result.ok is True
    assert len(result.fills) == 1
    assert result.fills[0].coin == "BTC"
    assert result.fills[0].fee == "0.027"


@pytest.mark.asyncio
async def test_fetch_user_fills_non_200_is_unmeasured() -> None:
    """A non-200 Info-API status yields ok=False (UNMEASURED, no fabricated fills)."""
    conn = HyperliquidGatewayConnector()
    servicer = MagicMock()
    mock_response = MagicMock()
    mock_response.status = 503
    mock_response.text = AsyncMock(return_value="upstream down")
    post_cm = MagicMock()
    post_cm.__aenter__ = AsyncMock(return_value=mock_response)
    post_cm.__aexit__ = AsyncMock(return_value=None)
    mock_session = MagicMock()
    mock_session.post = MagicMock(return_value=post_cm)
    servicer._get_http_session = AsyncMock(return_value=mock_session)

    result = await conn.fetch_user_fills(servicer, wallet_address="0xabc")
    assert result.ok is False
    assert result.fills == []


@pytest.mark.asyncio
async def test_fetch_user_funding_reads_deltas() -> None:
    """fetch_user_funding posts userFunding and decodes signed usdc deltas."""
    conn = HyperliquidGatewayConnector()
    servicer = MagicMock()
    _patch_session_post(
        servicer,
        json_payload=[
            {"time": 1720000001000, "delta": {"coin": "BTC", "usdc": "-0.0001", "fundingRate": "0.00001"}},
            {"time": 1720000002000, "delta": {"coin": "BTC", "usdc": "0.0002", "fundingRate": "0.00002"}},
        ],
    )
    result = await conn.fetch_user_funding(servicer, wallet_address="0xabc", coin="BTC")
    assert result.ok is True
    assert len(result.deltas) == 2
    assert result.deltas[0].usdc == "-0.0001"


# ──────────────────────────────────────────────────────────────────────────────
# Layer 2 — cloid correlation → PerpData
# ──────────────────────────────────────────────────────────────────────────────


def _fill(cloid: str, *, fee: str = "", closed_pnl: str = "", px: str = "", sz: str = "", coin: str = "BTC") -> Any:
    from almanak.gateway.services.perp_fill_service import PerpFillData

    return PerpFillData(coin=coin, px=px, sz=sz, fee=fee, closed_pnl=closed_pnl, cloid=cloid)


def test_aggregate_matches_by_cloid_and_sums() -> None:
    from almanak.connectors.hyperliquid.fill_accounting import _aggregate_matching_fills

    fills = [
        _fill(_CLOID_HEX, fee="0.02", closed_pnl="1.0", px="60000", sz="0.001"),
        _fill(_CLOID_HEX, fee="0.01", closed_pnl="0.5", px="60100", sz="0.001"),
        _fill("0xother", fee="9.99", closed_pnl="9.99", px="1", sz="1"),  # not ours
    ]
    agg = _aggregate_matching_fills(fills, _CLOID_HEX)
    assert agg.matched_fill_count == 2
    assert agg.fee_usd == Decimal("0.03")
    assert agg.realized_pnl_usd == Decimal("1.5")
    # size-weighted avg of 60000 and 60100 over equal sizes = 60050
    assert agg.avg_price == Decimal("60050")


def test_aggregate_empty_not_zero() -> None:
    """No matched fill reporting a fee → fee_usd stays None (unmeasured)."""
    from almanak.connectors.hyperliquid.fill_accounting import _aggregate_matching_fills

    agg = _aggregate_matching_fills([_fill(_CLOID_HEX, px="60000", sz="0.001")], _CLOID_HEX)
    assert agg.matched_fill_count == 1
    assert agg.fee_usd is None
    assert agg.realized_pnl_usd is None


def _make_hl_result(*, reduce_only: bool) -> Any:
    """Build an ExecutionResult carrying a decodable CoreWriter RawAction receipt."""
    from eth_abi import encode as abi_encode

    from almanak.connectors.hyperliquid.addresses import CORE_WRITER_ADDRESS, RAW_ACTION_EVENT_TOPIC
    from almanak.connectors.hyperliquid.markets import resolve_market
    from almanak.connectors.hyperliquid.sdk import LimitOrderAction, TIF_IOC, encode_limit_order_action

    market = resolve_market("BTC")
    order = LimitOrderAction(
        asset=market.asset_index,
        is_buy=not reduce_only,
        limit_px=6_000_000_000_000,
        sz=100000,
        reduce_only=reduce_only,
        tif=TIF_IOC,
        cloid=_CLOID_INT,
    )
    blob = encode_limit_order_action(order)
    # A RawAction log's non-indexed ``data`` is ABI-encoded ``(bytes)`` wrapping
    # the action blob — mirror the on-chain shape the receipt parser unwraps.
    log_data = abi_encode(["bytes"], [blob])
    log = {"address": CORE_WRITER_ADDRESS, "topics": [RAW_ACTION_EVENT_TOPIC], "data": "0x" + log_data.hex()}
    receipt = MagicMock()
    receipt.to_dict = MagicMock(return_value={"logs": [log]})
    tr = MagicMock()
    tr.receipt = receipt
    tr.tx_hash = "0xfeed"
    result = MagicMock()
    result.transaction_results = [tr]
    result.extracted_data = {}
    result.protocol_fees = None
    return result


def _mock_gateway_with_fills(fills: list[Any], funding: list[Any]) -> Any:
    gw = MagicMock()
    fills_resp = MagicMock()
    fills_resp.success = True
    fills_resp.fills = fills
    fund_resp = MagicMock()
    fund_resp.success = True
    fund_resp.deltas = funding
    gw.perp_fill.GetUserFills = MagicMock(return_value=fills_resp)
    gw.perp_fill.GetUserFunding = MagicMock(return_value=fund_resp)
    return gw


def test_build_perp_data_close_measures_pnl_and_funding() -> None:
    from almanak.connectors.hyperliquid.fill_accounting import build_perp_data_from_fills
    from almanak.gateway.services.perp_fill_service import PerpFundingData

    result = _make_hl_result(reduce_only=True)
    gw = _mock_gateway_with_fills(
        fills=[_fill(_CLOID_HEX, fee="0.027", closed_pnl="1.23", px="60000", sz="0.001")],
        funding=[PerpFundingData(coin="BTC", usdc="-0.0001", time_ms=1)],
    )
    bundle = build_perp_data_from_fills(result, gateway_client=gw, wallet_address="0xabc", is_open=False)
    assert bundle is not None
    assert bundle.perp.realized_pnl == Decimal("1.23")
    assert bundle.perp.exit_price == Decimal("60000")
    assert bundle.perp.funding_fee_usd == Decimal("-0.0001")
    assert bundle.fee_usd == Decimal("0.027")


def test_build_perp_data_no_match_returns_none() -> None:
    """No fill matching our cloid yet (async settlement) → None (honest unmeasured)."""
    from almanak.connectors.hyperliquid.fill_accounting import build_perp_data_from_fills

    result = _make_hl_result(reduce_only=True)
    gw = _mock_gateway_with_fills(fills=[_fill("0xnotours", fee="1", closed_pnl="1")], funding=[])
    assert build_perp_data_from_fills(result, gateway_client=gw, wallet_address="0xabc", is_open=False) is None


def test_build_perp_data_no_wallet_returns_none() -> None:
    from almanak.connectors.hyperliquid.fill_accounting import build_perp_data_from_fills

    result = _make_hl_result(reduce_only=True)
    gw = _mock_gateway_with_fills(fills=[], funding=[])
    assert build_perp_data_from_fills(result, gateway_client=gw, wallet_address="", is_open=False) is None


# ──────────────────────────────────────────────────────────────────────────────
# Runner hook stamps perp_data + protocol_fees
# ──────────────────────────────────────────────────────────────────────────────


def test_runner_hook_stamps_perp_data_and_fee() -> None:
    from almanak.connectors.hyperliquid.runner_hooks import HyperliquidRunnerHookConnector
    from almanak.framework.execution.extracted_data import PerpData
    from almanak.gateway.services.perp_fill_service import PerpFundingData

    result = _make_hl_result(reduce_only=True)
    gw = _mock_gateway_with_fills(
        fills=[_fill(_CLOID_HEX, fee="0.027", closed_pnl="1.23", px="60000", sz="0.001")],
        funding=[PerpFundingData(coin="BTC", usdc="-0.0001", time_ms=1)],
    )
    hook = HyperliquidRunnerHookConnector()
    hook.enrich_result(result, gateway_client=gw, chain="hyperevm", wallet_address="0xabc")

    perp = result.extracted_data.get("perp_data")
    assert isinstance(perp, PerpData)
    assert perp.realized_pnl == Decimal("1.23")
    assert perp.funding_fee_usd == Decimal("-0.0001")
    assert result.protocol_fees is not None
    assert result.protocol_fees.perp_fee_usd == Decimal("0.027")


def test_runner_hook_noop_off_chain() -> None:
    """The hook is inert for a non-hyperevm chain (no gateway calls)."""
    from almanak.connectors.hyperliquid.runner_hooks import HyperliquidRunnerHookConnector

    result = _make_hl_result(reduce_only=True)
    gw = MagicMock()
    HyperliquidRunnerHookConnector().enrich_result(result, gateway_client=gw, chain="arbitrum", wallet_address="0xabc")
    assert result.extracted_data.get("perp_data") is None


# ──────────────────────────────────────────────────────────────────────────────
# Layer 3 — end-to-end: serialized PerpData → PerpAccountingEvent via handle_perp
# ──────────────────────────────────────────────────────────────────────────────


def _serialize_perp_data(perp: Any) -> str:
    from almanak.framework.observability.ledger import serialize_extracted_data

    return serialize_extracted_data({"perp_data": perp})


def test_handle_perp_reads_measured_fill_economics() -> None:
    """A serialized PerpData (as the runner hook stamps) → measured PerpAccountingEvent."""
    from almanak.framework.accounting.category_handlers.perp_handler import handle_perp
    from almanak.framework.accounting.models import PerpEventType
    from almanak.framework.execution.extracted_data import PerpData

    perp = PerpData(
        position_id=_CLOID_HEX,
        exit_price=Decimal("60000"),
        realized_pnl=Decimal("1.23"),
        funding_fee_usd=Decimal("-0.0001"),
    )
    extracted_json = _serialize_perp_data(perp)

    led_id = str(uuid.uuid4())
    outbox_row = {
        "id": str(uuid.uuid4()),
        "deployment_id": "dep-1",
        "cycle_id": "cycle-1",
        "wallet_address": "0xabc",
        "position_key": "perp:hyperliquid:hyperevm:0xabc:btc",
        "market_id": "BTC",
    }
    ledger_row = {
        "id": led_id,
        "deployment_id": "dep-1",
        "cycle_id": "cycle-1",
        "execution_mode": "live",
        "timestamp": datetime.now(UTC).isoformat(),
        "intent_type": "PERP_CLOSE",
        "token_in": "USDC",
        "amount_in": "0",
        "token_out": "",
        "amount_out": "",
        "tx_hash": "0xfeed",
        "chain": "hyperevm",
        "protocol": "hyperliquid",
        "success": True,
        "extracted_data_json": extracted_json,
    }

    event = handle_perp(outbox_row, ledger_row)
    assert event is not None
    assert event.event_type == PerpEventType.PERP_CLOSE.value
    assert event.realized_pnl_usd == Decimal("1.23")
    assert event.funding_paid_usd == Decimal("-0.0001")


@pytest.mark.asyncio
async def test_processor_writes_perp_event_via_writer() -> None:
    """drain_one on a HL PERP_CLOSE row with measured fill economics writes the event."""
    from almanak.framework.accounting.basis import FIFOBasisStore
    from almanak.framework.accounting.perp_accounting import PerpAccountingEvent
    from almanak.framework.accounting.processor import AccountingProcessor
    from almanak.framework.execution.extracted_data import PerpData

    perp = PerpData(position_id=_CLOID_HEX, exit_price=Decimal("60000"), realized_pnl=Decimal("1.23"),
                    funding_fee_usd=Decimal("-0.0001"))
    extracted_json = _serialize_perp_data(perp)
    led_id = str(uuid.uuid4())

    outbox_row = {
        "id": str(uuid.uuid4()), "ledger_entry_id": led_id, "deployment_id": "dep-1", "cycle_id": "cycle-1",
        "intent_type": "PERP_CLOSE", "wallet_address": "0xabc",
        "position_key": "perp:hyperliquid:hyperevm:0xabc:btc", "market_id": "BTC",
        "status": "pending", "attempts": 0, "error": "",
    }
    ledger_row = {
        "id": led_id, "deployment_id": "dep-1", "cycle_id": "cycle-1", "execution_mode": "live",
        "timestamp": datetime.now(UTC).isoformat(), "intent_type": "PERP_CLOSE", "token_in": "USDC",
        "amount_in": "0", "token_out": "", "amount_out": "", "tx_hash": "0xfeed", "chain": "hyperevm",
        "protocol": "hyperliquid", "success": True, "extracted_data_json": extracted_json,
        "price_inputs_json": "", "pre_state_json": "", "post_state_json": "",
    }

    store = MagicMock()
    store.get_outbox_by_ledger_id = MagicMock(return_value=outbox_row)
    store.update_outbox_entry = MagicMock()
    store.has_accounting_events_for_ledger = MagicMock(return_value=False)
    store.get_ledger_entry_by_id = MagicMock(return_value=ledger_row)
    store.save_accounting_event = AsyncMock(return_value=True)

    proc = AccountingProcessor(state_manager=store, basis_store=FIFOBasisStore(), deployment_id="dep-1")
    ok = await proc.drain_one(led_id)

    assert ok is True
    store.save_accounting_event.assert_awaited_once()
    written = store.save_accounting_event.call_args[0][0]
    assert isinstance(written, PerpAccountingEvent)
    assert written.realized_pnl_usd == Decimal("1.23")
    assert written.funding_paid_usd == Decimal("-0.0001")


# ──────────────────────────────────────────────────────────────────────────────
# resolve_fill_status — fills → orderStatus reject-detection (VIB-5616)
# ──────────────────────────────────────────────────────────────────────────────


def _order_status_proto(raw: Any) -> Any:
    """Build the real ``OrderStatusResponse`` the gateway would return for ``raw``.

    Mirrors the live path exactly: the connector's pure parser turns the raw
    payload into a neutral ``OrderStatusData`` (as the provider now does), then
    the gateway maps that to proto — so the pump wiring is exercised end-to-end
    without a real socket.
    """
    from almanak.connectors.hyperliquid.fill_reconciliation import parse_order_status_response
    from almanak.gateway.services.perp_fill_service import OrderStatusData, _order_status_to_proto

    outcome = parse_order_status_response(raw)
    return _order_status_to_proto(
        OrderStatusData(
            status=str(outcome.status),
            filled_size="" if outcome.filled_size is None else str(outcome.filled_size),
            avg_fill_price="" if outcome.avg_fill_price is None else str(outcome.avg_fill_price),
            detail=outcome.detail,
        )
    )


def _mock_gateway(fills: list[Any], *, order_status_raw: Any = None, order_status_fault: bool = False) -> Any:
    """Gateway mock supporting GetUserFills + GetOrderStatus for the pump."""
    gw = MagicMock()
    fills_resp = MagicMock()
    fills_resp.success = True
    fills_resp.fills = fills
    gw.perp_fill.GetUserFills = MagicMock(return_value=fills_resp)

    if order_status_fault:
        gw.perp_fill.GetOrderStatus = MagicMock(side_effect=RuntimeError("gw down"))
    elif order_status_raw is not None:
        gw.perp_fill.GetOrderStatus = MagicMock(return_value=_order_status_proto(order_status_raw))
    else:
        # No orderStatus stubbed → a success=False envelope (unmeasured).
        unmeasured = MagicMock()
        unmeasured.success = False
        unmeasured.status = ""
        gw.perp_fill.GetOrderStatus = MagicMock(return_value=unmeasured)
    return gw


def _handle() -> Any:
    from almanak.connectors.hyperliquid.runner_hooks import PendingFillHandle

    return PendingFillHandle(protocol="hyperliquid", intent_type="PERP_OPEN", cloid_hex=_CLOID_HEX, coin="BTC")


def _resolve(gw: Any) -> Any:
    from almanak.connectors.hyperliquid.runner_hooks import HyperliquidRunnerHookConnector

    return HyperliquidRunnerHookConnector().resolve_fill_status(
        gateway_client=gw, wallet_address="0xabc", handle=_handle()
    )


def test_resolve_fill_beats_reject() -> None:
    """FILLED precedence: a matching fill wins even if orderStatus would say rejected."""
    from almanak.connectors.hyperliquid.fill_reconciliation import FillStatus

    gw = _mock_gateway(
        fills=[_fill(_CLOID_HEX, fee="0.02", closed_pnl="0", px="60000", sz="0.001")],
        order_status_raw={"status": "order", "order": {"status": "rejected", "order": {}}},
    )
    verdict = _resolve(gw)
    assert verdict is not None
    assert str(verdict.status) == str(FillStatus.FILLED)
    assert verdict.terminal is True
    # FILLED short-circuits BEFORE the orderStatus query — reject is never consulted.
    gw.perp_fill.GetOrderStatus.assert_not_called()


def test_resolve_reject_is_terminal() -> None:
    """No fill matched + orderStatus REJECTED → terminal REJECTED (clears PENDING)."""
    from almanak.connectors.hyperliquid.fill_reconciliation import FillStatus

    gw = _mock_gateway(
        fills=[_fill("0xnotours", fee="1", closed_pnl="1")],  # no match for our cloid
        order_status_raw={"status": "order", "order": {"status": "rejected", "order": {}}},
    )
    verdict = _resolve(gw)
    assert verdict is not None
    assert str(verdict.status) == str(FillStatus.REJECTED)
    assert verdict.terminal is True
    gw.perp_fill.GetOrderStatus.assert_called_once()


def test_resolve_unmeasured_order_status_stays_pending() -> None:
    """No fill + orderStatus unmeasured (success=False) → non-terminal (stays PENDING)."""
    from almanak.connectors.hyperliquid.fill_reconciliation import FillStatus

    gw = _mock_gateway(fills=[_fill("0xnotours")], order_status_fault=True)
    verdict = _resolve(gw)
    assert verdict is not None
    assert str(verdict.status) == str(FillStatus.UNMEASURED)
    assert verdict.terminal is False


def test_resolve_resting_order_status_stays_pending() -> None:
    """No fill + orderStatus RESTING → non-terminal (not a confirmed reject)."""
    from almanak.connectors.hyperliquid.fill_reconciliation import FillStatus

    gw = _mock_gateway(
        fills=[_fill("0xnotours")],
        order_status_raw={"status": "order", "order": {"status": "open", "order": {}}},
    )
    verdict = _resolve(gw)
    assert verdict is not None
    assert str(verdict.status) == str(FillStatus.UNMEASURED)
    assert verdict.terminal is False


def test_resolve_late_fill_via_order_status_is_terminal() -> None:
    """No fill in the book yet, but orderStatus shows FILLED → terminal FILLED."""
    from almanak.connectors.hyperliquid.fill_reconciliation import FillStatus

    gw = _mock_gateway(
        fills=[_fill("0xnotours")],
        order_status_raw={
            "status": "order",
            "order": {"status": "filled", "order": {"filledSz": "0.001", "avgPx": "60000"}},
        },
    )
    verdict = _resolve(gw)
    assert verdict is not None
    assert str(verdict.status) == str(FillStatus.FILLED)
    assert verdict.terminal is True


def test_resolve_fills_read_unavailable_stays_pending() -> None:
    """A failed userFills read → UNMEASURED without ever consulting orderStatus."""
    from almanak.connectors.hyperliquid.fill_reconciliation import FillStatus

    gw = MagicMock()
    bad = MagicMock()
    bad.success = False
    gw.perp_fill.GetUserFills = MagicMock(return_value=bad)
    gw.perp_fill.GetOrderStatus = MagicMock()
    verdict = _resolve(gw)
    assert verdict is not None
    assert str(verdict.status) == str(FillStatus.UNMEASURED)
    assert verdict.terminal is False
    gw.perp_fill.GetOrderStatus.assert_not_called()
