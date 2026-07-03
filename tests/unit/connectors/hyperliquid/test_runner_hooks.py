"""Unit tests for the Hyperliquid strategy-runner hooks (VIB-5614 / VIB-5615).

These pin the guard / fail-open branches of ``HyperliquidRunnerHookConnector``
that the happy-path fill-accounting tests don't reach — the seam that keeps the
hook honest (Empty ≠ Zero) and inert on non-HL results:

1. ``enrich_result`` no-ops — non-dict ``extracted_data``, an already-enriched
   result (idempotent), and a ``build_perp_data_from_fills`` that returns None
   (no settled fill) all leave the result untouched (no fabricated economics).
2. ``_maybe_stamp_fee`` — an unmeasured fee (None) is not stamped; an existing
   ``protocol_fees`` is never overwritten; a fee that cannot build a
   ``ProtocolFees`` is swallowed (debug-logged), never raised.
3. ``_is_open_result`` — falls back to "open" when the result carries no
   decodable order (conservative: opens don't book realized PnL / funding).
4. ``extract_pending_fill_handle`` — an OPEN yields a handle with the venue
   correlation keys; a reduce-only CLOSE and a non-HL result yield None.
5. ``resolve_fill_status`` — a foreign handle → None; a missing wallet or an
   unavailable ``userFills`` read → NON-terminal UNMEASURED (stays PENDING),
   never a fabricated terminal verdict.

The hook talks to the gateway only through the injected ``gateway_client``, so
these use a plain ``MagicMock`` gateway — no chain, no sockets.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

from almanak.connectors.hyperliquid.runner_hooks import (
    HyperliquidRunnerHookConnector,
    PendingFillHandle,
)

# Reuse the receipt-building + gateway-mock helpers from the fill-accounting
# suite so a HL result here is byte-identical to the real CoreWriter shape.
from tests.unit.connectors.hyperliquid.test_fill_accounting import (
    _CLOID_HEX,
    _CLOID_INT,
    _make_hl_result,
    _mock_gateway_with_fills,
)


def _hook() -> HyperliquidRunnerHookConnector:
    return HyperliquidRunnerHookConnector()


# ──────────────────────────────────────────────────────────────────────────────
# enrich_result — no-op / idempotent guards
# ──────────────────────────────────────────────────────────────────────────────


def test_enrich_result_noop_when_extracted_not_dict() -> None:
    """A result whose ``extracted_data`` is not a dict is left untouched."""
    result = MagicMock()
    result.extracted_data = None  # not a dict → early return
    _hook().enrich_result(result, gateway_client=MagicMock(), chain="hyperevm", wallet_address="0xabc")
    assert result.extracted_data is None


def test_enrich_result_idempotent_when_perp_data_present() -> None:
    """An already-enriched result (perp_data set) is never overwritten."""
    sentinel = object()
    result = MagicMock()
    result.extracted_data = {"perp_data": sentinel}
    gw = MagicMock()
    _hook().enrich_result(result, gateway_client=gw, chain="hyperevm", wallet_address="0xabc")
    assert result.extracted_data["perp_data"] is sentinel
    # No gateway read is attempted when already enriched.
    gw.perp_fill.GetUserFills.assert_not_called()


def test_enrich_result_noop_when_no_settled_fill() -> None:
    """build_perp_data_from_fills → None (no matching fill) leaves perp_data unset."""
    result = _make_hl_result(reduce_only=True)
    # Fills book has no entry for our cloid → bundle is None.
    gw = _mock_gateway_with_fills(fills=[], funding=[])
    _hook().enrich_result(result, gateway_client=gw, chain="hyperevm", wallet_address="0xabc")
    assert result.extracted_data.get("perp_data") is None


# ──────────────────────────────────────────────────────────────────────────────
# _maybe_stamp_fee — Empty≠Zero + no-overwrite + fail-soft
# ──────────────────────────────────────────────────────────────────────────────


def test_maybe_stamp_fee_noop_on_unmeasured_fee() -> None:
    """A None (unmeasured) fee is not stamped — Empty≠Zero."""
    result = MagicMock()
    result.protocol_fees = None
    HyperliquidRunnerHookConnector._maybe_stamp_fee(result, None)
    assert result.protocol_fees is None


def test_maybe_stamp_fee_does_not_overwrite_existing() -> None:
    """An existing protocol_fees is never overwritten by the hook."""
    existing = object()
    result = MagicMock()
    result.protocol_fees = existing
    HyperliquidRunnerHookConnector._maybe_stamp_fee(result, Decimal("0.5"))
    assert result.protocol_fees is existing


def test_maybe_stamp_fee_swallows_bad_fee_value() -> None:
    """A fee that can't build a ProtocolFees is debug-logged, never raised."""
    result = MagicMock()
    result.protocol_fees = None
    result.extracted_data = {}
    # A non-numeric fee makes ProtocolFees(total_usd=...) raise ValueError/TypeError
    # inside the guarded try — the hook must swallow it (fail-open), not crash.
    HyperliquidRunnerHookConnector._maybe_stamp_fee(result, "not-a-decimal")
    assert result.protocol_fees is None
    assert "protocol_fees" not in result.extracted_data


def test_maybe_stamp_fee_swallows_frozen_result_setter() -> None:
    """A result whose ``protocol_fees`` cannot be set (frozen/immutable) is
    handled fail-open — the hook logs and still mirrors into extracted_data."""

    class _FrozenResult:
        """protocol_fees reads as None but rejects assignment (odd frozen path)."""

        extracted_data: dict[str, Any] = {}

        @property
        def protocol_fees(self) -> Any:
            return None

        @protocol_fees.setter
        def protocol_fees(self, value: Any) -> None:
            raise AttributeError("frozen result")

    result = _FrozenResult()
    result.extracted_data = {}
    # Must not raise despite the setter raising — fail-open by contract.
    HyperliquidRunnerHookConnector._maybe_stamp_fee(result, Decimal("0.027"))
    # The top-level slot could not take the value, but the ledger-serialized
    # mirror in extracted_data still carries the measured fee.
    assert "protocol_fees" in result.extracted_data


def test_maybe_stamp_fee_stamps_measured_fee() -> None:
    """A measured fee is attached as ProtocolFees on both the slot and extracted_data."""
    from almanak.framework.execution.extracted_data import ProtocolFees

    result = MagicMock()
    result.protocol_fees = None
    result.extracted_data = {}
    HyperliquidRunnerHookConnector._maybe_stamp_fee(result, Decimal("0.027"))
    assert isinstance(result.protocol_fees, ProtocolFees)
    assert result.protocol_fees.perp_fee_usd == Decimal("0.027")
    assert result.extracted_data["protocol_fees"] is result.protocol_fees


# ──────────────────────────────────────────────────────────────────────────────
# _is_open_result — fallback when the order is not decodable
# ──────────────────────────────────────────────────────────────────────────────


def test_is_open_result_defaults_to_open_when_undecodable() -> None:
    """No decodable order → treated as an OPEN (conservative default)."""
    result = MagicMock()
    result.transaction_results = []  # nothing to decode
    assert HyperliquidRunnerHookConnector._is_open_result(result, {}) is True


def test_is_open_result_reads_reduce_only_from_decoded_order() -> None:
    """A decodable reduce-only order → NOT open (a close)."""
    close = _make_hl_result(reduce_only=True)
    assert HyperliquidRunnerHookConnector._is_open_result(close, {}) is False
    open_ = _make_hl_result(reduce_only=False)
    assert HyperliquidRunnerHookConnector._is_open_result(open_, {}) is True


# ──────────────────────────────────────────────────────────────────────────────
# extract_pending_fill_handle — only OPENs enter PENDING
# ──────────────────────────────────────────────────────────────────────────────


def test_extract_handle_for_open_carries_correlation_keys() -> None:
    """A PERP_OPEN result yields a handle with the venue correlation keys."""
    result = _make_hl_result(reduce_only=False)
    handle = _hook().extract_pending_fill_handle(result)
    assert isinstance(handle, PendingFillHandle)
    assert handle.protocol == "hyperliquid"
    assert handle.intent_type == "PERP_OPEN"
    assert int(handle.cloid_hex, 16) == _CLOID_INT
    assert handle.coin == "BTC"


def test_extract_handle_none_for_close() -> None:
    """A reduce-only CLOSE is not a pending open → no handle."""
    result = _make_hl_result(reduce_only=True)
    assert _hook().extract_pending_fill_handle(result) is None


def test_extract_handle_none_for_non_hl_result() -> None:
    """A result with no decodable HL order → no handle (inert on foreign results)."""
    result = MagicMock()
    result.transaction_results = []
    assert _hook().extract_pending_fill_handle(result) is None


# ──────────────────────────────────────────────────────────────────────────────
# resolve_fill_status — guard branches → non-terminal UNMEASURED
# ──────────────────────────────────────────────────────────────────────────────


def _handle() -> PendingFillHandle:
    return PendingFillHandle(
        protocol="hyperliquid", intent_type="PERP_OPEN", cloid_hex=_CLOID_HEX, coin="BTC"
    )


def test_resolve_returns_none_for_foreign_handle() -> None:
    """A handle that is not a HL PendingFillHandle → None (not ours to resolve)."""
    verdict = _hook().resolve_fill_status(
        gateway_client=MagicMock(), wallet_address="0xabc", handle=object()
    )
    assert verdict is None


def test_resolve_missing_wallet_is_unmeasured() -> None:
    """No wallet_address → NON-terminal UNMEASURED (never assume flat)."""
    from almanak.connectors.hyperliquid.fill_reconciliation import FillStatus

    verdict = _hook().resolve_fill_status(
        gateway_client=MagicMock(), wallet_address="", handle=_handle()
    )
    assert verdict is not None
    assert str(verdict.status) == str(FillStatus.UNMEASURED)
    assert verdict.terminal is False


def test_resolve_unavailable_fills_read_is_unmeasured() -> None:
    """An unavailable ``userFills`` read → NON-terminal UNMEASURED (stays PENDING)."""
    from almanak.connectors.hyperliquid.fill_reconciliation import FillStatus

    gw = MagicMock()
    # success=False envelope → _read_user_fills returns None (unmeasured).
    fills_resp = MagicMock()
    fills_resp.success = False
    fills_resp.fills = []
    gw.perp_fill.GetUserFills = MagicMock(return_value=fills_resp)

    verdict = _hook().resolve_fill_status(
        gateway_client=gw, wallet_address="0xabc", handle=_handle()
    )
    assert verdict is not None
    assert str(verdict.status) == str(FillStatus.UNMEASURED)
    assert verdict.terminal is False
    # orderStatus is NEVER consulted when the fills read itself was unmeasured —
    # we cannot distinguish reject from lag without a measured fills book.
    gw.perp_fill.GetOrderStatus.assert_not_called()
