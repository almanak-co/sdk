"""VIB-5568: PERP_CANCEL_ORDER vocabulary + GMX V2 compiler.

The recovery half of VIB-5116: a new first-class ``PERP_CANCEL_ORDER`` intent
verb that cancels a stranded GMX V2 pending order to recover its committed
collateral. These tests cover the vocabulary contract (fields, factory,
round-trip, fail-closed key validation, taxonomy classification) and the GMX V2
compiler (``ExchangeRouter.cancelOrder(bytes32)`` calldata, value=0, fail-closed
on an unsupported chain).
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from eth_utils import function_signature_to_4byte_selector

from almanak.connectors._strategy_base.base.compiler import PerpCompilerContext
from almanak.connectors.gmx_v2.compiler import GMXV2Compiler
from almanak.framework.intents.compiler_models import CompilationStatus
from almanak.framework.intents.vocabulary import Intent, IntentType, PerpCancelIntent
from almanak.framework.primitives.taxonomy import record_for
from almanak.framework.primitives.types import AccountingCategory, EventKind

# A full, well-formed bytes32 order key (0x + 64 hex chars = 66 chars).
_KEY = "0x" + "1234abcd" * 8

# VIB-5568: the cancel selector MUST be the real 4-byte selector of
# ``cancelOrder(bytes32)``. Deriving it from the signature (rather than
# hardcoding a literal) is the regression guard for the wrong-selector bug: the
# connector originally shipped ``0xd42a7b9e`` (a non-matching selector), so the
# runner's calldata reverted empty ("(no reason)") on-chain while cast — which
# re-encodes from the signature string — succeeded, masking the defect. The real
# selector is ``0x7489ec23``.
_CANCEL_ORDER_SELECTOR = "0x" + function_signature_to_4byte_selector("cancelOrder(bytes32)").hex()


# --------------------------------------------------------------------------- #
# Vocabulary
# --------------------------------------------------------------------------- #


def test_intent_type_declared():
    assert IntentType.PERP_CANCEL_ORDER.value == "PERP_CANCEL_ORDER"


def test_factory_builds_cancel_intent():
    intent = Intent.perp_cancel_order(order_key=_KEY, protocol="gmx_v2", chain="arbitrum")
    assert isinstance(intent, PerpCancelIntent)
    assert intent.intent_type == IntentType.PERP_CANCEL_ORDER
    assert intent.order_key == _KEY
    assert intent.protocol == "gmx_v2"
    assert intent.chain == "arbitrum"


def test_factory_defaults_to_gmx_v2():
    intent = Intent.perp_cancel_order(order_key=_KEY)
    assert intent.protocol == "gmx_v2"
    assert intent.chain is None


def test_serialize_deserialize_round_trip():
    intent = Intent.perp_cancel_order(order_key=_KEY, chain="arbitrum")
    data = intent.serialize()
    assert data["type"] == "PERP_CANCEL_ORDER"
    back = Intent.deserialize(data)
    assert isinstance(back, PerpCancelIntent)
    assert back.order_key == intent.order_key
    assert back.intent_type == IntentType.PERP_CANCEL_ORDER


@pytest.mark.parametrize(
    "bad_key",
    [
        "0xdead",  # too short
        "1234abcd" * 8,  # missing 0x prefix
        "0x" + "zz" * 32,  # non-hex
        "0x" + "ab" * 31,  # 62 hex chars (truncated bytes31)
        "0x" + "ab" * 33,  # 66 hex chars (oversized)
        "0x" + "a" * 31 + "_" + "b" * 32,  # 66 chars but interior underscore (int(,16) accepts, regex must not)
    ],
)
def test_malformed_key_rejected_fail_closed(bad_key):
    """A malformed / truncated key is rejected — never zero-padded into a
    DIFFERENT valid order key that would cancel (and refund) the wrong order."""
    with pytest.raises(Exception):  # noqa: B017,PT011 — pydantic ValidationError or ValueError
        PerpCancelIntent(order_key=bad_key)


def test_taxonomy_classifies_as_no_accounting_refund():
    """A cancel closes NO position: NO_ACCOUNTING / EventKind.NONE / no position
    type — never a PERP close (which would fabricate an unmatched close leg)."""
    record = record_for("PERP_CANCEL_ORDER")
    assert record.accounting_category == AccountingCategory.NO_ACCOUNTING
    assert record.event_kind == EventKind.NONE
    assert record.position_type is None
    assert record.required_lifecycle == ()


# --------------------------------------------------------------------------- #
# GMX V2 compiler
# --------------------------------------------------------------------------- #


@pytest.fixture
def compiler_ctx():
    compiler = GMXV2Compiler()
    ctx = PerpCompilerContext(
        chain="arbitrum",
        wallet_address="0x" + "ab" * 20,
        rpc_url=None,
        rpc_timeout=10.0,
        permission_discovery=False,
        allow_placeholder_prices=True,
        token_resolver=None,
        gateway_client=None,
        price_oracle=None,
        cache={},
        services=MagicMock(),
        default_protocol="gmx_v2",
        protocol="gmx_v2",
    )
    return compiler, ctx


def test_compiler_declares_cancel_intent():
    assert IntentType.PERP_CANCEL_ORDER in GMXV2Compiler.intents


def test_cancel_selector_matches_canonical_signature():
    """VIB-5568 regression: the adapter's hardcoded cancel selector MUST equal
    the real 4-byte selector of ``cancelOrder(bytes32)`` (``0x7489ec23``).

    The connector originally shipped ``0xd42a7b9e`` — a wrong selector that made
    every on-chain cancel revert with empty data ("(no reason)"), stranding the
    order's collateral. cast masked it by re-encoding from the signature string.
    """
    from almanak.connectors.gmx_v2.adapter import GMX_CANCEL_ORDER_SELECTOR

    canonical = "0x" + function_signature_to_4byte_selector("cancelOrder(bytes32)").hex()
    assert canonical == "0x7489ec23"
    assert GMX_CANCEL_ORDER_SELECTOR == canonical, (
        f"GMX_CANCEL_ORDER_SELECTOR={GMX_CANCEL_ORDER_SELECTOR} does not match "
        f"cancelOrder(bytes32)={canonical} — the runner's calldata will revert empty on-chain"
    )


def test_compile_cancel_builds_cancel_order_calldata(compiler_ctx):
    compiler, ctx = compiler_ctx
    intent = Intent.perp_cancel_order(order_key=_KEY, protocol="gmx_v2", chain="arbitrum")

    result = compiler.compile(ctx, intent)

    assert result.status == CompilationStatus.SUCCESS
    assert len(result.transactions) == 1
    tx = result.transactions[0]
    # ExchangeRouter.cancelOrder(bytes32) — single call, no keeper fee.
    assert tx.value == 0
    assert tx.tx_type == "perp_cancel_order"
    assert tx.data[:10] == _CANCEL_ORDER_SELECTOR
    # The bytes32 key is embedded verbatim in the calldata (no truncation).
    assert tx.data[10:].lower() == _KEY[2:].lower()
    assert result.action_bundle.intent_type == "PERP_CANCEL_ORDER"
    assert result.action_bundle.metadata["order_key"] == _KEY


def test_compile_cancel_fails_closed_on_unsupported_chain(compiler_ctx):
    compiler, _ = compiler_ctx
    ctx = PerpCompilerContext(
        chain="base",  # GMX V2 is not on base
        wallet_address="0x" + "ab" * 20,
        rpc_url=None,
        rpc_timeout=10.0,
        permission_discovery=False,
        allow_placeholder_prices=True,
        token_resolver=None,
        gateway_client=None,
        price_oracle=None,
        cache={},
        services=MagicMock(),
        default_protocol="gmx_v2",
        protocol="gmx_v2",
    )
    intent = Intent.perp_cancel_order(order_key=_KEY, protocol="gmx_v2", chain="base")

    result = compiler.compile(ctx, intent)

    assert result.status == CompilationStatus.FAILED
    assert "chain" in (result.error or "").lower()


def test_adapter_build_cancel_order_tx_is_stateless():
    """build_cancel_order_tx must NOT depend on in-memory order tracking — a
    teardown-discovered stranded order (fresh process) is never tracked."""
    from almanak.connectors.gmx_v2.adapter import GMXv2Adapter, GMXv2Config

    adapter = GMXv2Adapter(GMXv2Config(chain="arbitrum", wallet_address="0x" + "ab" * 20))
    tx = adapter.build_cancel_order_tx(_KEY)
    assert tx.value == 0
    assert tx.data[:10] == _CANCEL_ORDER_SELECTOR
    assert tx.data[10:].lower() == _KEY[2:].lower()


# --------------------------------------------------------------------------- #
# Cancel age-gate: residual discovery marks orders cancellable only past GMX's
# ~300s REQUEST_EXPIRATION_TIME (VIB-5568). Fail-closed: an unread clock or a
# key-only stub (age unmeasured) is NOT cancellable.
# --------------------------------------------------------------------------- #


def _pending_order(order_key: str, updated_at_time: int):
    from almanak.connectors.gmx_v2.orders_read import PendingOrder

    return PendingOrder(
        market="0x" + "11" * 20,
        initial_collateral_token="0x" + "22" * 20,
        initial_collateral_delta_amount=150_000000,
        size_delta_usd=0,
        order_type=2,
        execution_fee=0,
        is_long=True,
        order_key=order_key,
        updated_at_time=updated_at_time,
    )


def _residuals_by_key(monkeypatch, orders, now_ts):
    from almanak.connectors.gmx_v2 import teardown_residual_discovery as trd
    from almanak.connectors.gmx_v2.orders_read import PendingOrdersResult

    keys = [o.order_key for o in orders]
    monkeypatch.setattr(
        trd,
        "read_pending_orders",
        lambda *a, **k: PendingOrdersResult(orders=list(orders), order_keys=keys, ok=True, measured_count=len(orders)),
    )
    monkeypatch.setattr(trd, "read_chain_timestamp", lambda *a, **k: now_ts)
    result = trd.gmx_v2_teardown_residual_discovery("0x" + "ab" * 20, "arbitrum", object())
    assert result.ok is True
    return {r.identifier: r.details for r in result.residuals}


def test_residual_old_order_is_cancellable(monkeypatch):
    old = _pending_order("0x" + "ab" * 32, updated_at_time=1_000_000)
    details = _residuals_by_key(monkeypatch, [old], now_ts=1_000_000 + 400)  # 400s old > 315 gate
    assert details["0x" + "ab" * 32]["cancellable"] is True


def test_residual_young_order_is_deferred(monkeypatch):
    young = _pending_order("0x" + "cd" * 32, updated_at_time=1_000_000)
    details = _residuals_by_key(monkeypatch, [young], now_ts=1_000_000 + 70)  # 70s old < 315 gate
    d = details["0x" + "cd" * 32]
    assert d["cancellable"] is False
    assert d["seconds_until_cancellable"] > 0


def test_residual_unread_clock_defers_fail_closed(monkeypatch):
    """Unmeasured chain time → never cancel on a guessed clock."""
    order = _pending_order("0x" + "ef" * 32, updated_at_time=1_000_000)
    details = _residuals_by_key(monkeypatch, [order], now_ts=None)
    assert details["0x" + "ef" * 32]["cancellable"] is False
