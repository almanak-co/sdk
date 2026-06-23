"""Unit tests for VIB-4905 / F1 — partial-match SWAP contract.

Pins the new ``realized_pnl_usd_matched`` / ``unmatched_amount_in`` /
``unmatched_proceeds_usd`` field bundle.  Three semantic regimes:

* **No basis** (no prior lot, or all lots had unknown cost basis): nothing
  matched.  ``realized_pnl_usd = None``, ``realized_pnl_usd_matched = None``,
  ``unmatched_amount_in == amount_in``, ``unmatched_proceeds_usd ==
  amount_in_usd``.
* **Partial match** (sold more than was recorded): matched portion has PnL;
  unmatched residual is surfaced.  ``realized_pnl_usd = None`` (legacy
  contract preserved); ``realized_pnl_usd_matched`` populated.
* **Full match**: ``realized_pnl_usd == realized_pnl_usd_matched``;
  ``unmatched_amount_in == 0``; ``unmatched_proceeds_usd == 0``.

Plus payload schema additive contract, dataclass round-trip, and a real-
shape acceptance number lifted from the Codex audit on the canonical RSI
mainnet trace (matched PnL ≈ -$0.0136 on the SELL leg).
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from almanak.framework.accounting.basis import FIFOBasisStore
from almanak.framework.accounting.category_handlers.swap_handler import (
    _split_proceeds,
    handle_swap,
)
from almanak.framework.accounting.models import (
    AccountingConfidence,
    SwapAccountingEvent,
    SwapEventType,
)
from almanak.framework.accounting.payload_schemas import (
    MATCHING_POLICY_VERSIONS,
    PRIMITIVE_VERSIONS,
    SwapEventPayload,
)
from almanak.framework.primitives.types import Primitive


# ---------------------------------------------------------------------------
# Helpers — mirror the shape used in test_swap_accounting.py so the writer
# input contract stays canonical across the two files.
# ---------------------------------------------------------------------------


_DEPLOYMENT_ID = "dep-swap-f1"
_CYCLE_ID = "cycle-f1"
_WALLET = "0xabcdef1234567890abcdef1234567890abcdef12"
_CHAIN = "arbitrum"


def _outbox(wallet: str = _WALLET, position_key: str = "") -> dict[str, Any]:
    return {
        "id": str(uuid.uuid4()),
        "ledger_entry_id": str(uuid.uuid4()),
        "deployment_id": _DEPLOYMENT_ID,
        "cycle_id": _CYCLE_ID,
        "intent_type": "SWAP",
        "wallet_address": wallet,
        "position_key": position_key,
        "market_id": "",
        "status": "pending",
        "attempts": 0,
        "error": "",
        "created_at": datetime.now(UTC).isoformat(),
        "updated_at": datetime.now(UTC).isoformat(),
    }


def _ledger(
    *,
    token_in: str,
    amount_in: str,
    token_out: str,
    amount_out: str,
    price_inputs_json: str,
    tx_hash: str,
) -> dict[str, Any]:
    return {
        "id": str(uuid.uuid4()),
        "deployment_id": _DEPLOYMENT_ID,
        "cycle_id": _CYCLE_ID,
        "execution_mode": "live",
        "timestamp": datetime.now(UTC).isoformat(),
        "intent_type": "SWAP",
        "token_in": token_in,
        "amount_in": amount_in,
        "token_out": token_out,
        "amount_out": amount_out,
        "effective_price": "",
        "slippage_bps": 10,
        "gas_used": 0,
        "gas_usd": "0.01",
        "tx_hash": tx_hash,
        "chain": _CHAIN,
        "protocol": "uniswap_v3",
        "success": True,
        "error": "",
        "extracted_data_json": "",
        "price_inputs_json": price_inputs_json,
        "pre_state_json": "",
        "post_state_json": "",
    }


def _prices(weth_usd: str = "2000.0", usdc_usd: str = "1.0") -> str:
    return json.dumps({"WETH": weth_usd, "USDC": usdc_usd})


# ---------------------------------------------------------------------------
# Version bumps land on the per-primitive map.
# ---------------------------------------------------------------------------


def test_swap_matching_policy_version_bumped_to_v4() -> None:
    """F1 contract bump: SwapEventPayload partial-match fields → policy v4."""
    assert MATCHING_POLICY_VERSIONS[Primitive.SWAP] == 4


def test_swap_primitive_version_bumped_to_v2() -> None:
    """SWAP primitive contract version.

    VIB-4905 (F1) took it to v2 (partial-match field bundle); VIB-4988 took it
    to v3 (Pendle PT now emits PT_SELL / PT_REDEEM realized-yield events under
    the SWAP primitive), then to v4 (PT_BUY/PT_SELL payloads moved raw-18 →
    human units, uniform with PT_REDEEM); VIB-5316 took it to v5 (PT_BUY now
    populates the buy-time ``sy_price`` the held-PT USD cost basis is anchored
    to); VIB-5314 took it to v6 (PT_SELL/PT_REDEEM realized_yield_usd is strictly
    USD-or-None with a separate realized_yield_sy field). The assertion tracks the
    current value.
    """
    assert PRIMITIVE_VERSIONS[Primitive.SWAP] == 6


# ---------------------------------------------------------------------------
# Regime 1: no prior basis — nothing matched.
# ---------------------------------------------------------------------------


def test_no_prior_basis_emits_unmatched_full_amount_and_null_pnl() -> None:
    """SELL with no prior lots → realized_pnl* = None, unmatched = full amount."""
    basis = FIFOBasisStore()
    # No buys recorded — basis store is empty for WETH on this position key.
    event = handle_swap(
        _outbox(),
        _ledger(
            token_in="WETH",
            amount_in="0.001",
            token_out="USDC",
            amount_out="2",
            price_inputs_json=_prices(),
            tx_hash="0xsell-no-basis",
        ),
        basis,
    )
    assert event is not None
    assert event.realized_pnl_usd is None
    assert event.realized_pnl_usd_matched is None  # nothing matched
    assert event.unmatched_amount_in == Decimal("0.001")
    # Pro-rated unmatched proceeds = full amount_in_usd since matched=0.
    assert event.unmatched_proceeds_usd == event.amount_in_usd
    assert event.amount_in_usd == Decimal("2.0")  # 0.001 * 2000


def test_exhausted_lot_with_existing_key_keeps_matched_pnl_null() -> None:
    """Lots EXIST for the token but are fully consumed → matched PnL is None, not Decimal("0").

    Codex P2 audit catch (VIB-4905): ``match_swap_disposal`` returns
    ``(Decimal("0"), amount)`` for an existing FIFO key with all lots exhausted.
    Before the structural guard, the writer computed ``matched_pnl = 0 - 0 =
    Decimal("0")`` and stamped it as a *measured* zero — conflating with
    "actually $0 matched PnL".  Empty ≠ Zero discipline requires ``None`` here:
    the matched quantity was zero, so matched PnL is *unmeasured*, not
    measured-zero.
    """
    basis = FIFOBasisStore()

    # BUY 0.005 WETH for $10.
    handle_swap(
        _outbox(),
        _ledger(
            token_in="USDC",
            amount_in="10",
            token_out="WETH",
            amount_out="0.005",
            price_inputs_json=_prices(),
            tx_hash="0xbuy-exhaust",
        ),
        basis,
    )

    # SELL 0.005 WETH (consumes the full lot).
    handle_swap(
        _outbox(),
        _ledger(
            token_in="WETH",
            amount_in="0.005",
            token_out="USDC",
            amount_out="10",
            price_inputs_json=_prices(),
            tx_hash="0xsell-consume",
        ),
        basis,
    )

    # SELL again — same token key (still registered) but lots are now
    # exhausted.  match_swap_disposal returns ``(Decimal("0"), amount)``.
    event = handle_swap(
        _outbox(),
        _ledger(
            token_in="WETH",
            amount_in="0.001",
            token_out="USDC",
            amount_out="2",
            price_inputs_json=_prices(),
            tx_hash="0xsell-exhausted",
        ),
        basis,
    )
    assert event is not None
    # Matched PnL stays None even though ``cost_basis_consumed`` was
    # ``Decimal("0")`` (not None) — because the matched QUANTITY was zero.
    assert event.realized_pnl_usd_matched is None
    assert event.realized_pnl_usd is None
    # Unmatched picks up the whole disposal.
    assert event.unmatched_amount_in == Decimal("0.001")


# ---------------------------------------------------------------------------
# Regime 2: full match — legacy and new fields agree.
# ---------------------------------------------------------------------------


def test_full_match_realized_pnl_equals_matched_pnl_and_zero_unmatched() -> None:
    """Buy 1 → Sell same amount: full FIFO match.  ``realized_pnl_usd`` (legacy)
    equals ``realized_pnl_usd_matched``; ``unmatched_*`` are zero (measured zero,
    Empty ≠ Zero — never ``None`` on a known-matched leg).
    """
    basis = FIFOBasisStore()

    # BUY 0.01 WETH for $20 (USDC).
    handle_swap(
        _outbox(),
        _ledger(
            token_in="USDC",
            amount_in="20",
            token_out="WETH",
            amount_out="0.01",
            price_inputs_json=_prices(),
            tx_hash="0xbuy-1",
        ),
        basis,
    )

    # SELL 0.01 WETH for $20.10.
    event = handle_swap(
        _outbox(),
        _ledger(
            token_in="WETH",
            amount_in="0.01",
            token_out="USDC",
            amount_out="20.10",
            price_inputs_json=_prices(weth_usd="2010.0"),
            tx_hash="0xsell-full",
        ),
        basis,
    )
    assert event is not None
    # Full match: legacy and new fields agree.
    assert event.realized_pnl_usd is not None
    assert event.realized_pnl_usd_matched is not None
    assert event.realized_pnl_usd == event.realized_pnl_usd_matched
    # Sanity: realized PnL = matched proceeds ($20.10) - matched basis ($20) = $0.10.
    assert event.realized_pnl_usd_matched == pytest.approx(Decimal("0.10"), rel=Decimal("0.001"))
    # Unmatched is a measured zero, not None.
    assert event.unmatched_amount_in == Decimal("0")
    assert event.unmatched_proceeds_usd == Decimal("0")


# ---------------------------------------------------------------------------
# Regime 3: partial match — matched PnL still surfaces; unmatched populated.
# ---------------------------------------------------------------------------


def test_partial_match_emits_matched_pnl_and_unmatched_residual() -> None:
    """Buy 0.005 WETH → Sell 0.010 WETH (twice what's recorded).

    Pre-F1 contract: ``realized_pnl_usd = None`` (whole leg flagged unknown).
    F1 contract: ``realized_pnl_usd = None`` (legacy preserved) AND
    ``realized_pnl_usd_matched`` carries the matched-half PnL, with
    ``unmatched_amount_in = 0.005`` + pro-rated ``unmatched_proceeds_usd``.
    """
    basis = FIFOBasisStore()

    # BUY 0.005 WETH for $10 (USDC at $2000/ETH).
    handle_swap(
        _outbox(),
        _ledger(
            token_in="USDC",
            amount_in="10",
            token_out="WETH",
            amount_out="0.005",
            price_inputs_json=_prices(),
            tx_hash="0xbuy-half",
        ),
        basis,
    )

    # SELL 0.010 WETH for $20.10 — only the first 0.005 has a basis lot.
    event = handle_swap(
        _outbox(),
        _ledger(
            token_in="WETH",
            amount_in="0.010",
            token_out="USDC",
            amount_out="20.10",
            price_inputs_json=_prices(weth_usd="2010.0"),
            tx_hash="0xsell-partial",
        ),
        basis,
    )
    assert event is not None
    # Legacy field stays None on partial match — back-compat contract.
    assert event.realized_pnl_usd is None
    # New: matched-portion PnL populated.
    # Matched amount = 0.005; matched proceeds = $20.10 * 0.5 = $10.05;
    # matched basis = $10; matched PnL = $0.05.
    assert event.realized_pnl_usd_matched is not None
    assert event.realized_pnl_usd_matched == pytest.approx(Decimal("0.05"), rel=Decimal("0.001"))
    # Unmatched residual = 0.005 WETH; unmatched proceeds = $10.05.
    assert event.unmatched_amount_in == Decimal("0.005")
    assert event.unmatched_proceeds_usd is not None
    assert event.unmatched_proceeds_usd == pytest.approx(Decimal("10.05"), rel=Decimal("0.001"))


def test_partial_match_split_preserves_amount_in_usd() -> None:
    """Matched + unmatched proceeds must sum back to amount_in_usd."""
    basis = FIFOBasisStore()
    handle_swap(
        _outbox(),
        _ledger(
            token_in="USDC",
            amount_in="3",
            token_out="WETH",
            amount_out="0.0015",
            price_inputs_json=_prices(),
            tx_hash="0xbuy-tiny",
        ),
        basis,
    )
    event = handle_swap(
        _outbox(),
        _ledger(
            token_in="WETH",
            amount_in="0.0050",
            token_out="USDC",
            amount_out="10.00",
            price_inputs_json=_prices(weth_usd="2000.0"),
            tx_hash="0xsell-3x",
        ),
        basis,
    )
    assert event is not None
    assert event.amount_in_usd is not None
    assert event.unmatched_proceeds_usd is not None

    matched_proceeds = event.amount_in_usd - event.unmatched_proceeds_usd
    assert matched_proceeds + event.unmatched_proceeds_usd == event.amount_in_usd


# ---------------------------------------------------------------------------
# Direct unit tests for the _split_proceeds helper — pin the contract that
# the integration tests above exercise indirectly.  Each return shape is a
# separate semantic regime (Empty/None vs measured zero vs full split);
# pinning them here protects against a future refactor flipping one branch
# to a different sentinel that would still pass the integration tests.
# ---------------------------------------------------------------------------


def test_split_proceeds_none_when_amount_in_usd_unavailable() -> None:
    """Empty ≠ Zero: ``amount_in_usd is None`` → both legs None."""
    matched, unmatched = _split_proceeds(
        amount_in=Decimal("1.0"),
        amount_in_usd=None,
        unmatched=Decimal("0"),
    )
    assert matched is None
    assert unmatched is None


def test_split_proceeds_none_when_amount_in_is_zero() -> None:
    """Degenerate ``amount_in == 0`` is unmeasured, not a measured-zero swap."""
    matched, unmatched = _split_proceeds(
        amount_in=Decimal("0"),
        amount_in_usd=Decimal("10"),
        unmatched=Decimal("0"),
    )
    assert matched is None
    assert unmatched is None


def test_split_proceeds_no_matched_quantity_returns_none_matched_and_full_unmatched() -> None:
    """``matched_amount <= 0`` → None matched, full unmatched.

    Pinned shape — see ``_split_proceeds`` docstring's three-way contract.
    The matched leg returns ``None`` (not ``Decimal("0")``) because matched
    proceeds are *unmeasured* when matched quantity is zero — asking "how
    much USD attributable to zero tokens" has no answer.  Empty ≠ Zero.
    """
    matched, unmatched = _split_proceeds(
        amount_in=Decimal("1"),
        amount_in_usd=Decimal("10"),
        unmatched=Decimal("1"),  # all unmatched ⇒ matched_amount == 0
    )
    assert matched is None
    assert unmatched == Decimal("10")


def test_split_proceeds_full_pro_rated_split_preserves_sum_invariant() -> None:
    """Pro-rated split's sum invariant: matched + unmatched == amount_in_usd."""
    matched, unmatched = _split_proceeds(
        amount_in=Decimal("2"),
        amount_in_usd=Decimal("20.10"),
        unmatched=Decimal("1"),  # half matched, half unmatched
    )
    assert matched is not None
    assert unmatched is not None
    assert matched + unmatched == Decimal("20.10")
    # Half matched → ~$10.05 (within Decimal-context precision).
    assert matched == pytest.approx(Decimal("10.05"), rel=Decimal("0.001"))


# ---------------------------------------------------------------------------
# Codex acceptance: the canonical RSI mainnet trace.
# ---------------------------------------------------------------------------


def test_rsi_mainnet_matched_pnl_matches_codex_recomputation() -> None:
    """Approximation of the RSI mainnet trace SELL leg per Codex's audit.

    Two BUYs at ~$10 each → SELL the full WETH back for ~$9.99 (slippage +
    fees).  Codex computed: matched proceeds ≈ $9.9909519, matched basis ≈
    $10.0045810, matched PnL ≈ -$0.0136.

    This isn't a replay of the literal mainnet decimals — the trace's
    precision-perfect numbers are in
    ``/tmp/rsi_mainnet_test/almanak_state.db``.  The test pins the
    SHAPE-and-magnitude that the F1 contract must surface for any
    full-match SELL in the same regime: matched PnL on the order of cents,
    not None.
    """
    basis = FIFOBasisStore()

    # BUY #1: 10 USDC → 0.005 WETH.
    handle_swap(
        _outbox(),
        _ledger(
            token_in="USDC",
            amount_in="10.0",
            token_out="WETH",
            amount_out="0.005",
            price_inputs_json=_prices(),
            tx_hash="0xrsi-buy-1",
        ),
        basis,
    )
    # BUY #2: another 10 USDC → 0.005 WETH.
    handle_swap(
        _outbox(),
        _ledger(
            token_in="USDC",
            amount_in="10.0",
            token_out="WETH",
            amount_out="0.005",
            price_inputs_json=_prices(),
            tx_hash="0xrsi-buy-2",
        ),
        basis,
    )
    # SELL 0.01 WETH (full of acquired) for ~$19.95 (slippage).
    event = handle_swap(
        _outbox(),
        _ledger(
            token_in="WETH",
            amount_in="0.01",
            token_out="USDC",
            amount_out="19.95",
            price_inputs_json=_prices(weth_usd="1995.0"),
            tx_hash="0xrsi-sell",
        ),
        basis,
    )
    assert event is not None
    # Full match: realized PnL = $19.95 (proceeds) - $20 (basis) = -$0.05.
    # The matched field carries this on partial matches too — F1's
    # whole point.
    assert event.realized_pnl_usd is not None
    assert event.realized_pnl_usd_matched is not None
    assert event.realized_pnl_usd == event.realized_pnl_usd_matched
    # Negative cents — the magnitude / sign Codex documented.  Loose
    # tolerance because the test uses round numbers, not mainnet decimals.
    assert event.realized_pnl_usd_matched < Decimal("0")
    assert abs(event.realized_pnl_usd_matched) < Decimal("0.50")
    # Unmatched zero — full match.
    assert event.unmatched_amount_in == Decimal("0")


# ---------------------------------------------------------------------------
# Payload schema: additive — new fields validate, legacy payloads accepted.
# ---------------------------------------------------------------------------


def test_payload_schema_accepts_new_partial_match_fields() -> None:
    """The Pydantic model carries the three new fields without rejecting them."""
    p = SwapEventPayload(
        protocol="uniswap_v3",
        token_in="WETH",
        token_out="USDC",
        amount_in=Decimal("0.01"),
        amount_out=Decimal("20"),
        confidence="HIGH",
        realized_pnl_usd_matched=Decimal("0.05"),
        unmatched_amount_in=Decimal("0.005"),
        unmatched_proceeds_usd=Decimal("10.05"),
    )
    assert p.realized_pnl_usd_matched == Decimal("0.05")
    assert p.unmatched_amount_in == Decimal("0.005")
    assert p.unmatched_proceeds_usd == Decimal("10.05")


def test_payload_schema_legacy_payload_validates_without_new_fields() -> None:
    """Pre-v2 payloads on disk (no matched/unmatched bundle) must round-trip."""
    p = SwapEventPayload(
        protocol="uniswap_v3",
        token_in="WETH",
        token_out="USDC",
        amount_in=Decimal("0.01"),
        amount_out=Decimal("20"),
        confidence="HIGH",
        realized_pnl_usd=Decimal("0.05"),
        # No matched/unmatched bundle — should default to None.
    )
    assert p.realized_pnl_usd_matched is None
    assert p.unmatched_amount_in is None
    assert p.unmatched_proceeds_usd is None


# ---------------------------------------------------------------------------
# Dataclass round-trip: payload preserves the new fields end-to-end.
# ---------------------------------------------------------------------------


def test_swap_accounting_event_round_trip_preserves_new_fields() -> None:
    from almanak.framework.accounting.models import AccountingIdentity

    identity = AccountingIdentity(
        id="acc-1",
        deployment_id=_DEPLOYMENT_ID,
        cycle_id=_CYCLE_ID,
        execution_mode="live",
        timestamp=datetime.now(UTC),
        chain=_CHAIN,
        protocol="uniswap_v3",
        wallet_address=_WALLET,
        tx_hash="0xrt",
        ledger_entry_id="le-rt",
    )
    original = SwapAccountingEvent(
        identity=identity,
        event_type=SwapEventType.SWAP,
        protocol="uniswap_v3",
        token_in="WETH",
        token_out="USDC",
        amount_in=Decimal("0.01"),
        amount_out=Decimal("20.10"),
        amount_in_usd=Decimal("20.10"),
        amount_out_usd=Decimal("20.10"),
        effective_price=Decimal("2010"),
        slippage_bps=5,
        realized_pnl_usd=None,  # legacy: null on partial
        cost_basis_recorded=True,
        gas_usd=Decimal("0.01"),
        confidence=AccountingConfidence.HIGH,
        unavailable_reason="",
        swap_position_key=f"swap:{_CHAIN}:{_WALLET.lower()}",
        realized_pnl_usd_matched=Decimal("0.05"),
        unmatched_amount_in=Decimal("0.005"),
        unmatched_proceeds_usd=Decimal("10.05"),
    )
    raw = original.to_payload_json()
    decoded = SwapAccountingEvent.from_payload_json(identity, raw)

    assert decoded.realized_pnl_usd_matched == Decimal("0.05")
    assert decoded.unmatched_amount_in == Decimal("0.005")
    assert decoded.unmatched_proceeds_usd == Decimal("10.05")
    # Legacy field also preserved.
    assert decoded.realized_pnl_usd is None


def test_swap_accounting_event_round_trip_tolerates_legacy_v1_payload() -> None:
    """A v1 payload on disk (no matched/unmatched bundle) deserialises with
    ``None`` defaults — the read path doesn't crash on missing keys.
    """
    from almanak.framework.accounting.models import AccountingIdentity

    identity = AccountingIdentity(
        id="acc-legacy",
        deployment_id=_DEPLOYMENT_ID,
        cycle_id=_CYCLE_ID,
        execution_mode="live",
        timestamp=datetime.now(UTC),
        chain=_CHAIN,
        protocol="uniswap_v3",
        wallet_address=_WALLET,
        tx_hash="0xlegacy",
        ledger_entry_id="le-legacy",
    )
    # Hand-built legacy v1 payload — no matched/unmatched keys.
    legacy_payload = json.dumps(
        {
            "event_type": "SWAP",
            "protocol": "uniswap_v3",
            "token_in": "WETH",
            "token_out": "USDC",
            "amount_in": "0.01",
            "amount_out": "20",
            "amount_in_usd": "20",
            "amount_out_usd": "20",
            "effective_price": "2000",
            "slippage_bps": 0,
            "realized_pnl_usd": "0.0",
            "cost_basis_recorded": True,
            "gas_usd": "0.01",
            "confidence": "HIGH",
            "unavailable_reason": None,
            "swap_position_key": "swap:arbitrum:wallet",
            "schema_version": 1,
            "primitive_version": 1,
        }
    )
    decoded = SwapAccountingEvent.from_payload_json(identity, legacy_payload)

    # New fields default to None — v1 contract preserved on read.
    assert decoded.realized_pnl_usd_matched is None
    assert decoded.unmatched_amount_in is None
    assert decoded.unmatched_proceeds_usd is None
    assert decoded.realized_pnl_usd == Decimal("0.0")
