"""Unit tests for the Senior-Quant header aggregator.

Covers:
- Empty inputs → degraded but valid header.
- Cost stack: per-event-type accumulation across SWAP, LP_CLOSE,
  PERP_CLOSE, REPAY, WITHDRAW.
- Reconciliation (G6): PASS within ε, FAIL outside ε, NA when no data.
- Audit trail: counts price_inputs / pre+post / gas / version stamps.
- Posture: G2 PASS when all rows have gas; G6 FAIL surfaces in chips;
  Track-C cells stay XFAIL regardless of data.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

from almanak.framework.dashboard.quant_aggregations import (
    _detect_primitive,
    _open_position_cost_basis,
    _wallet_value_at_first_action,
    build_quant_header,
    compute_audit_trail,
    compute_cost_stack,
    compute_reconciliation,
)


def _ledger(
    *,
    gas_usd: str = "",
    price_inputs_json: str = "",
    pre_state_json: str = "",
    post_state_json: str = "",
    cycle_id: str = "cyc-1",
    tx_hash: str = "0xabc",
) -> SimpleNamespace:
    return SimpleNamespace(
        id="le-1",
        cycle_id=cycle_id,
        timestamp=datetime.now(tz=UTC),
        intent_type="SWAP",
        token_in="USDC",
        amount_in="100",
        token_out="WETH",
        amount_out="0.05",
        effective_price="0.0005",
        slippage_bps=2.0,
        gas_used=200_000,
        gas_usd=gas_usd,
        tx_hash=tx_hash,
        chain="arbitrum",
        protocol="uniswap_v3",
        success=True,
        error="",
        extracted_data_json="",
        price_inputs_json=price_inputs_json,
        pre_state_json=pre_state_json,
        post_state_json=post_state_json,
    )


def _event(
    event_type: str,
    payload: dict[str, object] | None = None,
    *,
    confidence: str = "HIGH",
) -> dict[str, object]:
    base = {"event_type": event_type, "schema_version": 1, "formula_version": 1, "matching_policy_version": 1}
    if payload:
        base.update(payload)
    return {
        "id": "ev-1",
        "deployment_id": "strat-1",
        "strategy_id": "strat-1",
        "cycle_id": "cyc-1",
        "execution_mode": "live",
        "event_type": event_type,
        "timestamp": datetime.now(tz=UTC).isoformat(),
        "chain": "arbitrum",
        "protocol": "test",
        "wallet_address": "0xWALLET",
        "ledger_entry_id": "le-1",
        "tx_hash": "0xabc",
        "confidence": confidence,
        "schema_version": 1,
        "payload_json": json.dumps(base),
    }


# ─── Cost stack ───────────────────────────────────────────────────────────


def test_cost_stack_empty():
    cs = compute_cost_stack([], [])
    assert cs.gas_usd == Decimal("0")
    assert cs.realized_pnl_usd == Decimal("0")


def test_cost_stack_swap_event():
    ledger = [_ledger(gas_usd="0.05")]
    events = [
        _event(
            "SWAP",
            {"slippage_usd": "0.10", "realized_pnl_usd": "1.50", "protocol_fee_usd": "0.20"},
        )
    ]
    cs = compute_cost_stack(ledger, events)
    assert cs.gas_usd == Decimal("0.05")
    assert cs.slippage_usd == Decimal("0.10")
    assert cs.realized_pnl_usd == Decimal("1.50")
    assert cs.protocol_fees_usd == Decimal("0.20")


def test_cost_stack_lp_close_fees_and_pnl_and_il():
    events = [
        _event(
            "LP_CLOSE",
            {"fees_total_usd": "5.0", "realized_pnl_usd": "2.5", "il_usd": "-0.7"},
        )
    ]
    cs = compute_cost_stack([], events)
    assert cs.fees_earned_usd == Decimal("5.0")
    assert cs.realized_pnl_usd == Decimal("2.5")
    assert cs.il_usd == Decimal("-0.7")


def test_cost_stack_perp_funding_split():
    events = [
        _event(
            "PERP_CLOSE",
            {
                "open_fee_usd": "0.30",
                "close_fee_usd": "0.40",
                "price_impact_usd": "0.15",
                "funding_paid_usd": "0.20",
                "funding_received_usd": "0.05",
                "realized_pnl_usd": "1.10",
            },
        )
    ]
    cs = compute_cost_stack([], events)
    assert cs.protocol_fees_usd == Decimal("0.40")  # close fee on PERP_CLOSE
    assert cs.funding_paid_usd == Decimal("0.20")
    assert cs.funding_earned_usd == Decimal("0.05")


def test_cost_stack_lending_interest_split():
    events = [
        _event("WITHDRAW", {"interest_accrued_usd": "0.15"}),
        _event("REPAY", {"interest_paid_usd": "0.50"}),
    ]
    cs = compute_cost_stack([], events)
    assert cs.interest_earned_usd == Decimal("0.15")
    assert cs.interest_paid_usd == Decimal("0.50")


# ─── Reconciliation (G6) ──────────────────────────────────────────────────


def test_reconciliation_empty_is_na():
    cs = compute_cost_stack([], [])
    rec = compute_reconciliation(
        initial_value_usd=Decimal("0"),
        nav_usd=Decimal("0"),
        cost_stack=cs,
        accounting_events=[],
    )
    assert rec.has_data is False
    assert rec.passed is False


def test_reconciliation_passes_within_epsilon():
    """Wallet PnL = -$0.04 (gas only). Components = -$0.04. Gap = 0."""
    ledger = [_ledger(gas_usd="0.04")]
    events: list[dict[str, object]] = []
    cs = compute_cost_stack(ledger, events)
    rec = compute_reconciliation(
        initial_value_usd=Decimal("100"),
        nav_usd=Decimal("99.96"),
        cost_stack=cs,
        accounting_events=events,
    )
    # has_data is True because initial > 0
    assert rec.has_data is True
    assert rec.passed is True
    assert rec.gap_usd <= rec.epsilon_usd


def test_reconciliation_fails_outside_epsilon():
    """Wallet says -$2.00 but components only explain -$0.05 — gap $1.95."""
    ledger = [_ledger(gas_usd="0.05")]
    events = [_event("SWAP", {"realized_pnl_usd": "0"})]
    cs = compute_cost_stack(ledger, events)
    rec = compute_reconciliation(
        initial_value_usd=Decimal("100"),
        nav_usd=Decimal("98.00"),
        cost_stack=cs,
        accounting_events=events,
    )
    assert rec.has_data is True
    assert rec.passed is False
    # Gap should be roughly $1.95
    assert rec.gap_usd > Decimal("1.5")


# ─── Audit trail ──────────────────────────────────────────────────────────


def test_audit_trail_partial_population():
    ledger = [
        _ledger(gas_usd="0.05", price_inputs_json='{"USDC":{"price_usd":"1.0"}}'),
        _ledger(gas_usd="", price_inputs_json=""),  # teardown-lane gap
        _ledger(
            gas_usd="0.08",
            price_inputs_json='{"WETH":{"price_usd":"3000"}}',
            pre_state_json='{"a":1}',
            post_state_json='{"a":2}',
        ),
    ]
    events = [_event("SWAP")]
    audit = compute_audit_trail(ledger, events)
    assert audit.ledger_total == 3
    assert audit.ledger_with_gas_usd == 2
    assert audit.ledger_with_price_inputs == 2
    assert audit.ledger_with_pre_post_state == 1
    assert audit.events_total == 1
    assert audit.events_with_versions == 1


# ─── Primitive detection ──────────────────────────────────────────────────


def test_detect_primitive_lp():
    assert _detect_primitive([_event("LP_OPEN"), _event("LP_CLOSE")]) == "lp"


def test_detect_primitive_lending():
    assert _detect_primitive([_event("SUPPLY"), _event("BORROW"), _event("REPAY")]) == "lending"


def test_detect_primitive_mixed():
    assert _detect_primitive([_event("LP_OPEN"), _event("SUPPLY")]) == "mixed"


def test_detect_primitive_swap_only():
    assert _detect_primitive([_event("SWAP")]) == "swap"


# ─── Top-level builder ────────────────────────────────────────────────────


def test_build_quant_header_lp_strategy_with_gas_complete():
    """LP run where gas_usd is populated and reconciliation passes."""
    metrics = SimpleNamespace(
        initial_value_usd="100.00",
        deposits_usd="0",
        withdrawals_usd="0",
        total_value_usd="105.00",
        initial_timestamp=datetime.now(tz=UTC).isoformat(),
        gas_spent_usd="0.10",
    )
    # VIB-3884 / VIB-3614 model: ``total_value_usd`` = deployed positions
    # only; cash is separate. Wallet NAV (header "NAV now" tile) is
    # ``total_value_usd + available_cash_usd``. The strategy below starts
    # at $100, ends with $50 deployed + $50 cash = $100 wallet NAV (flat).
    snapshots = [
        SimpleNamespace(
            total_value_usd="0",
            available_cash_usd="100",
            value_confidence="HIGH",
            deployed_capital_usd="0",
            positions_json="[]",
        ),
        SimpleNamespace(
            total_value_usd="55",
            available_cash_usd="50",
            value_confidence="HIGH",
            deployed_capital_usd="55",
            positions_json='[{"position_type":"LP","in_range":true}]',
        ),
    ]
    ledger = [
        _ledger(gas_usd="0.05", price_inputs_json='{"USDC":{"price_usd":"1"}}'),
    ]
    events = [
        _event("LP_OPEN", {"cost_basis_usd": "55"}),
        _event("LP_CLOSE", {"fees_total_usd": "5.05", "realized_pnl_usd": "5.0"}),
    ]

    h = build_quant_header(
        portfolio_metrics=metrics,
        snapshots=snapshots,
        ledger_entries=ledger,
        accounting_events=events,
    )

    assert h.deployed_usd == Decimal("100.00")
    # VIB-3884: wallet NAV = positions ($55) + cash ($50) = $105.
    assert h.nav_usd == Decimal("105")
    assert h.lifetime_pnl_usd == Decimal("5")
    assert h.value_confidence == "HIGH"
    assert h.posture.primitive == "lp"
    # G2 should pass (gas populated on the only ledger row)
    assert "G2" not in h.posture.failing
    # Track-C cells remain XFAIL regardless
    assert "G14" in h.posture.xfail
    assert "G15" in h.posture.xfail
    assert "LP2" in h.posture.xfail


def test_build_quant_header_simulates_may2_looping_run():
    """Reproduce the May 2 mainnet looping result — 4/8 ledger rows
    missing gas (teardown lane), pre/post-state NULL → G2 partial,
    L4 FAIL, G6 FAIL.
    """
    metrics = SimpleNamespace(
        initial_value_usd="1.00",
        deposits_usd="0",
        withdrawals_usd="0",
        total_value_usd="-1.00",  # net loss, gas-only effective
        initial_timestamp=datetime.now(tz=UTC).isoformat(),
        gas_spent_usd="0.07",
    )
    snapshots = [
        SimpleNamespace(
            total_value_usd="1.00",
            available_cash_usd="1.00",
            value_confidence="HIGH",
            deployed_capital_usd="0",
            positions_json="[]",
        ),
    ]
    # 4 iteration rows with gas, 4 teardown rows without
    ledger = [_ledger(gas_usd="0.012") for _ in range(4)] + [_ledger(gas_usd="") for _ in range(4)]
    # All ledger rows carry price_inputs (G12 PASS)
    for le in ledger:
        le.price_inputs_json = '{"USDC":{"price_usd":"1"}}'
    # Lending events with NULL principal/interest USD (VIB-3474 pending)
    events = [
        _event("SUPPLY"),
        _event("BORROW"),
        _event("WITHDRAW"),
        _event("REPAY", {"principal_repaid_usd": None, "interest_paid_usd": None}),
    ]

    h = build_quant_header(
        portfolio_metrics=metrics,
        snapshots=snapshots,
        ledger_entries=ledger,
        accounting_events=events,
    )

    # G2 should fail (only 4/8 have gas)
    assert "G2" in h.posture.failing
    # G12 should pass (all 8 have price_inputs)
    assert "G12" not in h.posture.failing
    # G13 should pass (versions stamped)
    assert "G13" not in h.posture.failing
    # L4 should fail (REPAY exists but USD fields NULL)
    assert "L4" in h.posture.failing
    # G6 should fail — wallet says -$2 but components only explain ~$0.05
    assert "G6" in h.posture.failing


# ─── VIB-3914: Wallet-anchored "Deployed" helpers ─────────────────────────


def _ledger_with_pre_state(
    *,
    wallet_balances: dict[str, str],
    prices: dict[str, str],
    timestamp: datetime | None = None,
) -> SimpleNamespace:
    """Build a ledger row with `pre_state_json` + `price_inputs_json` populated."""
    pre_state = json.dumps({"wallet_balances": wallet_balances})
    price_inputs = json.dumps({tok: {"price_usd": p} for tok, p in prices.items()})
    le = _ledger(
        gas_usd="0.01",
        price_inputs_json=price_inputs,
        pre_state_json=pre_state,
    )
    if timestamp is not None:
        le.timestamp = timestamp
    return le


def test_wallet_value_at_first_action_empty_returns_none():
    assert _wallet_value_at_first_action([]) is None


def test_wallet_value_at_first_action_no_pre_state_returns_none():
    """Ledger row without pre_state/price_inputs → fall back signal."""
    assert _wallet_value_at_first_action([_ledger()]) is None


def test_wallet_value_at_first_action_single_row():
    le = _ledger_with_pre_state(
        wallet_balances={"USDC": "10", "WETH": "0.005"},
        prices={"USDC": "1.0001", "WETH": "3700"},
    )
    total = _wallet_value_at_first_action([le])
    # 10 * 1.0001 + 0.005 * 3700 = 10.001 + 18.5 = 28.501
    assert total == Decimal("28.501")


def test_wallet_value_at_first_action_picks_earliest_by_timestamp():
    """When multiple rows carry pre_state, the earliest one wins.

    The earliest reflects the wallet's state when the strategy first
    acted — the right anchor for a broker-statement-style "Deployed".
    """
    early_ts = datetime(2026, 5, 1, tzinfo=UTC)
    late_ts = datetime(2026, 5, 3, tzinfo=UTC)
    early = _ledger_with_pre_state(
        wallet_balances={"USDC": "5"},
        prices={"USDC": "1"},
        timestamp=early_ts,
    )
    late = _ledger_with_pre_state(
        wallet_balances={"USDC": "999"},  # would dominate if picked
        prices={"USDC": "1"},
        timestamp=late_ts,
    )
    # Pass them in reverse order to confirm the helper sorts.
    total = _wallet_value_at_first_action([late, early])
    assert total == Decimal("5")


def test_wallet_value_at_first_action_skips_failed_rows_with_empty_balances():
    """A pre_state row with all-zero balances yields total=0 → keep
    looking. Real-world: pre-flight failure before wallet was funded."""
    empty = _ledger_with_pre_state(
        wallet_balances={"USDC": "0"},
        prices={"USDC": "1"},
        timestamp=datetime(2026, 5, 1, tzinfo=UTC),
    )
    populated = _ledger_with_pre_state(
        wallet_balances={"USDC": "20"},
        prices={"USDC": "1"},
        timestamp=datetime(2026, 5, 2, tzinfo=UTC),
    )
    total = _wallet_value_at_first_action([empty, populated])
    assert total == Decimal("20")


def test_wallet_value_at_first_action_malformed_json_falls_through():
    bad = _ledger(
        gas_usd="0.01",
        pre_state_json="{not-json",
        price_inputs_json='{"USDC":{"price_usd":"1"}}',
    )
    good = _ledger_with_pre_state(
        wallet_balances={"USDC": "7"},
        prices={"USDC": "1"},
    )
    total = _wallet_value_at_first_action([bad, good])
    assert total == Decimal("7")


def test_wallet_value_at_first_action_missing_price_for_token_skips_token():
    """A balance with no matching price entry contributes 0 — never
    invents a number."""
    le = _ledger_with_pre_state(
        wallet_balances={"USDC": "10", "WETH": "0.5"},
        prices={"USDC": "1"},  # no WETH price
    )
    total = _wallet_value_at_first_action([le])
    assert total == Decimal("10")


def test_open_position_cost_basis_empty_returns_zero():
    assert _open_position_cost_basis([]) == Decimal("0")


def test_open_position_cost_basis_single_open():
    events = [_event("LP_OPEN", {"cost_basis_usd": "6.45", "position_key": "lp:abc"})]
    assert _open_position_cost_basis(events) == Decimal("6.45")


def test_open_position_cost_basis_matched_close_zeroes_out():
    events = [
        _event("LP_OPEN", {"cost_basis_usd": "6.45", "position_key": "lp:abc"}),
        _event("LP_CLOSE", {"position_key": "lp:abc"}),
    ]
    assert _open_position_cost_basis(events) == Decimal("0")


def test_open_position_cost_basis_unmatched_close_leaves_open_alone():
    """A CLOSE on a different position_key must not zero out the open."""
    events = [
        _event("LP_OPEN", {"cost_basis_usd": "100", "position_key": "lp:abc"}),
        _event("LP_CLOSE", {"position_key": "lp:other"}),
    ]
    assert _open_position_cost_basis(events) == Decimal("100")


def test_open_position_cost_basis_no_position_key_still_counted():
    """Opens without position_key cannot be paired — they always
    contribute (the writer hasn't tagged them yet, but the deployed
    capital is real)."""
    events = [_event("LP_OPEN", {"cost_basis_usd": "12.34"})]
    assert _open_position_cost_basis(events) == Decimal("12.34")


def test_open_position_cost_basis_mixed_primitives():
    """LP + SUPPLY + PERP open events sum across types; one closed
    position is excluded."""
    events = [
        _event("LP_OPEN", {"cost_basis_usd": "10", "position_key": "lp:1"}),
        _event("SUPPLY", {"cost_basis_usd": "20", "position_key": "lend:aave-usdc"}),
        _event("PERP_OPEN", {"cost_basis_usd": "30", "position_key": "perp:eth-long"}),
        _event("WITHDRAW", {"position_key": "lend:aave-usdc"}),
    ]
    # LP open ($10) + PERP open ($30) still active; SUPPLY closed = -$20
    assert _open_position_cost_basis(events) == Decimal("40")


# ─── VIB-3914: build_quant_header — wallet-anchored deployed ──────────────


def test_build_quant_header_wallet_anchored_overrides_config_knob():
    """The May 3 2026 contaminated-wallet bug: config says $4 but the
    wallet held $19.31 of pre-existing tokens at first action. The
    header must anchor on the wallet value, not the config knob,
    otherwise PnL = NAV − $4 produces a +381% phantom gain.
    """
    metrics = SimpleNamespace(
        initial_value_usd="4.00",  # the misleading config knob
        deposits_usd="0",
        withdrawals_usd="0",
        total_value_usd="19.27",
        initial_timestamp=datetime.now(tz=UTC).isoformat(),
    )
    snapshots = [
        SimpleNamespace(
            total_value_usd="6.45",
            available_cash_usd="12.86",
            value_confidence="HIGH",
            deployed_capital_usd="6.45",
            positions_json='[{"position_type":"LP"}]',
        ),
    ]
    ledger = [
        _ledger_with_pre_state(
            wallet_balances={"USDC": "8.52", "WETH": "0.0029"},
            prices={"USDC": "1.0001", "WETH": "3720.45"},
        ),
    ]
    events = [_event("LP_OPEN", {"cost_basis_usd": "6.45", "position_key": "lp:1"})]

    h = build_quant_header(
        portfolio_metrics=metrics,
        snapshots=snapshots,
        ledger_entries=ledger,
        accounting_events=events,
    )

    # Wallet-anchored: 8.52*1.0001 + 0.0029*3720.45 = ~19.31 (not $4)
    assert h.deployed_usd > Decimal("19")
    assert h.deployed_usd < Decimal("20")
    # NAV = positions + cash = 6.45 + 12.86 = 19.31
    assert h.nav_usd == Decimal("19.31")
    # PnL is now small (within ε of zero), not +381%
    assert abs(h.lifetime_pnl_pct) < Decimal("5")


def test_build_quant_header_falls_back_to_metrics_when_ledger_lacks_pre_state():
    """If no ledger row carries pre_state (strategy hasn't acted, or
    pre/post-state writer not wired), fall back to the config knob —
    same behaviour as pre-VIB-3914."""
    metrics = SimpleNamespace(
        initial_value_usd="100.00",
        deposits_usd="0",
        withdrawals_usd="0",
        total_value_usd="100.00",
        initial_timestamp=datetime.now(tz=UTC).isoformat(),
    )
    snapshots = [
        SimpleNamespace(
            total_value_usd="0",
            available_cash_usd="100",
            value_confidence="HIGH",
            deployed_capital_usd="0",
            positions_json="[]",
        ),
    ]
    h = build_quant_header(
        portfolio_metrics=metrics,
        snapshots=snapshots,
        ledger_entries=[_ledger()],  # no pre_state
        accounting_events=[],
    )
    assert h.deployed_usd == Decimal("100")


def test_build_quant_header_open_exposure_reconstructed_when_snapshot_zero():
    """VIB-3894 / VIB-3914: snapshot writer leaves deployed_capital=0
    despite an open LP_OPEN event on disk. The header must reconstruct
    open exposure from the accounting events instead of rendering $0
    next to a "Range / in-range" tile."""
    metrics = SimpleNamespace(
        initial_value_usd="20",
        deposits_usd="0",
        withdrawals_usd="0",
        total_value_usd="20",
        initial_timestamp=datetime.now(tz=UTC).isoformat(),
    )
    snapshots = [
        SimpleNamespace(
            total_value_usd="0",
            available_cash_usd="13",
            value_confidence="HIGH",
            deployed_capital_usd="0",  # bug case — writer didn't sum
            positions_json="[]",
        ),
    ]
    events = [_event("LP_OPEN", {"cost_basis_usd": "6.45", "position_key": "lp:1"})]

    h = build_quant_header(
        portfolio_metrics=metrics,
        snapshots=snapshots,
        ledger_entries=[],
        accounting_events=events,
    )
    assert h.deployed_capital_usd == Decimal("6.45")
    assert h.open_position_count == 1


def test_build_quant_header_primary_risk_fallback_from_events():
    """When position_summary is None (PositionSummary RPC empty) but
    accounting_events show an open LP, primary_risk_kind must read 'lp'
    with neutral 'unknown' colouring — never the misleading default
    'Positions N/A'."""
    metrics = SimpleNamespace(
        initial_value_usd="20",
        deposits_usd="0",
        withdrawals_usd="0",
        total_value_usd="20",
        initial_timestamp=datetime.now(tz=UTC).isoformat(),
    )
    snapshots = [
        SimpleNamespace(
            total_value_usd="0",
            available_cash_usd="13",
            value_confidence="HIGH",
            deployed_capital_usd="0",
            positions_json="[]",
        ),
    ]
    events = [_event("LP_OPEN", {"cost_basis_usd": "6.45", "position_key": "lp:1"})]

    h = build_quant_header(
        portfolio_metrics=metrics,
        snapshots=snapshots,
        ledger_entries=[],
        accounting_events=events,
        position_summary=None,
    )
    assert h.primary_risk_kind == "lp"
    assert h.primary_risk_color == "neutral"
    # VIB-3925: empty-state copy — "UNKNOWN" replaced with "pending" so the
    # tile reads as honest (data-not-yet-available) instead of broken.
    assert "pending" in h.primary_risk_value


def _hf_header(hf_value: Decimal):
    """Helper: build a minimal header with a lending PositionSummary at the
    given health factor. All other fields default to neutral so the
    primary-risk-color assertion below isolates the HF-ladder branch."""
    from almanak.framework.dashboard.models import PositionSummary

    metrics = SimpleNamespace(
        initial_value_usd="100",
        deposits_usd="0",
        withdrawals_usd="0",
        total_value_usd="100",
        initial_timestamp=datetime.now(tz=UTC).isoformat(),
        gas_spent_usd="0",
    )
    return build_quant_header(
        portfolio_metrics=metrics,
        snapshots=[],
        ledger_entries=[],
        accounting_events=[],
        position_summary=PositionSummary(health_factor=hf_value),
    )


def test_vib3924_hf_ladder_green_above_1_5():
    """HF ≥ 1.5 → green tile (safe Aave/Morpho zone)."""
    h = _hf_header(Decimal("1.50"))
    assert h.primary_risk_kind == "lending"
    assert h.primary_risk_color == "green"
    h2 = _hf_header(Decimal("2.78"))
    assert h2.primary_risk_color == "green"


def test_vib3924_hf_ladder_yellow_between_1_2_and_1_5():
    """HF in [1.2, 1.5) → yellow tile (caution zone)."""
    h = _hf_header(Decimal("1.49"))
    assert h.primary_risk_color == "yellow"
    h2 = _hf_header(Decimal("1.20"))
    assert h2.primary_risk_color == "yellow"


def test_vib3924_hf_ladder_red_below_1_2():
    """HF < 1.2 → red tile (liquidation imminent)."""
    h = _hf_header(Decimal("1.19"))
    assert h.primary_risk_color == "red"
    h2 = _hf_header(Decimal("1.05"))
    assert h2.primary_risk_color == "red"


def test_vib3924_hf_ladder_no_debt_neutral():
    """HF==0 (no debt) → neutral tile, no caution colour applied."""
    h = _hf_header(Decimal("0"))
    assert h.primary_risk_value == "no debt"
    assert h.primary_risk_color == "neutral"
