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
    PnLSummary,
    _apply_primary_risk_gauge,
    _detect_primitive,
    _drawdowns,
    _open_position_cost_basis,
    _wallet_value_at_first_action,
    build_quant_header,
    compute_audit_trail,
    compute_cost_stack,
    compute_inventory_unrealized,
    compute_pnl_summary,
    compute_reconciliation,
    lifetime_drawdowns_from_nav_text,
)
from almanak.framework.valuation.net_debt import (
    net_debt_from_positions_json,
    net_debt_from_snapshot,
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


# ─── VIB-4979: native-gas symmetry between Deployed and NAV ────────────────


def _snapshot(
    *,
    total_value_usd: str,
    available_cash_usd: str,
    positions_json: str = "[]",
) -> SimpleNamespace:
    """Minimal portfolio_snapshot stand-in for compute_pnl_summary."""
    return SimpleNamespace(
        total_value_usd=total_value_usd,
        available_cash_usd=available_cash_usd,
        value_confidence="HIGH",
        deployed_capital_usd="0",
        positions_json=positions_json,
        timestamp=datetime.now(tz=UTC),
    )


def test_lifetime_pnl_zero_when_wallet_is_only_native_gas():
    """VIB-4979 regression: a wallet holding ONLY the chain's native gas
    token, with no positions and no trades beyond capturing it, must read
    ~$0 lifetime PnL — not +gas.

    Before the fix the Deployed anchor (pre_state_json.wallet_balances)
    excluded native gas while NAV (available_cash_usd) included it, so
    lifetime_pnl = NAV − Deployed inherited the entire gas balance as
    phantom profit (~+26% observed live on lp_triple Arbitrum). Now that
    snapshot_balances_for_intent captures native gas into the pre-state,
    the two bases share the same token universe and the phantom collapses.
    """
    # Pre-state now carries ETH (native) — the symmetric universe.
    deployed_ledger = _ledger_with_pre_state(
        wallet_balances={"USDC": "0", "ETH": "0.002"},
        prices={"USDC": "1", "ETH": "2440"},
    )
    # NAV: no open positions, available cash == the native-gas value.
    eth_value = Decimal("0.002") * Decimal("2440")  # $4.88
    snap = _snapshot(total_value_usd="0", available_cash_usd=str(eth_value))

    pnl = compute_pnl_summary(
        portfolio_metrics=None,
        snapshots=[snap],
        ledger_entries=[deployed_ledger],
        accounting_events=[],
    )

    # Deployed and NAV are both ~$4.88 → lifetime PnL is ~$0, NOT +$4.88.
    assert pnl.deployed_usd == eth_value
    assert pnl.nav_usd == eth_value
    assert pnl.lifetime_pnl_usd == Decimal("0")
    assert pnl.net_apr_pct == Decimal("0")


def test_lifetime_pnl_phantom_gas_when_deployed_excludes_native_gas():
    """Pin the OLD-broken shape as the failure it was: when the pre-state
    omits native gas but NAV includes it, lifetime PnL equals the gas
    balance. This is the asymmetry the fix removes — kept as a contrast
    guard so a future regression that drops native gas from the pre-state
    is caught by the symmetric test above flipping while this one stays
    consistent with its (intentionally asymmetric) inputs."""
    deployed_ledger = _ledger_with_pre_state(
        wallet_balances={"USDC": "0"},  # native gas MISSING (pre-fix bug)
        prices={"USDC": "1", "ETH": "2440"},
    )
    eth_value = Decimal("0.002") * Decimal("2440")
    snap = _snapshot(total_value_usd="0", available_cash_usd=str(eth_value))

    pnl = compute_pnl_summary(
        portfolio_metrics=None,
        snapshots=[snap],
        ledger_entries=[deployed_ledger],
        accounting_events=[],
    )

    # Deployed sees $0 (all-zero balances → helper returns None → falls back
    # to portfolio_metrics, which is None → deployed stays 0), NAV sees
    # $4.88 → the entire gas balance shows as phantom PnL.
    assert pnl.deployed_usd == Decimal("0")
    assert pnl.nav_usd == eth_value
    assert pnl.lifetime_pnl_usd == eth_value  # phantom — the defect


# ─── VIB-4983: debt-netted NAV for open leveraged-lending positions ───────


def test_net_debt_from_positions_json_sums_negative_legs():
    """The helper returns (count, Σ|negative value_usd|, Σ|cost_basis of those
    debt legs|) — the BORROW mark and cost — and ignores positive legs
    (collateral) for both debt totals."""
    raw = json.dumps(
        [
            {"position_type": "SUPPLY", "value_usd": "6.75", "cost_basis_usd": "6.75"},
            {"position_type": "BORROW", "value_usd": "-1.56", "cost_basis_usd": "1.55"},
        ]
    )
    count, debt_mark, debt_cost = net_debt_from_positions_json(raw)
    assert count == 2
    assert debt_mark == Decimal("1.56")
    assert debt_cost == Decimal("1.55")


def test_net_debt_from_positions_json_no_debt_is_zero():
    """A position set with no negative leg (LP / swap / single-supply) nets
    zero debt — the byte-identical guard for non-leveraged strategies."""
    raw = json.dumps(
        [
            {"position_type": "LP", "value_usd": "12.34", "cost_basis_usd": "12.00"},
            {"position_type": "SUPPLY", "value_usd": "5.00", "cost_basis_usd": "5.00"},
        ]
    )
    count, debt_mark, debt_cost = net_debt_from_positions_json(raw)
    assert count == 2
    assert debt_mark == Decimal("0")
    assert debt_cost == Decimal("0")


def test_net_debt_from_positions_json_unmeasured_is_skipped():
    """Empty≠Zero: an absent/unparsable value_usd is unmeasured and skipped,
    never coerced to a measured zero (and never crashes the helper)."""
    raw = json.dumps(
        [
            {"position_type": "BORROW", "value_usd": "-2.00", "cost_basis_usd": "2.00"},
            {"position_type": "BORROW", "value_usd": None},
            {"position_type": "BORROW", "value_usd": ""},
            {"position_type": "BORROW"},  # missing key
            {"position_type": "BORROW", "value_usd": "not-a-number"},
            "malformed-non-dict",
        ]
    )
    count, debt_mark, debt_cost = net_debt_from_positions_json(raw)
    assert count == 6
    assert debt_mark == Decimal("2.00")
    assert debt_cost == Decimal("2.00")


def test_net_debt_from_positions_json_debt_leg_missing_cost_is_skipped():
    """Empty≠Zero asymmetry: a measured-negative leg whose cost_basis_usd is
    absent/unparsable still nets its mark (the liability is real) but is skipped
    for the cost total (fabricating a 0 cost would over-net deployed capital)."""
    raw = json.dumps(
        [
            {"position_type": "BORROW", "value_usd": "-3.00"},  # no cost_basis_usd
            {"position_type": "BORROW", "value_usd": "-1.00", "cost_basis_usd": ""},
            {"position_type": "BORROW", "value_usd": "-1.00", "cost_basis_usd": "x"},
            {"position_type": "BORROW", "value_usd": "-2.00", "cost_basis_usd": "1.90"},
        ]
    )
    count, debt_mark, debt_cost = net_debt_from_positions_json(raw)
    assert count == 4
    assert debt_mark == Decimal("7.00")  # all four measured-negative marks
    assert debt_cost == Decimal("1.90")  # only the one measured cost


def test_net_debt_from_positions_json_malformed_payload_is_zero():
    """A malformed / empty / non-list-non-dict payload yields (0, 0, 0) without
    raising."""
    assert net_debt_from_positions_json(None) == (0, Decimal("0"), Decimal("0"))
    assert net_debt_from_positions_json("") == (0, Decimal("0"), Decimal("0"))
    assert net_debt_from_positions_json("not json") == (0, Decimal("0"), Decimal("0"))


def test_net_debt_from_positions_json_accepts_preparsed_payload():
    """VIB-4983 (Gemini review): hosted Postgres JSON/JSONB columns (and some
    test mocks) hand back an ALREADY-deserialized list/dict. json.loads on it
    would TypeError → the (0, 0, 0) bypass → debt-netting silently skipped and
    the phantom loss resurrected on the hosted path. The helper must net the
    debt from a pre-parsed payload identically to the JSON-string path."""
    positions = [
        {"position_type": "SUPPLY", "value_usd": "6.75", "cost_basis_usd": "6.75"},
        {"position_type": "BORROW", "value_usd": "-1.56", "cost_basis_usd": "1.55"},
    ]
    # Pre-parsed bare list (not json.dumps'd).
    assert net_debt_from_positions_json(positions) == (2, Decimal("1.56"), Decimal("1.55"))
    # Pre-parsed VIB-3923 envelope dict.
    assert net_debt_from_positions_json({"schema_version": 1, "positions": positions}) == (
        2,
        Decimal("1.56"),
        Decimal("1.55"),
    )
    assert net_debt_from_positions_json(json.dumps({"no": "positions"})) == (
        0,
        Decimal("0"),
        Decimal("0"),
    )


# ─── VIB-4983 follow-up: cost-basis + drawdown debt-netting consistency ─────


# Real Arbitrum-mainnet looping snapshot that rendered the phantom −28.89% on
# the dashboard (quant-user run 20260616-0945). Collateral USDC supply, USDT
# borrow, plus a dust swap-inventory token. Net equity is ~flat; the gross
# Open-cost-basis column double-counts the borrow as a positive asset.
_LEVERAGE_LOOP_POSITIONS = json.dumps(
    [
        {"position_type": "SUPPLY", "value_usd": "9.77047300", "cost_basis_usd": "9.77036500"},
        {"position_type": "BORROW", "value_usd": "-3.90821500", "cost_basis_usd": "3.90814600"},
        {"position_type": "TOKEN", "value_usd": "0.02798400", "cost_basis_usd": "0.02798400"},
    ]
)
# Writer convention (portfolio_valuer.py): total_value_usd EXCLUDES the negative
# borrow leg (= 9.77047300 + 0.02798400); deployed_capital_usd = Σ abs(cost) so it
# INCLUDES the borrow cost as a positive term (= 9.77036500 + 3.90814600 + 0.02798400).
_LEVERAGE_TOTAL_VALUE = "9.79845700"
_LEVERAGE_DEPLOYED_CAPITAL = "13.70649500"
_LEVERAGE_CASH = "2.59891741"


def _leverage_snapshot() -> SimpleNamespace:
    return SimpleNamespace(
        total_value_usd=_LEVERAGE_TOTAL_VALUE,
        available_cash_usd=_LEVERAGE_CASH,
        value_confidence="HIGH",
        deployed_capital_usd=_LEVERAGE_DEPLOYED_CAPITAL,
        positions_json=_LEVERAGE_LOOP_POSITIONS,
        timestamp=datetime.now(tz=UTC),
    )


def test_compute_pnl_summary_nets_cost_basis_for_open_leverage_loop():
    """VIB-4983 follow-up: an OPEN leveraged-lending loop must read a debt-netted
    Open cost basis, not the gross Σ abs(cost) column — otherwise Strategy PnL
    (open NAV − cost basis) reads netted-NAV minus gross-cost = a phantom −debt
    loss on a flat position. Verified against the real −28.89% snapshot: the
    headline phantom was −$7.82; after netting both sides the position is flat."""
    pnl = compute_pnl_summary(
        portfolio_metrics=None,
        snapshots=[_leverage_snapshot()],
        ledger_entries=[],
        accounting_events=[],
    )
    # NAV nets the debt mark ONCE (total_value_usd excluded it): 9.79845700 - 3.90821500.
    assert pnl.nav_usd == Decimal("8.48915941")
    # Open cost basis = net equity cost computed directly from the legs
    # (collateral cost + token cost − borrow cost) = 9.77036500 + 0.02798400 − 3.90814600.
    assert pnl.deployed_capital_usd == Decimal("5.89020300")
    # Strategy-PnL building blocks (mirrors _detail_header._strategy_pnl_usd):
    open_position_nav = pnl.nav_usd - pnl.available_cash_usd
    unrealized = open_position_nav - pnl.deployed_capital_usd
    assert abs(unrealized) < Decimal("0.01")  # flat, NOT the −$7.82 phantom


def test_compute_pnl_summary_non_leveraged_cost_basis_unchanged():
    """Byte-identity guard: a position set with no negative leg leaves the
    Open-cost-basis column exactly as the writer persisted it (no spurious
    netting for LP / swap / single-supply)."""
    snap = SimpleNamespace(
        total_value_usd="17.00",
        available_cash_usd="3.00",
        value_confidence="HIGH",
        deployed_capital_usd="16.00",
        positions_json=json.dumps(
            [
                {"position_type": "LP", "value_usd": "12.00", "cost_basis_usd": "11.50"},
                {"position_type": "SUPPLY", "value_usd": "5.00", "cost_basis_usd": "4.50"},
            ]
        ),
        timestamp=datetime.now(tz=UTC),
    )
    pnl = compute_pnl_summary(portfolio_metrics=None, snapshots=[snap], ledger_entries=[], accounting_events=[])
    assert pnl.nav_usd == Decimal("20.00")  # 17 + 3, debt 0
    assert pnl.deployed_capital_usd == Decimal("16.00")  # untouched


def test_drawdowns_net_debt_across_leverage_lifecycle():
    """VIB-4983 follow-up: the recent-window drawdown must net the borrow leg
    per-snapshot. Un-netted, wallet NAV phantom-spikes up by the borrow when the
    loop opens and collapses at teardown, manufacturing a large lifecycle
    drawdown on a flat equity loop. Netted, the equity series is ~flat."""
    pre_open = SimpleNamespace(
        total_value_usd="0",
        available_cash_usd="8.50",
        positions_json="[]",
        timestamp=datetime.now(tz=UTC),
    )
    opened = _leverage_snapshot()  # net equity ≈ 8.49, gross collateral+cash ≈ 12.40
    torn_down = SimpleNamespace(
        total_value_usd="0",
        available_cash_usd="8.49",
        positions_json="[]",
        timestamp=datetime.now(tz=UTC),
    )
    max_dd, _current = _drawdowns([pre_open, opened, torn_down])
    # Netted equity stays ~8.5 throughout → negligible drawdown. The un-netted
    # series would peak at ~12.4 (open) and fall to ~8.5 (teardown) ≈ 31% phantom.
    assert max_dd < Decimal("1.0")


def _real_leverage_snapshot():
    """A REAL ``PortfolioSnapshot`` (the production shape from
    ``StateManager.get_recent_snapshots``) — typed ``positions``, NO
    ``positions_json`` attribute. This is the object the dashboard actually
    receives; the prior ``getattr(snap, "positions_json")`` read returned None on
    it, silently no-op'ing the netting in production (the inert-feature bug)."""
    from almanak.framework.portfolio.models import PortfolioSnapshot

    def _p(t, v, c):
        return {
            "position_type": t,
            "protocol": "aave_v3",
            "chain": "arbitrum",
            "value_usd": v,
            "cost_basis_usd": c,
            "label": t,
            "tokens": [],
            "details": {},
        }

    return PortfolioSnapshot.from_dict(
        {
            "timestamp": datetime.now(tz=UTC).isoformat(),
            "deployment_id": "d",
            "total_value_usd": _LEVERAGE_TOTAL_VALUE,
            "available_cash_usd": _LEVERAGE_CASH,
            "deployed_capital_usd": _LEVERAGE_DEPLOYED_CAPITAL,
            "wallet_total_value_usd": "12.4",
            "value_confidence": "HIGH",
            "positions": [
                _p("SUPPLY", "9.77047300", "9.77036500"),
                _p("BORROW", "-3.90821500", "3.90814600"),
                _p("TOKEN", "0.02798400", "0.02798400"),
            ],
        }
    )


def test_compute_pnl_summary_nets_real_portfolio_snapshot():
    """REGRESSION (the inert-feature bug): a real ``PortfolioSnapshot`` carries a
    typed ``positions`` list and has NO ``positions_json`` attribute. The netting
    MUST activate off ``.positions`` — reading ``positions_json`` returned None
    and made VIB-4983's NAV netting (and the cost/PnL netting) silently no-op in
    production. Verified against the live −28.89% looping snapshot."""
    snap = _real_leverage_snapshot()
    assert getattr(snap, "positions_json", None) is None  # the trap this test guards
    pnl = compute_pnl_summary(portfolio_metrics=None, snapshots=[snap], ledger_entries=[], accounting_events=[])
    assert pnl.nav_usd == Decimal("8.48915941")  # netted, NOT 12.40 gross
    assert pnl.deployed_capital_usd == Decimal("5.89020300")  # net equity, NOT 13.71 gross
    open_position_nav = pnl.nav_usd - pnl.available_cash_usd
    assert abs(open_position_nav - pnl.deployed_capital_usd) < Decimal("0.01")  # flat, not −7.82


def test_lifetime_pnl_excludes_borrowed_amount_for_lending_loop():
    """REGRESSION (ALM-2789): the "Wallet lifetime PnL" tile must NOT count the
    borrowed principal as profit. ``lifetime_pnl_usd = wallet_nav − deployed`` and
    ``wallet_nav`` reads ``total_value_usd`` (positive-position-scoped — the BORROW
    leg is dropped) plus cash, so without debt-netting an open lending loop reads a
    phantom GAIN equal to the borrowed amount.

    Anchor the deployed capital to the loop's net equity (a flat loop) and assert
    lifetime PnL is ~$0 — NOT the +debt_mark the un-netted surface used to report.
    """
    snap = _real_leverage_snapshot()
    # Net equity NAV = 8.48915941 (proven netted in the sibling tests). Anchor
    # deployed capital to that same equity so the loop is flat end-to-end.
    metrics = SimpleNamespace(
        initial_value_usd="8.48915941",
        deposits_usd="0",
        withdrawals_usd="0",
        initial_timestamp=None,
    )
    pnl = compute_pnl_summary(portfolio_metrics=metrics, snapshots=[snap], ledger_entries=[], accounting_events=[])
    assert pnl.deployed_usd == Decimal("8.48915941")
    assert pnl.nav_usd == Decimal("8.48915941")  # debt-netted, NOT 12.39737441 gross
    # Flat loop → ~$0 lifetime PnL. The un-netted surface would read
    # 12.39737441 − 8.48915941 = +3.90821500 (exactly the borrowed amount).
    assert pnl.lifetime_pnl_usd == Decimal("0")
    assert pnl.lifetime_pnl_usd != Decimal("3.90821500")  # the borrowed-amount phantom


def test_net_debt_from_snapshot_reads_typed_positions():
    """``net_debt_from_snapshot`` prefers the typed ``positions`` list and returns the
    debt mark, debt cost, and the signed net equity cost computed directly."""
    snap = _real_leverage_snapshot()
    count, debt_mark, debt_cost, net_cost = net_debt_from_snapshot(snap)
    assert count == 3
    assert debt_mark == Decimal("3.90821500")
    assert debt_cost == Decimal("3.90814600")
    # net equity cost = collateral cost + token cost − borrow cost
    assert net_cost == Decimal("9.77036500") + Decimal("0.02798400") - Decimal("3.90814600")


def test_cost_basis_netting_correct_when_column_zero_reconstruction_path():
    """REGRESSION (Codex/pr-auditor P2): when the snapshot's deployed_capital_usd
    column is 0 the basis is rebuilt from accounting events (collateral-only, no
    BORROW term). The old ``-= 2×debt_cost`` would under-net by a debt leg on that
    path, flipping a flat loop to a phantom GAIN. Computing the net equity cost
    DIRECTLY from the position legs is source-independent, so the leverage loop
    reads flat whether the basis came from the gross column or the reconstruction."""
    from almanak.framework.portfolio.models import PortfolioSnapshot

    def _p(t, v, c):
        return {
            "position_type": t,
            "protocol": "aave_v3",
            "chain": "arbitrum",
            "value_usd": v,
            "cost_basis_usd": c,
            "label": t,
            "tokens": [],
            "details": {},
        }

    snap = PortfolioSnapshot.from_dict(
        {
            "timestamp": datetime.now(tz=UTC).isoformat(),
            "deployment_id": "d",
            "total_value_usd": _LEVERAGE_TOTAL_VALUE,
            "available_cash_usd": _LEVERAGE_CASH,
            "deployed_capital_usd": "0",  # force the reconstruction fallback
            "wallet_total_value_usd": "12.4",
            "value_confidence": "HIGH",
            "positions": [
                _p("SUPPLY", "9.77047300", "9.77036500"),
                _p("BORROW", "-3.90821500", "3.90814600"),
                _p("TOKEN", "0.02798400", "0.02798400"),
            ],
        }
    )
    pnl = compute_pnl_summary(portfolio_metrics=None, snapshots=[snap], ledger_entries=[], accounting_events=[])
    # Net equity cost from the legs directly — not under-netted, not negative.
    assert pnl.deployed_capital_usd == Decimal("5.89020300")
    open_position_nav = pnl.nav_usd - pnl.available_cash_usd
    assert abs(open_position_nav - pnl.deployed_capital_usd) < Decimal("0.01")  # flat


def test_lifetime_drawdown_nets_borrow_leg_across_open_teardown():
    """REGRESSION (Codex finding): the lifetime drawdown is PREFERRED over the
    recent window on the main PnL surface, and reads ``get_nav_series`` text rows
    that now carry ``positions_json`` (5th element). Without per-row netting the
    series phantom-spikes by the borrow at open and collapses at teardown,
    reporting a large drawdown on a flat leverage loop. Rows are
    ``(ts, total, cash, id, positions_json)`` oldest-first."""
    leverage_pj = _LEVERAGE_LOOP_POSITIONS
    base = datetime(2026, 1, 1, tzinfo=UTC)
    rows = [
        (base, "0", "8.50", 1, "[]"),  # pre-open: cash only
        (base.replace(hour=1), _LEVERAGE_TOTAL_VALUE, _LEVERAGE_CASH, 2, leverage_pj),  # open loop
        (base.replace(hour=2), "0", "8.49", 3, "[]"),  # torn down: cash ~equity
    ]
    max_dd, _current = lifetime_drawdowns_from_nav_text(rows)
    # Netted equity stays ~8.5 throughout → small drawdown. Un-netted the open row
    # would read ~12.4 (Codex measured ~31.5% phantom). Assert it is netted.
    assert max_dd < Decimal("1.0")


# ─── VIB-4983: _apply_primary_risk_gauge extraction — branch coverage ──────
# Direct tests for the helper extracted out of compute_pnl_summary so the
# primary-risk tile branches are covered at the unit level (behaviour is
# byte-identical to the inline block it replaced).


def _lp_position_summary(in_range):
    from almanak.framework.dashboard.models import LPPosition, PositionSummary

    return PositionSummary(
        lp_positions=[
            LPPosition(
                pool="WETH/USDC",
                token0="WETH",
                token1="USDC",
                liquidity_usd=Decimal("100"),
                range_lower=Decimal("1800"),
                range_upper=Decimal("2200"),
                current_price=Decimal("2000"),
                in_range=in_range,
            )
        ]
    )


def test_primary_risk_gauge_lp_in_range_yes():
    pnl = PnLSummary()
    _apply_primary_risk_gauge(pnl, _lp_position_summary(True), [])
    assert pnl.primary_risk_kind == "lp"
    assert pnl.primary_risk_value == "in-range YES"
    assert pnl.primary_risk_color == "green"


def test_primary_risk_gauge_lp_in_range_no():
    pnl = PnLSummary()
    _apply_primary_risk_gauge(pnl, _lp_position_summary(False), [])
    assert pnl.primary_risk_value == "in-range NO"
    assert pnl.primary_risk_color == "red"


def test_primary_risk_gauge_lp_in_range_pending():
    pnl = PnLSummary()
    _apply_primary_risk_gauge(pnl, _lp_position_summary(None), [])
    assert pnl.primary_risk_value == "in-range pending"
    assert pnl.primary_risk_color == "neutral"


def test_primary_risk_gauge_perp_leverage():
    from almanak.framework.dashboard.models import PositionSummary

    pnl = PnLSummary()
    _apply_primary_risk_gauge(pnl, PositionSummary(leverage=Decimal("2.5")), [])
    assert pnl.primary_risk_kind == "perp"
    assert pnl.primary_risk_label == "Leverage"
    assert pnl.primary_risk_value == "2.50×"
    assert pnl.primary_risk_color == "neutral"


def test_primary_risk_gauge_none_position_summary_no_change():
    pnl = PnLSummary()
    _apply_primary_risk_gauge(pnl, None, [])
    assert pnl.primary_risk_kind == "none"


def test_primary_risk_gauge_fallback_lending_from_events():
    pnl = PnLSummary()
    pnl.deployed_capital_usd = Decimal("5")
    _apply_primary_risk_gauge(pnl, None, [_event("SUPPLY"), _event("BORROW")])
    assert pnl.primary_risk_kind == "lending"
    assert pnl.primary_risk_value == "unknown"
    assert pnl.primary_risk_color == "neutral"


def test_primary_risk_gauge_fallback_perp_from_events():
    pnl = PnLSummary()
    pnl.deployed_capital_usd = Decimal("5")
    _apply_primary_risk_gauge(pnl, None, [_event("PERP_OPEN")])
    assert pnl.primary_risk_kind == "perp"
    assert pnl.primary_risk_value == "unknown"


def test_primary_risk_gauge_fallback_skipped_when_no_deployed_capital():
    pnl = PnLSummary()
    # deployed_capital_usd defaults to 0 → fallback must not fire.
    _apply_primary_risk_gauge(pnl, None, [_event("LP_OPEN")])
    assert pnl.primary_risk_kind == "none"


def test_pnl_summary_open_leverage_loop_nets_debt():
    """VIB-4983 regression: an OPEN USDC/USDT Aave leverage loop must read a
    debt-netted NAV (collateral − debt + cash), so Strategy PnL is ~flat and
    not −debt.

    Models the live looping-mainnet failure: collateral SUPPLY +$6.75 and
    BORROW −$1.56 both ride in positions_json, but total_value_usd is
    positive-position-scoped (VIB-3614) so it equals only the collateral
    ($6.75) and drops the debt. Pre-fix nav_usd = 6.75 + cash overstated by
    $1.56 → Strategy PnL read ≈ −$1.56 (the phantom leverage loss). Post-fix
    nav_usd nets the debt: 6.75 − 1.56 + cash.
    """
    positions_json = json.dumps(
        [
            {"position_type": "SUPPLY", "value_usd": "6.75"},
            {"position_type": "BORROW", "value_usd": "-1.56"},
        ]
    )
    # total_value_usd is positive-scoped → collateral only; cash is the small
    # residual equity sitting in the wallet.
    snap = _snapshot(
        total_value_usd="6.75",
        available_cash_usd="0.40",
        positions_json=positions_json,
    )
    # Deployed baseline = the equity the operator actually put in (≈ $5.19 of
    # net collateral − debt at open). Model via portfolio_metrics initial.
    metrics = SimpleNamespace(
        deposits_usd="0",
        withdrawals_usd="0",
        initial_value_usd="5.19",
        initial_timestamp=datetime.now(tz=UTC).isoformat(),
    )

    pnl = compute_pnl_summary(
        portfolio_metrics=metrics,
        snapshots=[snap],
        ledger_entries=[],
        accounting_events=[],
    )

    # NAV is debt-netted: 6.75 − 1.56 + 0.40 = 5.59 (NOT 6.75 + 0.40 = 7.15).
    assert pnl.nav_usd == Decimal("5.59")
    # Strategy PnL ≈ flat against the $5.19 deployed equity (+$0.40 of measured
    # cash residual), NOT −$1.56. The un-netted phantom loss is gone.
    assert pnl.deployed_usd == Decimal("5.19")
    assert pnl.lifetime_pnl_usd == Decimal("0.40")
    assert pnl.open_position_count == 2


def test_pnl_summary_non_leveraged_unchanged_by_debt_netting():
    """A non-leveraged strategy (single-supply / LP / swap — no negative leg)
    produces output IDENTICAL to a snapshot with no positions_json debt: the
    debt-netting subtracts Decimal('0') and nav_usd is unchanged."""
    no_debt_positions = json.dumps(
        [
            {"position_type": "LP", "value_usd": "12.00"},
            {"position_type": "SUPPLY", "value_usd": "5.00"},
        ]
    )
    snap_with_positions = _snapshot(
        total_value_usd="17.00",
        available_cash_usd="3.00",
        positions_json=no_debt_positions,
    )
    snap_baseline = _snapshot(total_value_usd="17.00", available_cash_usd="3.00")

    metrics = SimpleNamespace(
        deposits_usd="0",
        withdrawals_usd="0",
        initial_value_usd="20.00",
        initial_timestamp=datetime.now(tz=UTC).isoformat(),
    )

    pnl_positions = compute_pnl_summary(
        portfolio_metrics=metrics,
        snapshots=[snap_with_positions],
        ledger_entries=[],
        accounting_events=[],
    )
    pnl_baseline = compute_pnl_summary(
        portfolio_metrics=metrics,
        snapshots=[snap_baseline],
        ledger_entries=[],
        accounting_events=[],
    )

    # NAV = 17.00 + 3.00 = 20.00 either way; debt-netting changed nothing.
    assert pnl_positions.nav_usd == Decimal("20.00")
    assert pnl_positions.nav_usd == pnl_baseline.nav_usd
    assert pnl_positions.lifetime_pnl_usd == pnl_baseline.lifetime_pnl_usd
    assert pnl_positions.deployed_usd == pnl_baseline.deployed_usd


def test_pnl_summary_debt_netting_handles_envelope_shape():
    """The VIB-3923 envelope ({schema_version, positions, metadata, ...}) must
    be unwrapped so the BORROW leg is netted on enveloped writes, not only on
    the legacy bare-list shape."""
    enveloped = json.dumps(
        {
            "schema_version": 1,
            "positions": [
                {"position_type": "SUPPLY", "value_usd": "6.75"},
                {"position_type": "BORROW", "value_usd": "-1.56"},
            ],
            "metadata": {},
            "reconciliation": {},
        }
    )
    snap = _snapshot(
        total_value_usd="6.75",
        available_cash_usd="0.40",
        positions_json=enveloped,
    )

    pnl = compute_pnl_summary(
        portfolio_metrics=None,
        snapshots=[snap],
        ledger_entries=[],
        accounting_events=[],
    )

    assert pnl.nav_usd == Decimal("5.59")
    assert pnl.open_position_count == 2


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


# ---------------------------------------------------------------------------
# VIB-4984: compute_inventory_unrealized — held swap-inventory mark-to-market
# ---------------------------------------------------------------------------


def _swap_event(
    *,
    deployment_id: str,
    chain: str = "arbitrum",
    wallet: str = "0xwallet",
    token_in: str,
    amount_in: str,
    token_out: str,
    amount_out: str,
    amount_out_usd: str,
    timestamp: str = "2026-06-01T00:00:00+00:00",
) -> dict:
    """Build a raw accounting_events SWAP row (the shape FIFOBasisStore replays)."""
    return {
        "event_type": "SWAP",
        "deployment_id": deployment_id,
        "position_key": f"swap:{chain}:{wallet}",
        "chain": chain,
        "wallet_address": wallet,
        "timestamp": timestamp,
        "payload_json": json.dumps(
            {
                "token_in": token_in,
                "amount_in": amount_in,
                "token_out": token_out,
                "amount_out": amount_out,
                "amount_out_usd": amount_out_usd,
            }
        ),
    }


def _price(chain: str, address: str, symbol: str, price_usd: str) -> dict:
    """A portfolio_snapshots.token_prices entry: {chain:address: {price_usd, symbol}}."""
    return {f"{chain}:{address}": {"symbol": symbol, "price_usd": price_usd}}


_DEP = "Strat:abc"


def test_inventory_unrealized_no_inventory_returns_none():
    # All acquired inventory disposed (net-flat round-trip leaves no residual).
    events = [
        _swap_event(
            deployment_id=_DEP,
            token_in="USDC",
            amount_in="2000",
            token_out="WETH",
            amount_out="1.0",
            amount_out_usd="2000",
        ),
        _swap_event(
            deployment_id=_DEP,
            token_in="WETH",
            amount_in="1.0",
            token_out="USDC",
            amount_out="2000",
            amount_out_usd="2000",
        ),
    ]
    prices = _price("arbitrum", "0xweth", "WETH", "2000")
    assert compute_inventory_unrealized(events, _DEP, prices) is None


def test_inventory_unrealized_empty_events_returns_none():
    assert compute_inventory_unrealized([], _DEP, {}) is None


def test_inventory_unrealized_trending_gain_positive():
    # Net-long 1 WETH at cost 2000; mark 2100 → +100.
    events = [
        _swap_event(
            deployment_id=_DEP,
            token_in="USDC",
            amount_in="2000",
            token_out="WETH",
            amount_out="1.0",
            amount_out_usd="2000",
        ),
    ]
    prices = _price("arbitrum", "0xweth", "WETH", "2100")
    result = compute_inventory_unrealized(events, _DEP, prices)
    assert result == Decimal("100")


def test_inventory_unrealized_live_rsi_trending_loss():
    # Live RSI shape: net-long ~0.0064 WETH, FIFO cost 11.9105, mark 11.9067.
    # Acquire 0.0064 WETH for 11.9105 USD; price = 11.9067 / 0.0064.
    events = [
        _swap_event(
            deployment_id=_DEP,
            token_in="USDC",
            amount_in="12",
            token_out="WETH",
            amount_out="0.0064",
            amount_out_usd="11.9105",
        ),
    ]
    mark = Decimal("11.9067") / Decimal("0.0064")
    prices = _price("arbitrum", "0xweth", "WETH", str(mark))
    result = compute_inventory_unrealized(events, _DEP, prices)
    # 0.0064 * mark - 11.9105 = 11.9067 - 11.9105 = -0.0038
    assert result is not None
    assert abs(result - Decimal("-0.0038")) < Decimal("1e-9")


def test_inventory_unrealized_multi_token_sums():
    # Two net-long legs: WETH (+100) and WBTC (-50).
    events = [
        _swap_event(
            deployment_id=_DEP,
            token_in="USDC",
            amount_in="2000",
            token_out="WETH",
            amount_out="1.0",
            amount_out_usd="2000",
        ),
        _swap_event(
            deployment_id=_DEP,
            token_in="USDC",
            amount_in="1000",
            token_out="WBTC",
            amount_out="0.02",
            amount_out_usd="1000",
        ),
    ]
    prices = {}
    prices.update(_price("arbitrum", "0xweth", "WETH", "2100"))  # +100
    prices.update(_price("arbitrum", "0xwbtc", "WBTC", "47500"))  # 0.02*47500=950 -> -50
    result = compute_inventory_unrealized(events, _DEP, prices)
    assert result == Decimal("50")  # +100 + (-50)


def test_inventory_unrealized_shared_wallet_isolation():
    # Two deployments trading the same wallet — each marks only its own lots.
    other = "Other:zzz"
    events = [
        _swap_event(
            deployment_id=_DEP,
            token_in="USDC",
            amount_in="2000",
            token_out="WETH",
            amount_out="1.0",
            amount_out_usd="2000",
        ),
        _swap_event(
            deployment_id=other,
            token_in="USDC",
            amount_in="9000",
            token_out="WETH",
            amount_out="5.0",
            amount_out_usd="9000",
        ),
    ]
    prices = _price("arbitrum", "0xweth", "WETH", "2100")
    # _DEP: 1 WETH, cost 2000, mark 2100 → +100 (NOT affected by other's 5 WETH).
    assert compute_inventory_unrealized(events, _DEP, prices) == Decimal("100")


def test_inventory_unrealized_missing_deployment_id_fails_closed():
    # Without a deployment_id the events cannot be scoped to this strategy;
    # summing a shared wallet's full stream would leak a co-located strategy's
    # inventory. Fail closed (None ⇒ "—"), NOT an unscoped sum. (CodeRabbit)
    other = "Other:zzz"
    events = [
        _swap_event(
            deployment_id=_DEP,
            token_in="USDC",
            amount_in="2000",
            token_out="WETH",
            amount_out="1.0",
            amount_out_usd="2000",
        ),
        _swap_event(
            deployment_id=other,
            token_in="USDC",
            amount_in="9000",
            token_out="WETH",
            amount_out="5.0",
            amount_out_usd="9000",
        ),
    ]
    prices = _price("arbitrum", "0xweth", "WETH", "2100")
    assert compute_inventory_unrealized(events, "", prices) is None
    assert compute_inventory_unrealized(events, None, prices) is None  # type: ignore[arg-type]


def test_inventory_unrealized_missing_basis_returns_none():
    # Held lot has cost_usd=None (amount_out_usd absent) → whole term None.
    events = [
        {
            "event_type": "SWAP",
            "deployment_id": _DEP,
            "position_key": "swap:arbitrum:0xwallet",
            "chain": "arbitrum",
            "wallet_address": "0xwallet",
            "timestamp": "2026-06-01T00:00:00+00:00",
            "payload_json": json.dumps(
                {"token_in": "USDC", "amount_in": "2000", "token_out": "WETH", "amount_out": "1.0"}
            ),
        }
    ]
    prices = _price("arbitrum", "0xweth", "WETH", "2100")
    assert compute_inventory_unrealized(events, _DEP, prices) is None


def test_inventory_unrealized_missing_mark_price_returns_none():
    # Net-long WETH but no mark price in token_prices → degrade (no fetch).
    events = [
        _swap_event(
            deployment_id=_DEP,
            token_in="USDC",
            amount_in="2000",
            token_out="WETH",
            amount_out="1.0",
            amount_out_usd="2000",
        ),
    ]
    assert compute_inventory_unrealized(events, _DEP, {}) is None
