"""Dashboard endpoint serialization preserves G6 identity and measurement evidence."""

import json
from datetime import UTC, datetime
from decimal import Decimal

from almanak.framework.accounting.inventory_revaluation import compute_inventory_revaluation
from almanak.framework.dashboard.quant_aggregations import CostStack, compute_reconciliation
from almanak.framework.portfolio.models import PortfolioSnapshot, PositionValue, TokenBalance
from almanak.framework.teardown.models import PositionType
from almanak.gateway.services.dashboard_service import DashboardServiceServicer

WALLET = "0x0000000000000000000000000000000000000001"
DEPLOYMENT = "deployment:scope"


def test_dashboard_snapshot_keeps_zero_scope_and_principal_token_evidence():
    balance = TokenBalance("BNB", Decimal("0"), Decimal("0"), price_usd=Decimal("0"))
    balance.chain = "bsc"
    balance.wallet_address = WALLET
    scope = {"schema_version": 1, "chain_wallets": {"bsc": WALLET}}
    position = PositionValue(
        PositionType.TOKEN,
        "pt",
        "bsc",
        Decimal("12"),
        "held PT",
        details={
            "source": "pt_inventory_lots",
            "pt_symbol": "PT-ASSET-31DEC2026",
            "quantity": "1",
        },
        cost_basis_usd=Decimal("10"),
    )
    snapshot = PortfolioSnapshot(
        timestamp=datetime.now(UTC),
        deployment_id=DEPLOYMENT,
        total_value_usd=Decimal("12"),
        available_cash_usd=Decimal("0"),
        positions=[position],
        wallet_balances=[balance],
        chain="bsc",
        snapshot_metadata={"wallet_scope": scope},
    )
    row = DashboardServiceServicer._snapshot_for_inventory_revaluation(snapshot)
    assert row["deployment_id"] == DEPLOYMENT
    wallet = json.loads(row["wallet_balances_json"])[0]
    assert wallet["balance"] == wallet["value_usd"] == wallet["price_usd"] == "0"
    assert (wallet["chain"], wallet["wallet_address"]) == ("bsc", WALLET)
    envelope = json.loads(row["positions_json"])
    assert envelope["metadata"]["wallet_scope"] == scope
    assert envelope["positions"][0]["cost_basis_usd"] == "10"
    event = {
        "deployment_id": DEPLOYMENT,
        "chain": "bsc",
        "wallet_address": WALLET,
        "event_type": "PT_BUY",
        "position_key": "pendle_pt",
        "timestamp": "2026-09-10T00:00:00+00:00",
        "payload_json": json.dumps(
            {"pt_token": "PT-ASSET-31DEC2026", "pt_amount": "1", "sy_amount": "10", "sy_price": "1"}
        ),
    }
    assert compute_inventory_revaluation(
        snapshot_initial=row, snapshot_final=row, accounting_events=[event], deployment_id=DEPLOYMENT
    ).total_usd == Decimal("2")


def test_zero_gap_cannot_certify_an_unmeasured_inventory_term():
    endpoint = {
        "deployment_id": DEPLOYMENT,
        "wallet_balances_json": json.dumps([{"symbol": "USDT", "balance": "1", "price_usd": "1"}]),
    }
    result = compute_reconciliation(
        accounting_events=[],
        initial_value_usd=Decimal("1"),
        nav_usd=Decimal("1"),
        cost_stack=CostStack(),
        snapshot_initial=endpoint,
        snapshot_final=endpoint,
        deployment_id=DEPLOYMENT,
    )
    assert result.gap_usd == Decimal("0")
    assert result.has_data and result.has_unmeasured
    assert not result.passed


def test_deployment_dashboard_cannot_pass_when_both_endpoints_are_missing():
    result = compute_reconciliation(
        accounting_events=[],
        initial_value_usd=Decimal("1"),
        nav_usd=Decimal("1"),
        cost_stack=CostStack(),
        deployment_id=DEPLOYMENT,
        snapshot_initial=None,
        snapshot_final=None,
    )
    assert result.has_unmeasured
    assert not result.passed


def test_identity_survives_the_persistence_round_trip_g6_actually_uses():
    """The test above plants `chain` / `wallet_address` on a live object, so it
    passes even when the typed model drops them on load. GetAuditPosture never sees
    a live object: StateManager.get_first_snapshot / get_latest_snapshot both go
    through PortfolioSnapshot.from_dict. Pin that path instead."""
    balance = TokenBalance("BNB", Decimal("2"), Decimal("4"), price_usd=Decimal("2"))
    balance.chain = "bsc"
    balance.wallet_address = WALLET
    snapshot = PortfolioSnapshot(
        timestamp=datetime.now(UTC),
        deployment_id=DEPLOYMENT,
        total_value_usd=Decimal("4"),
        available_cash_usd=Decimal("0"),
        positions=[],
        wallet_balances=[balance],
        chain="bsc",
        snapshot_metadata={"wallet_scope": {"schema_version": 1, "chain_wallets": {"bsc": WALLET}}},
    )

    reloaded = PortfolioSnapshot.from_dict(snapshot.to_dict())
    assert reloaded.wallet_balances[0].chain == "bsc"
    assert reloaded.wallet_balances[0].wallet_address == WALLET

    row = DashboardServiceServicer._snapshot_for_inventory_revaluation(reloaded)
    wallet = json.loads(row["wallet_balances_json"])[0]
    assert (wallet["chain"], wallet["wallet_address"]) == ("bsc", WALLET)


def test_unscoped_round_trip_stays_unmeasured_rather_than_inventing_identity():
    """Negative control: a row with no observed identity must not acquire one from
    the snapshot-level chain, or legacy captures would be silently certified."""
    balance = TokenBalance("BNB", Decimal("2"), Decimal("4"), price_usd=Decimal("2"))
    snapshot = PortfolioSnapshot(
        timestamp=datetime.now(UTC),
        deployment_id=DEPLOYMENT,
        total_value_usd=Decimal("4"),
        available_cash_usd=Decimal("0"),
        positions=[],
        wallet_balances=[balance],
        chain="bsc",
    )
    reloaded = PortfolioSnapshot.from_dict(snapshot.to_dict())
    row = DashboardServiceServicer._snapshot_for_inventory_revaluation(reloaded)
    wallet = json.loads(row["wallet_balances_json"])[0]
    assert wallet["chain"] is None and wallet["wallet_address"] is None

    assert (
        compute_inventory_revaluation(
            snapshot_initial=row, snapshot_final=row, accounting_events=[], deployment_id=DEPLOYMENT
        ).confidence
        == "unmeasured_identity"
    )
