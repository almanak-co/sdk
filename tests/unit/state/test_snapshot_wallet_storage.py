"""Wallet identity persists through both local SQLite writes and hosted RPC envelopes."""

import json
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.framework.portfolio.models import (
    BaselineProvenance,
    PortfolioMetrics,
    PortfolioSnapshot,
    TokenBalance,
    ValueConfidence,
    encode_baseline_provenance,
)
from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore
from almanak.framework.state.gateway_state_manager import GatewayStateManager
from almanak.gateway.services.state_service import StateServiceServicer

WALLET = "0x1234567890123456789012345678901234567890"


def snapshot():
    return PortfolioSnapshot(
        timestamp=datetime.now(UTC),
        deployment_id="deployment:scope",
        total_value_usd=Decimal("0"),
        available_cash_usd=Decimal("2"),
        wallet_total_value_usd=Decimal("2"),
        value_confidence=ValueConfidence.HIGH,
        chain="bsc",
        execution_mode="live",
        cycle_id="original-cycle",
        wallet_balances=[
            TokenBalance("USDT", Decimal("2"), Decimal("2"), price_usd=None, chain="bsc", wallet_address=WALLET)
        ],
        snapshot_metadata={"wallet_scope": {"schema_version": 1, "chain_wallets": {"bsc": WALLET}}},
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("atomic", [False, True])
async def test_real_sqlite_restart_preserves_row_and_endpoint_scope(tmp_path, atomic):
    config = SQLiteConfig(db_path=str(tmp_path / "scope.db"))
    store = SQLiteStore(config)
    original = snapshot()
    try:
        if atomic:
            metrics = PortfolioMetrics(
                original.deployment_id,
                original.timestamp,
                Decimal("2"),
                Decimal("2"),
                positions_json=encode_baseline_provenance(
                    BaselineProvenance("snapshot_available_cash_usd", Decimal("2"))
                ),
            )
            await store.save_snapshot_and_metrics(original, metrics)
        else:
            await store.save_portfolio_snapshot(original)
    finally:
        await store.close()
    reopened = SQLiteStore(config)
    try:
        restored = await reopened.get_latest_snapshot(original.deployment_id)
        assert restored is not None
        assert restored.wallet_balances[0].wallet_address == WALLET
        assert restored.wallet_balances[0].chain == "bsc"
        assert restored.wallet_balances[0].price_usd is None
        assert restored.to_positions_payload()["metadata"]["wallet_scope"] == original.snapshot_metadata["wallet_scope"]
        assert restored.cycle_id == "original-cycle"
    finally:
        await reopened.close()


@pytest.mark.asyncio
async def test_hosted_rpc_envelope_and_gateway_decoder_preserve_scope():
    original = snapshot()
    client = MagicMock()
    client.state.SavePortfolioSnapshot.return_value = MagicMock(success=True, snapshot_id=1)
    manager = GatewayStateManager(client)
    await manager.save_portfolio_snapshot(original)
    request = client.state.SavePortfolioSnapshot.call_args.args[0]
    payload = json.loads(request.positions_json)
    assert payload["wallet_balances"][0]["chain"] == "bsc"
    assert payload["wallet_balances"][0]["wallet_address"] == WALLET
    assert payload["wallet_balances"][0]["price_usd"] is None
    metadata = payload["metadata"]
    _, _, rows, _ = StateServiceServicer._extract_smuggled_snapshot_fields(payload, metadata)
    assert rows[0]["wallet_address"] == WALLET
    assert metadata["wallet_scope"] == original.snapshot_metadata["wallet_scope"]
    assert request.cycle_id == "original-cycle"
