"""VIB-3872 WI-3 — live managed-Anvil E2E for the perp settlement reconciler.

Proves the FULL WI-3 write path end-to-end against REAL keeper bytes through the
live gateway: seed a PERP_OPEN submission ledger row carrying a real Arbitrum GMX
order's ``async_orders``, boot a managed Anvil fork pinned just after that order's
keeper settlement, run one reconciler tick, and assert a terminal ``PERP_SETTLEMENT``
accounting_events row lands with the measured economics — derivation → live
``resolve_perp_settlements`` (WI-2) → drain-first commit → ``AccountingWriter`` →
SQLite row.

Why a seeded settled order (not a continuous run): on an Anvil fork there is NO
real keeper, so a plain continuous run leaves the order correctly PENDING (the
strat-test barrier is the only Anvil keeper simulator, and it is not the
production reconciler path). Seeding a REAL Arbitrum-mainnet settled order and
reading it back through the live gateway is the faithful, reproducible Anvil proof
of the reconciler's production path; the real keeper-latency proof is the sealed
mainnet WI-5.

OPT-IN: only runs when ``RUN_PERP_SETTLEMENT_E2E=1`` (and ``ARBITRUM_RPC_URL`` is
configured); skips in the unit lane.
"""

from __future__ import annotations

import json
import os
import tempfile
from decimal import Decimal
from types import SimpleNamespace

import pytest

_ENABLED = os.environ.get("RUN_PERP_SETTLEMENT_E2E") == "1"
pytestmark = pytest.mark.skipif(
    not _ENABLED, reason="opt-in live managed-Anvil E2E; set RUN_PERP_SETTLEMENT_E2E=1 to run"
)

_FORK_BLOCK = 487_783_800
_OPEN_ORDER_KEY = "0x585d42d95b9a4e84e78d53073d85fa6e67304c119fc000a6052661068200f9cf"
# Block 487_783_565 keeper tx; used as the submission tx so the reconciler's
# fromBlock includes the OrderExecuted for this order.
_KEEPER_TX = "0xffae75a8fa92bc1f8b27b7892dfa99751ce34b737fdc13ca26e3cb1bd31d85c1"
_DEPLOYMENT = "deployment:wi3e2e"


def _arbitrum_rpc_url() -> str | None:
    if os.environ.get("ARBITRUM_RPC_URL"):
        return os.environ["ARBITRUM_RPC_URL"]
    try:
        for line in open(".env"):
            if line.strip().startswith("ARBITRUM_RPC_URL="):
                return line.strip().split("=", 1)[1].strip().strip('"').strip("'")
    except OSError:
        return None
    return None


def _async_orders_json() -> str:
    repr_str = (
        f"AsyncOrderData(protocol='gmx_v2', order_id='{_OPEN_ORDER_KEY}', "
        f"status=<AsyncOrderStatus.PENDING: 'pending'>, kind=<AsyncOrderKind.INCREASE: 'increase'>, "
        f"market='0xmkt', collateral_token='0xusdc', is_long=True, size_delta_usd=None)"
    )
    return json.dumps({"async_orders": [repr_str]})


@pytest.mark.asyncio
async def test_reconciler_books_settlement_from_real_keeper_order() -> None:
    url = _arbitrum_rpc_url()
    if not url:
        pytest.skip("ARBITRUM_RPC_URL not configured")

    from almanak.framework.accounting.basis import FIFOBasisStore
    from almanak.framework.accounting.processor import AccountingProcessor
    from almanak.framework.gateway_client import GatewayClient, GatewayClientConfig
    from almanak.framework.observability.ledger import LedgerEntry
    from almanak.framework.runner.perp_settlement_reconciler import reconcile_perp_settlements
    from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore
    from almanak.gateway.core.settings import GatewaySettings
    from tests.conftest_gateway import (
        TEST_GATEWAY_PORT,
        AnvilFixture,
        GatewayServerThread,
        is_gateway_running,
    )

    tmp = tempfile.mkdtemp()
    os.environ.setdefault("ALMANAK_STATE_DB", os.path.join(tmp, "state.db"))
    store = SQLiteStore(SQLiteConfig(db_path=os.path.join(tmp, "strategy.db")))
    await store.initialize()

    # Seed the Phase-1 submission ledger row (PERP_OPEN) with the real order key.
    await store.save_ledger_entry(
        LedgerEntry(
            id="e2e-open",
            cycle_id="cyc-0",
            deployment_id=_DEPLOYMENT,
            execution_mode="paper",
            intent_type="PERP_OPEN",
            tx_hash=_KEEPER_TX,
            chain="arbitrum",
            protocol="gmx_v2",
            success=True,
            extracted_data_json=_async_orders_json(),
        )
    )

    processor = AccountingProcessor(state_manager=store, basis_store=FIFOBasisStore(), deployment_id=_DEPLOYMENT)
    runner = SimpleNamespace(
        state_manager=store,
        _accounting_processor=processor,
        config=SimpleNamespace(chain="arbitrum"),
        _is_live_mode=lambda: False,
    )
    strategy = SimpleNamespace(deployment_id=_DEPLOYMENT, chain="arbitrum", wallet_address="0xwallet")

    anvil = AnvilFixture("arbitrum", url, fork_block_number=_FORK_BLOCK)
    server = None
    client = None
    try:
        anvil.start(timeout=90.0)
        settings = GatewaySettings(
            grpc_port=TEST_GATEWAY_PORT,
            grpc_host="127.0.0.1",
            network="anvil",
            metrics_enabled=False,
            audit_enabled=False,
            allow_insecure=True,
        )
        server = GatewayServerThread(settings, anvil_ports={"arbitrum": anvil.port})
        server.start()
        assert is_gateway_running(TEST_GATEWAY_PORT)
        client = GatewayClient(GatewayClientConfig(host="127.0.0.1", port=TEST_GATEWAY_PORT))
        client.connect()

        # One reconciler tick — derive → live resolve → commit → write.
        await reconcile_perp_settlements(
            runner, strategy, deployment_id=_DEPLOYMENT, cycle_id="cyc-1", gateway_client=client
        )

        rows = await store.get_accounting_events(_DEPLOYMENT, event_type="PERP_SETTLEMENT")
        assert len(rows) == 1, f"expected exactly one PERP_SETTLEMENT row, got {len(rows)}"
        payload = json.loads(rows[0]["payload_json"] if isinstance(rows[0], dict) else rows[0].payload_json)
        assert payload["settlement_state"] == "EXECUTED"
        assert payload["order_key"] == _OPEN_ORDER_KEY
        assert payload["submission_ledger_entry_id"] == "e2e-open"
        assert Decimal(payload["size_delta_usd"]) == pytest.approx(Decimal("2422.85"), abs=Decimal("0.5"))
        assert payload["position_fee_usd"] is not None and Decimal(payload["position_fee_usd"]) > 0
        assert payload["entry_price"] is not None
        print("\n[PASS] PERP_SETTLEMENT booked from real keeper order:", payload["order_key"])
        print("  size_delta_usd:", payload["size_delta_usd"], "position_fee_usd:", payload["position_fee_usd"])
    finally:
        for closer in (
            lambda: client.disconnect() if client else None,
            lambda: server.stop() if server else None,
            anvil.stop,
        ):
            try:
                closer()
            except Exception:  # noqa: BLE001 — best-effort teardown
                pass
