"""VIB-3872 WI-2 — live managed-Anvil integration proof for the settlement capability.

Boots a real managed Arbitrum Anvil fork behind the gateway (pinned just after two
public GMX keeper settlements) and drives
``GmxV2RunnerHookConnector.resolve_perp_settlements`` through the LIVE gateway —
exercising the real gateway-routed ``eth_getLogs`` correlation +
``eth_getTransactionReceipt`` + ``extract_perp_fill`` end-to-end (not a fake).

This test is OPT-IN: it only runs when ``RUN_GMX_SETTLEMENT_INTEGRATION=1`` (and a
usable ``ARBITRUM_RPC_URL`` is configured), so it never runs in the unit lane. Run
it explicitly:

    RUN_GMX_SETTLEMENT_INTEGRATION=1 uv run pytest \
        tests/integration/connectors/test_gmx_perp_settlement_anvil.py \
        --import-mode=importlib -s

The deterministic correlation logic is covered CI-side by
``test_perp_settlement_capability.py`` using the same real keeper bytes through a
fake gateway handle; this test proves the LIVE gateway round-trip.
"""

import os
import tempfile

import pytest

_ENABLED = os.environ.get("RUN_GMX_SETTLEMENT_INTEGRATION") == "1"

pytestmark = pytest.mark.skipif(
    not _ENABLED,
    reason="opt-in live managed-Anvil integration; set RUN_GMX_SETTLEMENT_INTEGRATION=1 to run",
)

# Fork pinned just after both settlements so [submission_block, latest] is a tiny,
# durable range (independent of when the test is run). Both orders are public
# Arbitrum GMX keeper executions captured for the WI-1 fixture.
_FORK_BLOCK = 487_783_800
_OPEN = ("0x585d42d95b9a4e84e78d53073d85fa6e67304c119fc000a6052661068200f9cf", 487_783_560)
_CLOSE = ("0x8a490b71b5c4f3605468e71c74d563cbf6af5e536ae66f8323ec5d7bf47a8494", 487_783_770)


def _arbitrum_rpc_url() -> str | None:
    env = os.environ.get("ARBITRUM_RPC_URL")
    if env:
        return env
    try:
        for line in open(".env"):
            line = line.strip()
            if line.startswith("ARBITRUM_RPC_URL="):
                return line.split("=", 1)[1].strip().strip('"').strip("'")
    except OSError:
        return None
    return None


def test_live_gateway_settlement_returns_executed_with_measured_fill() -> None:
    url = _arbitrum_rpc_url()
    if not url:
        pytest.skip("ARBITRUM_RPC_URL not configured")

    # Load the gateway integration harness by path (tests/ is importable as a
    # package, but the heavy conftest fixtures are session-scoped there; we drive
    # the building blocks directly for a single-chain pinned fork).
    from almanak.connectors._strategy_base.runner_hook_registry import (
        PerpSettlementState,
        PerpSettlementWatchEntry,
    )
    from almanak.connectors.gmx_v2.runner_hooks import GmxV2RunnerHookConnector
    from almanak.framework.gateway_client import GatewayClient, GatewayClientConfig
    from almanak.gateway.core.settings import GatewaySettings
    from tests.conftest_gateway import (
        TEST_GATEWAY_PORT,
        AnvilFixture,
        GatewayServerThread,
        is_gateway_running,
    )

    os.environ.setdefault("ALMANAK_STATE_DB", os.path.join(tempfile.mkdtemp(), "state.db"))

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

        entries = (
            PerpSettlementWatchEntry(
                order_key=_OPEN[0], submission_block=_OPEN[1], is_open=True, seconds_since_submission=30
            ),
            PerpSettlementWatchEntry(
                order_key=_CLOSE[0], submission_block=_CLOSE[1], is_open=False, seconds_since_submission=30
            ),
        )
        verdicts = GmxV2RunnerHookConnector().resolve_perp_settlements(
            gateway_client=client,
            chain="arbitrum",
            wallet_address="0x0000000000000000000000000000000000000000",
            watch_entries=entries,
        )

        assert len(verdicts) == 2
        open_v, close_v = verdicts

        assert open_v.state is PerpSettlementState.EXECUTED
        assert open_v.terminal is True
        assert open_v.keeper_tx_hash is not None
        assert open_v.fill_data is not None
        assert open_v.fill_data.is_open is True
        # VIB-6110: entry_price is scaled USD-per-token. This fixture's OPEN market is
        # BTC/USD (listed, decimals 8) → a real ~$64.5k price, not the sub-1 raw ratio.
        assert open_v.fill_data.entry_price is not None and open_v.fill_data.entry_price > 1000
        assert open_v.fill_data.size_delta_usd is not None and open_v.fill_data.size_delta_usd > 0
        assert open_v.fill_data.position_fee_usd is not None

        assert close_v.state is PerpSettlementState.EXECUTED
        assert close_v.fill_data is not None
        assert close_v.fill_data.is_open is False
        # This fixture's CLOSE market (0xdab2…) is NOT in GMX_V2_INDEX_TOKEN_DECIMALS, so
        # VIB-6110 fails closed: exit_price is UNMEASURED (None), never the raw GMX ratio.
        assert close_v.fill_data.exit_price is None
        # Real close of a losing short → signed negative realized PnL.
        assert close_v.fill_data.realized_pnl_usd is not None
        assert close_v.fill_data.realized_pnl_usd < 0
        assert close_v.fill_data.funding_fee_usd is not None
    finally:
        if client is not None:
            try:
                client.disconnect()
            except Exception:  # noqa: BLE001 — best-effort teardown
                pass
        if server is not None:
            try:
                server.stop()
            except Exception:  # noqa: BLE001
                pass
        try:
            anvil.stop()
        except Exception:  # noqa: BLE001
            pass
