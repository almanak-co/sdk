"""Characterization tests for ToolExecutor._execute_teardown_vault.

These tests lock down the existing state-machine behaviour BEFORE refactoring
(Phase 7.1b). Every phase transition, resume path, error-rollback semantic,
and gateway-call sequence is captured here so we can extract helpers without
silently regressing crash-recovery semantics for vault teardown - a fund-loss
path if broken.

State machine phases (persisted under agent_state["_teardown"]["phase"]):
    start -> lp_closing -> lp_closed -> swapping -> swapped -> settling -> torn_down

Resume matrix:
    - on "start" or "lp_closing" -> replays LP close if lp_position_id present
    - on "lp_closed" / "swapping" / "settling" -> skips LP close, uses saved counts
    - on "torn_down" -> fast-path success return
"""

from __future__ import annotations

import json
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.agent_tools.executor import ToolExecutor
from almanak.framework.agent_tools.policy import AgentPolicy
from almanak.framework.agent_tools.schemas import ToolResponse

UNDERLYING = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
TOKEN_A = "0x1111111111111111111111111111111111111111"
TOKEN_B = "0x2222222222222222222222222222222222222222"
VAULT = "0x" + "a" * 40
SAFE = "0x" + "b" * 40
VALUATOR = "0x1234567890abcdef1234567890abcdef12345678"


@pytest.fixture
def mock_gateway():
    client = MagicMock()
    client.is_connected = True
    return client


@pytest.fixture
def executor(mock_gateway):
    policy = AgentPolicy(
        allowed_chains={"base"},
        max_tool_calls_per_minute=200,
        cooldown_seconds=0,
        max_single_trade_usd=Decimal("999999999"),
        max_daily_spend_usd=Decimal("999999999"),
        max_position_size_usd=Decimal("999999999"),
        require_human_approval_above_usd=Decimal("999999999"),
        require_rebalance_check=False,
    )
    return ToolExecutor(
        mock_gateway,
        policy=policy,
        wallet_address="0x1234567890abcdef1234567890abcdef12345678",
        strategy_id="test-strategy",
        default_chain="base",
    )


def _make_sdk(underlying=UNDERLYING, total_assets=1_000_000, raise_total_assets=False):
    sdk = MagicMock()
    sdk.get_underlying_token_address.return_value = underlying
    if raise_total_assets:
        sdk.get_total_assets.side_effect = RuntimeError("rpc down")
    else:
        sdk.get_total_assets.return_value = total_assets
    return sdk


def _set_state(mock_gateway, state: dict | None):
    """Prime LoadState mock with the given state dict (or empty payload)."""
    state_resp = MagicMock()
    state_resp.data = json.dumps(state).encode() if state else b""
    mock_gateway.state.LoadState.return_value = state_resp
    mock_gateway.state.SaveState.return_value = MagicMock(success=True)


def _args(**overrides: Any) -> dict:
    base = {
        "vault_address": VAULT,
        "safe_address": SAFE,
        "valuator_address": VALUATOR,
        "chain": "base",
    }
    base.update(overrides)
    return base


def _saved_states(mock_gateway) -> list[dict]:
    """Return the list of agent_state dicts passed to SaveState in order."""
    out = []
    for call in mock_gateway.state.SaveState.call_args_list:
        args, _kwargs = call
        req = args[0]
        out.append(json.loads(req.data))
    return out


class TestHappyPath:
    """Full happy-path teardown: LP close -> swap -> settle -> torn_down."""

    @pytest.mark.asyncio
    async def test_happy_path_full_flow_with_lp_and_swap(self, executor, mock_gateway):
        _set_state(
            mock_gateway,
            {
                "phase": "running",
                "lp_position_id": "42",
                "token_a": TOKEN_A,
                "token_b": TOKEN_B,
            },
        )

        calls: list[tuple[str, dict]] = []

        async def fake_execute(name, args):
            calls.append((name, args))
            if name == "close_lp_position":
                return ToolResponse(status="success", data={"tx_hash": "0xclose"})
            if name == "get_balance":
                return ToolResponse(status="success", data={"balance": "10", "balance_usd": "10"})
            if name == "swap_tokens":
                return ToolResponse(status="success", data={"tx_hash": "0xswap"})
            if name == "settle_vault":
                return ToolResponse(status="success", data={"tx_hash": "0xsettle", "new_total_assets": "2000"})
            return ToolResponse(status="error", error={"message": "unmocked"})

        lp_info = ToolResponse(
            status="success", data={"token_a": TOKEN_A, "token_b": TOKEN_B}
        )

        with (
            patch.object(executor, "execute", side_effect=fake_execute),
            patch.object(executor, "_execute_get_lp_position", return_value=lp_info) as get_lp_position,
            patch("almanak.framework.connectors.lagoon.sdk.LagoonVaultSDK", return_value=_make_sdk()),
        ):
            result = await executor._execute_teardown_vault(_args())

        assert result.status == "success"
        assert result.data["status"] == "success"
        assert result.data["positions_closed"] == 1
        # Both token_a and token_b are non-underlying, so two swaps.
        assert result.data["swaps_executed"] == 2
        assert result.data["final_nav"] == "1000000"
        # Swap-target discovery MUST consult _execute_get_lp_position using
        # the pre-close LP id (42) — not the cleared live state. This locks
        # down the "close -> get_lp_position -> get_balance -> swap" flow.
        get_lp_position.assert_awaited_once()
        assert get_lp_position.await_args.args[0]["position_id"] == "42"
        # tx_hashes include close + 2 swaps + settle
        assert set(result.data["tx_hashes"]) >= {"0xclose", "0xswap", "0xsettle"}

        # Verify state transitions were persisted: lp_closing -> lp_closed ->
        # swapping -> swapped -> settling -> torn_down (final phase is the
        # outer agent_state phase, not the _teardown sub-state).
        saved = _saved_states(mock_gateway)
        phases = [s.get("_teardown", {}).get("phase") for s in saved]
        assert "lp_closing" in phases
        assert "lp_closed" in phases
        assert "swapping" in phases
        assert "swapped" in phases
        assert "settling" in phases
        # Final save should wipe _teardown and mark torn_down
        assert saved[-1]["phase"] == "torn_down"
        assert saved[-1]["lp_position_id"] is None
        assert "_teardown" not in saved[-1]

    @pytest.mark.asyncio
    async def test_happy_path_no_lp_no_swap_just_settle(self, executor, mock_gateway):
        _set_state(mock_gateway, {"phase": "running"})  # no lp_position_id, no tokens

        async def fake_execute(name, args):
            if name == "settle_vault":
                return ToolResponse(status="success", data={"tx_hash": "0xsettle"})
            return ToolResponse(status="error", error={"message": "unmocked"})

        with (
            patch.object(executor, "execute", side_effect=fake_execute),
            patch("almanak.framework.connectors.lagoon.sdk.LagoonVaultSDK", return_value=_make_sdk()),
        ):
            result = await executor._execute_teardown_vault(_args())

        assert result.status == "success"
        assert result.data["positions_closed"] == 0
        assert result.data["swaps_executed"] == 0
        assert result.data["tx_hashes"] == ["0xsettle"]


class TestAlreadyTornDown:
    """Fast-path: if state already torn_down, return immediately."""

    @pytest.mark.asyncio
    async def test_agent_state_phase_torn_down_short_circuits(self, executor, mock_gateway):
        _set_state(mock_gateway, {"phase": "torn_down"})

        # Any execute / SDK call would be a bug - assert they don't happen.
        with (
            patch.object(executor, "execute", side_effect=AssertionError("should not run")),
            patch("almanak.framework.connectors.lagoon.sdk.LagoonVaultSDK", side_effect=AssertionError("should not run")),
        ):
            result = await executor._execute_teardown_vault(_args())

        assert result.status == "success"
        assert "already torn down" in result.data["message"].lower()

    @pytest.mark.asyncio
    async def test_teardown_sub_state_torn_down_short_circuits(self, executor, mock_gateway):
        # Even if outer phase is running, _teardown.phase=torn_down must short-circuit.
        _set_state(mock_gateway, {"phase": "running", "_teardown": {"phase": "torn_down"}})

        with (
            patch.object(executor, "execute", side_effect=AssertionError("should not run")),
            patch("almanak.framework.connectors.lagoon.sdk.LagoonVaultSDK", side_effect=AssertionError("should not run")),
        ):
            result = await executor._execute_teardown_vault(_args())

        assert result.status == "success"


class TestResumeFromLpClosed:
    """Resume after a previous attempt that completed LP close."""

    @pytest.mark.asyncio
    async def test_resume_from_lp_closed_skips_close_preserves_count(self, executor, mock_gateway):
        # Mid-recovery: LP already closed (lp_position_id cleared), swap + settle pending.
        _set_state(
            mock_gateway,
            {
                "phase": "running",
                "lp_position_id": None,
                "_teardown": {"phase": "lp_closed", "positions_closed": 1},
                "token_a": TOKEN_A,
            },
        )

        calls: list[str] = []

        async def fake_execute(name, args):
            calls.append(name)
            if name == "close_lp_position":
                raise AssertionError("must not re-run close on resume")
            if name == "get_balance":
                return ToolResponse(status="success", data={"balance": "5"})
            if name == "swap_tokens":
                return ToolResponse(status="success", data={"tx_hash": "0xswap"})
            if name == "settle_vault":
                return ToolResponse(status="success", data={"tx_hash": "0xsettle"})
            return ToolResponse(status="error", error={"message": "unmocked"})

        with (
            patch.object(executor, "execute", side_effect=fake_execute),
            patch("almanak.framework.connectors.lagoon.sdk.LagoonVaultSDK", return_value=_make_sdk()),
        ):
            result = await executor._execute_teardown_vault(_args())

        assert result.status == "success"
        # Count was preserved from prior attempt.
        assert result.data["positions_closed"] == 1
        assert result.data["swaps_executed"] == 1
        assert "close_lp_position" not in calls


class TestResumeFromSettling:
    """Resume where only final settlement remains."""

    @pytest.mark.asyncio
    async def test_resume_from_settling_skips_lp_close_still_runs_swap_and_settle(
        self, executor, mock_gateway
    ):
        _set_state(
            mock_gateway,
            {
                "phase": "running",
                "lp_position_id": None,
                "_teardown": {"phase": "settling", "positions_closed": 1},
            },
        )

        calls: list[str] = []

        async def fake_execute(name, args):
            calls.append(name)
            if name == "settle_vault":
                return ToolResponse(status="success", data={"tx_hash": "0xsettle"})
            if name == "close_lp_position":
                raise AssertionError("must not rerun close")
            return ToolResponse(status="error", error={"message": "unmocked"})

        with (
            patch.object(executor, "execute", side_effect=fake_execute),
            patch("almanak.framework.connectors.lagoon.sdk.LagoonVaultSDK", return_value=_make_sdk()),
        ):
            result = await executor._execute_teardown_vault(_args())

        assert result.status == "success"
        assert "close_lp_position" not in calls
        assert "settle_vault" in calls


class TestLpCloseFailure:
    """LP close failure: fires alert, returns recoverable error, persists progress."""

    @pytest.mark.asyncio
    async def test_lp_close_returns_error_status_blocks_teardown(self, executor, mock_gateway):
        _set_state(mock_gateway, {"phase": "running", "lp_position_id": "42"})

        async def fake_execute(name, args):
            if name == "close_lp_position":
                return ToolResponse(status="error", error={"message": "slippage too high"})
            raise AssertionError(f"should not run {name} after LP close failure")

        with (
            patch.object(executor, "execute", side_effect=fake_execute),
            patch.object(executor, "_fire_alert") as fire_alert,
            patch("almanak.framework.connectors.lagoon.sdk.LagoonVaultSDK", return_value=_make_sdk()),
        ):
            result = await executor._execute_teardown_vault(_args())

        assert result.status == "error"
        assert result.error["error_code"] == "teardown_lp_close_failed"
        assert result.error["recoverable"] is True
        fire_alert.assert_called_once()
        # critical severity
        assert fire_alert.call_args.kwargs["severity"] == "critical"

        # Progress should be persisted with phase=lp_closing + error annotation
        saved = _saved_states(mock_gateway)
        last_teardown = saved[-1]["_teardown"]
        assert last_teardown["phase"] == "lp_closing"
        assert "error" in last_teardown

    @pytest.mark.asyncio
    async def test_lp_close_raises_exception_also_blocks_teardown(self, executor, mock_gateway):
        _set_state(mock_gateway, {"phase": "running", "lp_position_id": "42"})

        async def fake_execute(name, args):
            if name == "close_lp_position":
                raise RuntimeError("rpc down")
            raise AssertionError(f"should not run {name}")

        with (
            patch.object(executor, "execute", side_effect=fake_execute),
            patch.object(executor, "_fire_alert"),
            patch("almanak.framework.connectors.lagoon.sdk.LagoonVaultSDK", return_value=_make_sdk()),
        ):
            result = await executor._execute_teardown_vault(_args())

        assert result.status == "error"
        assert result.error["error_code"] == "teardown_lp_close_failed"
        assert "rpc down" in result.error["message"]


class TestSwapFailureIsTolerated:
    """Per-token swap failure is logged but does not abort teardown."""

    @pytest.mark.asyncio
    async def test_swap_exception_does_not_abort_settle(self, executor, mock_gateway):
        _set_state(
            mock_gateway,
            {"phase": "running", "lp_position_id": None, "token_a": TOKEN_A},
        )

        async def fake_execute(name, args):
            if name == "get_balance":
                return ToolResponse(status="success", data={"balance": "1"})
            if name == "swap_tokens":
                raise RuntimeError("swap blew up")
            if name == "settle_vault":
                return ToolResponse(status="success", data={"tx_hash": "0xsettle"})
            return ToolResponse(status="error", error={"message": "unmocked"})

        with (
            patch.object(executor, "execute", side_effect=fake_execute),
            patch("almanak.framework.connectors.lagoon.sdk.LagoonVaultSDK", return_value=_make_sdk()),
        ):
            result = await executor._execute_teardown_vault(_args())

        assert result.status == "success"
        assert result.data["swaps_executed"] == 0

    @pytest.mark.asyncio
    async def test_zero_balance_token_is_not_swapped(self, executor, mock_gateway):
        _set_state(
            mock_gateway,
            {"phase": "running", "lp_position_id": None, "token_a": TOKEN_A},
        )

        async def fake_execute(name, args):
            if name == "get_balance":
                return ToolResponse(status="success", data={"balance": "0"})
            if name == "swap_tokens":
                raise AssertionError("should not swap zero balance")
            if name == "settle_vault":
                return ToolResponse(status="success", data={"tx_hash": "0xsettle"})
            return ToolResponse(status="error", error={"message": "unmocked"})

        with (
            patch.object(executor, "execute", side_effect=fake_execute),
            patch("almanak.framework.connectors.lagoon.sdk.LagoonVaultSDK", return_value=_make_sdk()),
        ):
            result = await executor._execute_teardown_vault(_args())

        assert result.status == "success"
        assert result.data["swaps_executed"] == 0


class TestSettlementFailure:
    """Final settlement failure: partial_failure status + critical alert."""

    @pytest.mark.asyncio
    async def test_settle_returns_error_yields_partial_failure(self, executor, mock_gateway):
        _set_state(mock_gateway, {"phase": "running", "lp_position_id": None})

        async def fake_execute(name, args):
            if name == "settle_vault":
                return ToolResponse(status="error", error={"message": "valuator offline"})
            return ToolResponse(status="error", error={"message": "unmocked"})

        with (
            patch.object(executor, "execute", side_effect=fake_execute),
            patch.object(executor, "_fire_alert") as fire_alert,
            patch("almanak.framework.connectors.lagoon.sdk.LagoonVaultSDK", return_value=_make_sdk()),
        ):
            result = await executor._execute_teardown_vault(_args())

        assert result.status == "partial_failure"
        assert result.data["status"] == "partial_failure"
        fire_alert.assert_called_once()
        assert fire_alert.call_args.kwargs["severity"] == "critical"

        # Partial failure does NOT promote to torn_down.
        saved = _saved_states(mock_gateway)
        last = saved[-1]
        assert last.get("phase") != "torn_down"
        assert last["_teardown"]["phase"] == "settling"
        assert last["_teardown"].get("error") == "settlement failed"

    @pytest.mark.asyncio
    async def test_settle_exception_also_yields_partial_failure(self, executor, mock_gateway):
        _set_state(mock_gateway, {"phase": "running", "lp_position_id": None})

        async def fake_execute(name, args):
            if name == "settle_vault":
                raise RuntimeError("rpc dead")
            return ToolResponse(status="error", error={"message": "unmocked"})

        with (
            patch.object(executor, "execute", side_effect=fake_execute),
            patch.object(executor, "_fire_alert"),
            patch("almanak.framework.connectors.lagoon.sdk.LagoonVaultSDK", return_value=_make_sdk()),
        ):
            result = await executor._execute_teardown_vault(_args())

        assert result.status == "partial_failure"


class TestDryRun:
    """dry_run=True must not persist any state and must report 'simulated'.

    Note: current behaviour treats any non-'success' sub-call status (including
    'simulated') as a sub-step failure. The characterization below therefore
    drives the 'no LP / no tokens' dry-run path, which is what the CLI exercises
    in preview mode, and uses status='success' sub-responses (connectors still
    return 'success' under dry_run=True; the outer 'simulated' comes from the
    teardown state-machine itself, not the sub-tools).
    """

    @pytest.mark.asyncio
    async def test_dry_run_does_not_persist_state(self, executor, mock_gateway):
        _set_state(mock_gateway, {"phase": "running", "lp_position_id": None})

        async def fake_execute(name, args):
            # dry_run must propagate to every sub-tool
            assert args.get("dry_run") is True, f"{name} missing dry_run"
            if name == "settle_vault":
                return ToolResponse(status="success", data={"tx_hash": None})
            return ToolResponse(status="error", error={"message": "unmocked"})

        with (
            patch.object(executor, "execute", side_effect=fake_execute),
            patch("almanak.framework.connectors.lagoon.sdk.LagoonVaultSDK", return_value=_make_sdk()),
        ):
            result = await executor._execute_teardown_vault(_args(dry_run=True))

        assert result.status == "simulated"
        assert result.data["status"] == "simulated"
        # Dry-run must never touch the state store.
        assert mock_gateway.state.SaveState.call_count == 0


class TestStateLoadResilience:
    """LoadState failures must not crash teardown; we start with clean state."""

    @pytest.mark.asyncio
    async def test_load_state_raises_starts_from_clean_state(self, executor, mock_gateway):
        mock_gateway.state.LoadState.side_effect = RuntimeError("state backend down")
        mock_gateway.state.SaveState.return_value = MagicMock(success=True)

        async def fake_execute(name, args):
            if name == "settle_vault":
                return ToolResponse(status="success", data={"tx_hash": "0xsettle"})
            return ToolResponse(status="error", error={"message": "unmocked"})

        with (
            patch.object(executor, "execute", side_effect=fake_execute),
            patch("almanak.framework.connectors.lagoon.sdk.LagoonVaultSDK", return_value=_make_sdk()),
        ):
            result = await executor._execute_teardown_vault(_args())

        # No LP, no tokens in (empty) state -> only settle runs.
        assert result.status == "success"
        assert result.data["positions_closed"] == 0
        assert result.data["swaps_executed"] == 0

    @pytest.mark.asyncio
    async def test_save_state_failure_does_not_abort_flow(self, executor, mock_gateway):
        _set_state(mock_gateway, {"phase": "running", "lp_position_id": None})
        mock_gateway.state.SaveState.side_effect = RuntimeError("write failed")

        async def fake_execute(name, args):
            if name == "settle_vault":
                return ToolResponse(status="success", data={"tx_hash": "0xsettle"})
            return ToolResponse(status="error", error={"message": "unmocked"})

        with (
            patch.object(executor, "execute", side_effect=fake_execute),
            patch("almanak.framework.connectors.lagoon.sdk.LagoonVaultSDK", return_value=_make_sdk()),
        ):
            result = await executor._execute_teardown_vault(_args())

        assert result.status == "success"


class TestUnderlyingAndNavResilience:
    """Underlying-token / NAV lookup failures must degrade gracefully."""

    @pytest.mark.asyncio
    async def test_underlying_lookup_fails_skips_swap_phase(self, executor, mock_gateway):
        _set_state(
            mock_gateway,
            {"phase": "running", "lp_position_id": None, "token_a": TOKEN_A},
        )

        sdk = MagicMock()
        sdk.get_underlying_token_address.side_effect = RuntimeError("bad vault")
        sdk.get_total_assets.return_value = 500

        async def fake_execute(name, args):
            if name == "swap_tokens":
                raise AssertionError("swap must be skipped when underlying unknown")
            if name == "settle_vault":
                return ToolResponse(status="success", data={"tx_hash": "0xsettle"})
            return ToolResponse(status="error", error={"message": "unmocked"})

        with (
            patch.object(executor, "execute", side_effect=fake_execute),
            patch("almanak.framework.connectors.lagoon.sdk.LagoonVaultSDK", return_value=sdk),
        ):
            result = await executor._execute_teardown_vault(_args())

        assert result.status == "success"
        assert result.data["swaps_executed"] == 0
        assert result.data["final_nav"] == "500"

    @pytest.mark.asyncio
    async def test_final_nav_lookup_fails_returns_zero(self, executor, mock_gateway):
        _set_state(mock_gateway, {"phase": "running", "lp_position_id": None})

        async def fake_execute(name, args):
            if name == "settle_vault":
                return ToolResponse(status="success", data={"tx_hash": "0xsettle"})
            return ToolResponse(status="error", error={"message": "unmocked"})

        with (
            patch.object(executor, "execute", side_effect=fake_execute),
            patch(
                "almanak.framework.connectors.lagoon.sdk.LagoonVaultSDK",
                return_value=_make_sdk(raise_total_assets=True),
            ),
        ):
            result = await executor._execute_teardown_vault(_args())

        assert result.status == "success"
        assert result.data["final_nav"] == "0"


class TestUnderlyingTokenExclusion:
    """The underlying token itself must never appear in the swap set."""

    @pytest.mark.asyncio
    async def test_tokens_matching_underlying_case_insensitive_are_excluded(
        self, executor, mock_gateway
    ):
        # token_a matches underlying (different case), token_b is different.
        _set_state(
            mock_gateway,
            {
                "phase": "running",
                "lp_position_id": None,
                "token_a": UNDERLYING.upper(),
                "token_b": TOKEN_B,
            },
        )

        swap_targets: list[str] = []

        async def fake_execute(name, args):
            if name == "get_balance":
                swap_targets.append(args["token"])
                return ToolResponse(status="success", data={"balance": "1"})
            if name == "swap_tokens":
                return ToolResponse(status="success", data={"tx_hash": "0xswap"})
            if name == "settle_vault":
                return ToolResponse(status="success", data={"tx_hash": "0xsettle"})
            return ToolResponse(status="error", error={"message": "unmocked"})

        with (
            patch.object(executor, "execute", side_effect=fake_execute),
            patch("almanak.framework.connectors.lagoon.sdk.LagoonVaultSDK", return_value=_make_sdk()),
        ):
            result = await executor._execute_teardown_vault(_args())

        assert result.status == "success"
        assert UNDERLYING.upper() not in swap_targets
        assert TOKEN_B in swap_targets
