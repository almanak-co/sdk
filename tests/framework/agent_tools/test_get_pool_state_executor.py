"""ToolExecutor routing + policy enforcement for get_pool_state (VIB-4727 D3.F7).

The ``get_pool_state`` tool is the LLM-facing surface that ultimately
returns pool analytics from the gateway. Per AGENTS.md "Agent-tools rule"
+ UAT-GATE.md "Domain-specific hard rules", every LLM-mediated tool —
including read-only ones with ``RiskTier.NONE`` — MUST route through
``ToolExecutor.execute()``. Spend gates are deliberately skipped for
``RiskTier.NONE``; tool/chain/rate-limit/circuit checks are not.

These tests assert the user-visible side effects of that routing
(audit-log emission, rate-limit RateLimited outcome) rather than only
inspecting internal counters. The fourth test is a structural anti-
bypass guard: a grep proves no other call site invokes
``_execute_get_pool_state`` outside ``executor.py``.
"""

from __future__ import annotations

import asyncio
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.agent_tools.catalog import RiskTier
from almanak.framework.agent_tools.executor import ToolExecutor
from almanak.framework.agent_tools.policy import AgentPolicy
from almanak.framework.agent_tools.schemas import ToolResponse
from almanak.framework.agent_tools.tracing import DecisionTracer

_BASE_ARGS = {
    "token_a": "USDC",
    "token_b": "WETH",
    "fee_tier": 500,
    "chain": "arbitrum",
    "protocol": "uniswap_v3",
    # An explicit pool_address shortcuts factory.getPool() so the test
    # doesn't depend on a real gateway RPC even if the dispatch path is
    # invoked accidentally.
    "pool_address": "0xc6962004f452be9203591991d15f6b388e09e8d0",
}


def _make_executor() -> ToolExecutor:
    """Build a ToolExecutor with a stubbed gateway and a focused policy."""
    gateway = MagicMock()
    # Default policy keeps the conservative spend defaults; allow arbitrum
    # (the default) so chain check passes.
    return ToolExecutor(
        gateway_client=gateway,
        policy=AgentPolicy(),
        wallet_address="0x0000000000000000000000000000000000000001",
        deployment_id="test-deployment",
        default_chain="arbitrum",
        tracer=DecisionTracer(),
    )


# ============================================================================
# D3.F7 Test 1 — Audit-log emission + RiskTier.NONE routing
# ============================================================================


def test_execute_get_pool_state_emits_audit_log_and_skips_spend_check():
    """User-visible audit-log surface contains exactly one record for the
    call; PolicyEngine.check was reached; ``_check_spend_limits`` was NOT
    called (RiskTier.NONE skips spend gates by contract)."""
    executor = _make_executor()

    # Stub the actual data-tool dispatch so the test doesn't hit a real
    # gateway RPC. The routing-through-execute is the assertion target,
    # not the RPC payload.
    async def fake_dispatch(_name: str, _args: dict) -> ToolResponse:
        return ToolResponse(status="success", data={"pool_address": _BASE_ARGS["pool_address"]})

    with (
        patch.object(executor, "_dispatch_data", new=fake_dispatch),
        patch.object(
            executor._policy_engine,
            "_check_spend_limits",
            wraps=executor._policy_engine._check_spend_limits,
        ) as spend_spy,
    ):
        result = asyncio.run(executor.execute("get_pool_state", dict(_BASE_ARGS)))

    assert result.status == "success"

    # User-visible audit log: exactly one record for this call.
    entries = executor._tracer.get_entries()
    assert len(entries) == 1
    entry = entries[0]
    assert entry.tool_name == "get_pool_state"
    assert entry.policy_result is not None
    assert entry.policy_result["allowed"] is True  # RiskTier.NONE -> allowed

    # Spend-limit check was deliberately skipped because RiskTier.NONE.
    spend_spy.assert_not_called()

    # Confirm the tool is in fact RiskTier.NONE (the contract this test verifies).
    tool_def = executor._catalog.get("get_pool_state")
    assert tool_def is not None
    assert tool_def.risk_tier == RiskTier.NONE


# ============================================================================
# D3.F7 Test 2 — Rate-limit user-visible outcome
# ============================================================================


def test_rate_limit_saturation_returns_rate_limited_response():
    """A saturated tool-call rate-limit fixture causes ``ToolExecutor.execute(...)``
    to return a rate-limit error (positive proof the rate limiter is in the
    routing path; without ToolExecutor.execute() it would NEVER hit)."""
    import time

    executor = _make_executor()

    # Saturate the rate limit: pre-fill ``max_tool_calls_per_minute`` recent
    # timestamps so the next ``_check_rate_limits`` rejects the call.
    cap = executor._policy_engine.policy.max_tool_calls_per_minute
    now = time.time()
    executor._policy_engine._tool_calls_this_minute = [now - 1.0] * cap

    async def fake_dispatch(_name: str, _args: dict) -> ToolResponse:
        pytest.fail("dispatch must NOT be reached when rate-limit is saturated")

    with patch.object(executor, "_dispatch_data", new=fake_dispatch):
        result = asyncio.run(executor.execute("get_pool_state", dict(_BASE_ARGS)))

    assert result.status == "error"
    # Error envelope mentions the rate-limit (user-visible signal).
    assert "rate limit" in (result.error.get("message", "") if isinstance(result.error, dict) else str(result.error)).lower()

    # Audit log captured the denial.
    entries = executor._tracer.get_entries()
    assert len(entries) == 1
    assert entries[0].policy_result is not None
    assert entries[0].policy_result["allowed"] is False


# ============================================================================
# D3.F7 Test 3 — Static anti-bypass: no direct callers of _execute_get_pool_state
# ============================================================================


def test_no_direct_callers_of_execute_get_pool_state_outside_executor():
    """Anti-bypass guard: ``_execute_get_pool_state`` must only be called
    by ``ToolExecutor._dispatch_data`` (inside ``executor.py``). A direct
    caller anywhere else would bypass policy / rate-limit / audit-log.
    """
    repo_root = Path(__file__).resolve().parents[3]
    # ``git grep`` is the cheapest and most portable scanner; falls back to
    # ``grep -rn`` if not in a git checkout.
    # Restrict to Python sources: doc/markdown mentions of the symbol are
    # expected (UAT card, design discussions) and never bypass the executor.
    cmd = ["git", "grep", "-n", "_execute_get_pool_state", "--", "*.py"]
    try:
        out = subprocess.run(
            cmd,
            cwd=repo_root,
            capture_output=True,
            text=True,
            check=False,
        )
        lines = out.stdout.splitlines()
        if out.returncode not in (0, 1):
            pytest.skip(f"git grep unavailable: {out.stderr.strip()}")
    except FileNotFoundError:
        pytest.skip("git not installed")

    # Acceptable references:
    #   * executor.py — the definition + the single dispatch call site.
    #   * this test file — naming the symbol to keep this guard live.
    # Anything else is a bypass.
    bad: list[str] = []
    for line in lines:
        if not line:
            continue
        path = line.split(":", 1)[0]
        if path.endswith("almanak/framework/agent_tools/executor.py"):
            continue
        if path.endswith("tests/framework/agent_tools/test_get_pool_state_executor.py"):
            continue
        bad.append(line)

    assert not bad, (
        "Found callers of _execute_get_pool_state outside executor.py.\n"
        "Any direct caller bypasses ToolExecutor.execute() and therefore\n"
        "the PolicyEngine + rate-limit + audit-log path.\n\n"
        + "\n".join(bad)
    )
