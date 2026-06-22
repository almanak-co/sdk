"""VIB-5346: runner WEI lane for fungible-LP close chaining.

Layer-2 coverage for ``StrategyRunner._run_single_chain_intents`` /
``_resolve_chained_amount_for_intent``:

* A ``[LP_OPEN(result LPOpenData.liquidity=N), LP_CLOSE(amount="all")]``
  sequence captures the minted-LP wei into the dedicated WEI lane and resolves
  the LP_CLOSE ``position_id`` to ``str(N)`` — WITHOUT touching the swap-output
  lane (``previous_amount_received``) or ``swap_amounts``.
* A ``LP_CLOSE(amount="all")`` with no prior LP_OPEN fails the step with
  COMPILATION_FAILED carrying the LP-specific message.

The two lanes are strictly separate by design: the swap-output lane only ever
reads ``swap_amounts``, the WEI lane only ever reads ``LPOpenData.liquidity``.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.intents.vocabulary import (
    Intent,
    IntentType,
    LPCloseIntent,
    LPOpenIntent,
)
from almanak.framework.runner.strategy_runner import (
    IterationResult,
    IterationStatus,
    RunIterationState,
    RunnerConfig,
    StrategyRunner,
)

_MINTED_WEI = 1_200_000_000_000_000_000


def _build_runner(dry_run: bool = False) -> StrategyRunner:
    config = RunnerConfig(
        default_interval_seconds=1,
        enable_state_persistence=False,
        enable_alerting=False,
        dry_run=dry_run,
    )
    return StrategyRunner(
        price_oracle=MagicMock(),
        balance_provider=MagicMock(),
        execution_orchestrator=MagicMock(),
        state_manager=MagicMock(),
        config=config,
        circuit_breaker=None,
    )


def _build_state(intents, deployment_id="vib-5346-test") -> RunIterationState:
    strategy = MagicMock()
    strategy.deployment_id = deployment_id
    strategy.chain = "arbitrum"
    strategy.wallet_address = "0x" + "ab" * 20
    state = RunIterationState(
        strategy=strategy,
        deployment_id=deployment_id,
        start_time=datetime.now(UTC),
    )
    state.intents = list(intents)
    state.market = MagicMock()
    return state


def _lp_open_success(intent, deployment_id, liquidity):
    """An LP_OPEN success result whose execution_result carries LPOpenData.

    ``swap_amounts`` is None so the swap-output lane stays empty; only the WEI
    lane (LPOpenData.liquidity) is populated.
    """
    lp_open_data = SimpleNamespace(liquidity=liquidity)
    return IterationResult(
        status=IterationStatus.SUCCESS,
        intent=intent,
        deployment_id=deployment_id,
        duration_ms=1,
        execution_result=SimpleNamespace(swap_amounts=None, lp_open_data=lp_open_data),
    )


def _lp_close_success(intent, deployment_id):
    return IterationResult(
        status=IterationStatus.SUCCESS,
        intent=intent,
        deployment_id=deployment_id,
        duration_ms=1,
        execution_result=SimpleNamespace(swap_amounts=None, lp_open_data=None),
    )


@pytest.mark.asyncio
async def test_lp_open_then_close_all_resolves_position_id_via_wei_lane() -> None:
    lp_open = LPOpenIntent(
        pool="PT-stuff/SY",
        amount0=Decimal("100"),
        amount1=Decimal("0"),
        range_lower=Decimal("1"),
        range_upper=Decimal("2"),
        protocol="pendle",
    )
    lp_close = LPCloseIntent(position_id="0", pool="0x" + "1" * 40, protocol="pendle", amount="all")
    runner = _build_runner()
    state = _build_state([lp_open, lp_close])

    captured: list = []

    async def _fake_execute(*, strategy, intent, start_time, total_intents, market, record_metrics):
        captured.append(intent)
        if intent.intent_type == IntentType.LP_OPEN:
            return _lp_open_success(intent, state.deployment_id, _MINTED_WEI)
        return _lp_close_success(intent, state.deployment_id)

    with patch.object(runner, "_execute_single_chain", new=AsyncMock(side_effect=_fake_execute)):
        await runner._run_single_chain_intents(state)

    # Both steps dispatched.
    assert len(captured) == 2
    resolved_close = captured[1]
    assert resolved_close.intent_type == IntentType.LP_CLOSE
    # WEI lane resolved minted liquidity into position_id; marker cleared.
    assert resolved_close.position_id == str(_MINTED_WEI)
    assert resolved_close.amount is None
    assert int(resolved_close.position_id) == _MINTED_WEI


def test_resolver_uses_wei_lane_not_swap_lane() -> None:
    """``_resolve_chained_amount_for_intent`` resolves an LP_CLOSE from the WEI
    lane and ignores ``previous_amount_received`` entirely."""
    runner = _build_runner()
    strategy = SimpleNamespace(deployment_id="vib-5346-test")
    intent = LPCloseIntent(position_id="0", protocol="pendle", amount="all")

    resolved, early, should_continue = runner._resolve_chained_amount_for_intent(
        intent=intent,
        idx=1,
        intents=[MagicMock(), intent],
        is_multi_intent=True,
        previous_amount_received=Decimal("999.99"),  # swap lane is set but MUST be ignored
        previous_lp_minted_wei=_MINTED_WEI,
        market=MagicMock(),
        strategy=strategy,
        start_time=datetime.now(UTC),
    )

    assert early is None
    assert should_continue is False
    # Resolved from the WEI lane (minted liquidity), NOT the stale swap value.
    assert resolved.position_id == str(_MINTED_WEI)
    assert resolved.amount is None


def test_close_all_without_prior_lp_open_fails_compilation() -> None:
    runner = _build_runner(dry_run=False)
    strategy = SimpleNamespace(deployment_id="vib-5346-test")
    intent = LPCloseIntent(position_id="0", protocol="pendle", amount="all")

    resolved, early, should_continue = runner._resolve_chained_amount_for_intent(
        intent=intent,
        idx=0,
        intents=[intent],
        is_multi_intent=False,
        previous_amount_received=None,
        previous_lp_minted_wei=None,
        market=MagicMock(),
        strategy=strategy,
        start_time=datetime.now(UTC),
    )

    assert early is not None
    assert early.status == IterationStatus.COMPILATION_FAILED
    assert "no prior LP_OPEN minted-LP amount available" in (early.error or "")
    assert should_continue is False  # break


def test_close_all_without_prior_lp_open_dry_run_skips() -> None:
    runner = _build_runner(dry_run=True)
    strategy = SimpleNamespace(deployment_id="vib-5346-test")
    intent = LPCloseIntent(position_id="0", protocol="pendle", amount="all")

    resolved, early, should_continue = runner._resolve_chained_amount_for_intent(
        intent=intent,
        idx=0,
        intents=[intent],
        is_multi_intent=False,
        previous_amount_received=None,
        previous_lp_minted_wei=None,
        market=MagicMock(),
        strategy=strategy,
        start_time=datetime.now(UTC),
    )

    assert early is not None
    assert early.status == IterationStatus.DRY_RUN
    assert should_continue is True  # continue


@pytest.mark.asyncio
async def test_close_all_result_has_no_swap_amounts() -> None:
    """Ledger safety: the LP_CLOSE(all) close result never enters the swap path,
    so its ``swap_amounts`` stays None (the swap ledger lane is not triggered)."""
    lp_open = LPOpenIntent(
        pool="PT-stuff/SY",
        amount0=Decimal("100"),
        amount1=Decimal("0"),
        range_lower=Decimal("1"),
        range_upper=Decimal("2"),
        protocol="pendle",
    )
    lp_close = LPCloseIntent(position_id="0", pool="0x" + "1" * 40, protocol="pendle", amount="all")
    runner = _build_runner()
    state = _build_state([lp_open, lp_close])

    close_results: list = []

    async def _fake_execute(*, strategy, intent, start_time, total_intents, market, record_metrics):
        if intent.intent_type == IntentType.LP_OPEN:
            return _lp_open_success(intent, state.deployment_id, _MINTED_WEI)
        res = _lp_close_success(intent, state.deployment_id)
        close_results.append(res)
        return res

    with patch.object(runner, "_execute_single_chain", new=AsyncMock(side_effect=_fake_execute)):
        await runner._run_single_chain_intents(state)

    assert len(close_results) == 1
    assert close_results[0].execution_result.swap_amounts is None


# ---------------------------------------------------------------------------
# VIB-5346 fail-closed runner capability gate (NFT/identity connectors).
#
# The PRIMARY control: an NFT-identity LP connector (position_id IS an NFT
# token-id / pool address, NOT a fungible LP-token wei amount) must be REJECTED
# by the runner BEFORE the minted-LP wei is resolved into ``position_id``. The
# pre-fix bug: the runner unconditionally resolved minted-liquidity wei into the
# NFT ``position_id`` slot for ANY protocol, so the connector compiler guards
# (which only see the DIRECT-compile path) never fired on the runner-chained
# path. aerodrome_slipstream is the worst case — its compiler VALIDATES
# position_id is a numeric token-id, so it would ACCEPT the garbage minted-wei.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "protocol",
    ["uniswap_v3", "aerodrome_slipstream"],
)
async def test_lp_open_then_close_all_nft_protocol_rejected_before_resolution(
    protocol: str,
) -> None:
    """An NFT-identity LP connector's LP_CLOSE amount="all" must FAIL compilation
    at the runner gate, and the minted-LP wei must NEVER be written into
    ``position_id`` (the close is rejected, not resolved)."""
    lp_open = LPOpenIntent(
        pool="0x" + "2" * 40,
        amount0=Decimal("100"),
        amount1=Decimal("0"),
        range_lower=Decimal("1"),
        range_upper=Decimal("2"),
        protocol=protocol,
    )
    lp_close = LPCloseIntent(position_id="0", pool="0x" + "1" * 40, protocol=protocol, amount="all")
    runner = _build_runner()
    state = _build_state([lp_open, lp_close])

    dispatched: list = []

    async def _fake_execute(*, strategy, intent, start_time, total_intents, market, record_metrics):
        dispatched.append(intent)
        if intent.intent_type == IntentType.LP_OPEN:
            return _lp_open_success(intent, state.deployment_id, _MINTED_WEI)
        # If the runner ever dispatches the LP_CLOSE for an NFT protocol the
        # gate has already failed — record it so the assertion below catches it.
        return _lp_close_success(intent, state.deployment_id)

    with patch.object(runner, "_execute_single_chain", new=AsyncMock(side_effect=_fake_execute)):
        results = await runner._run_single_chain_intents(state)

    # The LP_OPEN dispatched; the LP_CLOSE was NEVER dispatched (rejected at the
    # runner gate before resolution / execution).
    close_dispatched = [i for i in dispatched if i.intent_type == IntentType.LP_CLOSE]
    assert close_dispatched == [], (
        f"NFT protocol {protocol} LP_CLOSE was dispatched to execution; "
        "the fail-closed runner gate did not reject it"
    )

    # The runner produced a COMPILATION_FAILED result for the LP_CLOSE step with
    # the fail-closed capability-gate message.
    result_list = results if isinstance(results, list) else [results]
    close_results = [
        r
        for r in result_list
        if getattr(getattr(r, "intent", None), "intent_type", None) == IntentType.LP_CLOSE
    ]
    assert close_results, f"no LP_CLOSE result recorded for {protocol}"
    close_res = close_results[-1]
    assert close_res.status == IterationStatus.COMPILATION_FAILED
    assert "not supported for protocol" in (close_res.error or "")
    # CRUCIAL: the minted-LP wei was NEVER resolved into position_id; the close
    # intent on the result is the original (position_id still "0"), proving the
    # close was rejected rather than silently re-pointed at garbage.
    assert close_res.intent.position_id == "0", (
        f"minted-LP wei leaked into NFT position_id for {protocol}: "
        f"{close_res.intent.position_id!r}"
    )


def test_resolver_rejects_nft_protocol_amount_all_before_resolution() -> None:
    """Unit-level: ``_resolve_chained_amount_for_intent`` rejects an NFT-protocol
    LP_CLOSE even when a prior minted-LP wei IS available — the wei must not be
    resolved into the NFT position_id slot."""
    runner = _build_runner(dry_run=False)
    strategy = SimpleNamespace(deployment_id="vib-5346-test")
    intent = LPCloseIntent(position_id="0", protocol="aerodrome_slipstream", amount="all")

    resolved, early, should_continue = runner._resolve_chained_amount_for_intent(
        intent=intent,
        idx=1,
        intents=[MagicMock(), intent],
        is_multi_intent=True,
        previous_amount_received=None,
        previous_lp_minted_wei=_MINTED_WEI,  # wei IS available — must still reject
        market=MagicMock(),
        strategy=strategy,
        start_time=datetime.now(UTC),
    )

    assert early is not None
    assert early.status == IterationStatus.COMPILATION_FAILED
    assert "not supported for protocol" in (early.error or "")
    assert should_continue is False  # break
    # The returned intent is the untouched original — wei NOT resolved in.
    assert resolved.position_id == "0"
