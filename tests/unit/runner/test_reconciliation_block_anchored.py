"""VIB-3350: reconciliation pins post-execution reads to the confirmed receipt block.

Companion to tests/gateway/test_web3_balance_provider_block_anchored.py (which
covers the provider). These cover the runner wiring: the post-read is pinned to
``_last_receipt_block(execution_result)``; when no receipt block is available the
read degrades to force-refresh "latest" and the report is flagged degraded.
"""

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.intents.vocabulary import SwapIntent
from almanak.framework.runner.reconciliation import BalanceSnapshot
from almanak.framework.runner.runner_models import RunnerConfig
from almanak.framework.runner.strategy_runner import StrategyRunner

RECEIPT_BLOCK = 21_000_000


def _make_runner(balance_provider, **config_kwargs):
    config = RunnerConfig(
        default_interval_seconds=1,
        enable_state_persistence=False,
        enable_alerting=False,
        dry_run=False,
        **config_kwargs,
    )
    return StrategyRunner(
        price_oracle=MagicMock(),
        balance_provider=balance_provider,
        execution_orchestrator=MagicMock(),
        state_manager=MagicMock(),
        config=config,
    )


def _swap_provider():
    """Provider whose post-balances reflect a completed USDC->ETH swap of 4."""
    usdc_bal, eth_bal = MagicMock(), MagicMock()
    usdc_bal.balance, eth_bal.balance = Decimal("96"), Decimal("4")
    bp = MagicMock()
    bp.get_balance = AsyncMock(
        side_effect=lambda t, *, force_refresh=False, as_of_block=None: usdc_bal if t == "USDC" else eth_bal
    )
    return bp


def _exec_result_with_block(block: int):
    """Shape accepted by ``_last_receipt_block``: dict tx with a receipt block."""
    return {"transaction_results": [{"success": True, "receipt": {"block_number": block}}]}


def _exec_result_bundle(*txs: tuple[bool, int]):
    """A multi-tx bundle: each (success, block) becomes a transaction_result."""
    return {"transaction_results": [{"success": ok, "receipt": {"block_number": blk}} for ok, blk in txs]}


def _strategy():
    strategy = MagicMock()
    strategy.deployment_id = "test"
    return strategy


@pytest.mark.asyncio
async def test_post_reads_pinned_to_receipt_block():
    """With a receipt block, post-reads pass as_of_block (not force_refresh)."""
    bp = _swap_provider()
    runner = _make_runner(bp)
    intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("4"))
    pre = BalanceSnapshot.now({"USDC": Decimal("100"), "ETH": Decimal("0")})

    recon = await runner._reconcile_post_execution_balances(
        _strategy(), intent, _exec_result_with_block(RECEIPT_BLOCK), pre_snapshot=pre
    )

    assert recon is not None
    assert recon["reconciliation_block"] == RECEIPT_BLOCK
    assert recon["reconciliation_degraded"] is False
    # every post-read was pinned to the receipt block
    for call in bp.get_balance.await_args_list:
        assert call.kwargs.get("as_of_block") == RECEIPT_BLOCK


@pytest.mark.asyncio
async def test_multi_tx_bundle_pins_to_last_successful_receipt_block():
    """VIB-3350 / PRD §3.4: for a multi-tx bundle the post-reads pin to the LAST
    SUCCESSFUL receipt block (state after the whole bundle landed) — and a later
    FAILED tx does not move the anchor off the last successful block."""
    bp = _swap_provider()
    runner = _make_runner(bp)
    intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("4"))
    pre = BalanceSnapshot.now({"USDC": Decimal("100"), "ETH": Decimal("0")})

    # Bundle: success@100, success@105, then a FAILED tx@110. Anchor must be 105.
    exec_result = _exec_result_bundle((True, 100), (True, 105), (False, 110))
    recon = await runner._reconcile_post_execution_balances(_strategy(), intent, exec_result, pre_snapshot=pre)

    assert recon["reconciliation_block"] == 105
    assert recon["reconciliation_degraded"] is False
    for call in bp.get_balance.await_args_list:
        assert call.kwargs.get("as_of_block") == 105


@pytest.mark.asyncio
async def test_failed_pinned_read_degrades_report():
    """VIB-3350 (Codex follow-up): if a pinned post-read FAILS for any intent
    token, the report is degraded — a partial pinned reconciliation must not look
    clean (a missing intent-token balance means we cannot prove block-anchoring,
    so enforcement must refuse to enforce against it)."""
    from almanak.framework.data.interfaces import DataSourceUnavailable

    usdc_bal = MagicMock()
    usdc_bal.balance = Decimal("96")

    async def gb(token, *, force_refresh=False, as_of_block=None):
        if token == "ETH":  # one intent token's pinned read fails
            raise DataSourceUnavailable(source="gateway", reason="pin not honored")
        return usdc_bal

    bp = MagicMock()
    bp.get_balance = AsyncMock(side_effect=gb)
    runner = _make_runner(bp)
    intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("4"))
    pre = BalanceSnapshot.now({"USDC": Decimal("100"), "ETH": Decimal("0")})

    recon = await runner._reconcile_post_execution_balances(
        _strategy(), intent, _exec_result_with_block(RECEIPT_BLOCK), pre_snapshot=pre
    )

    assert recon is not None  # USDC still read -> a report is produced
    assert recon["reconciliation_degraded"] is True  # ETH pinned read failed -> degraded


@pytest.mark.asyncio
async def test_degraded_when_no_receipt_block():
    """No receipt block -> cannot pin -> degraded flag + force-refresh fallback."""
    bp = _swap_provider()
    runner = _make_runner(bp)
    intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("4"))
    pre = BalanceSnapshot.now({"USDC": Decimal("100"), "ETH": Decimal("0")})

    # execution_result=None -> _last_receipt_block returns None
    recon = await runner._reconcile_post_execution_balances(_strategy(), intent, None, pre_snapshot=pre)

    assert recon is not None
    assert recon["reconciliation_block"] is None
    assert recon["reconciliation_degraded"] is True
    # fell back to force-refresh "latest"; no read was pinned
    for call in bp.get_balance.await_args_list:
        assert call.kwargs.get("as_of_block") in (None, 0)
        assert call.kwargs.get("force_refresh") is True


@pytest.mark.asyncio
async def test_pinned_falls_back_for_legacy_provider_and_degrades():
    """VIB-3350 (H2): a provider that predates as_of_block still reconciles, but
    because the post-read fell back to unpinned 'latest' the report MUST be
    flagged degraded — it must not claim a pin that never happened."""
    usdc_bal, eth_bal = MagicMock(), MagicMock()
    usdc_bal.balance, eth_bal.balance = Decimal("96"), Decimal("4")

    async def legacy_get_balance(token, *, force_refresh=False):  # no as_of_block kwarg
        return usdc_bal if token == "USDC" else eth_bal

    bp = MagicMock()
    bp.get_balance = AsyncMock(side_effect=legacy_get_balance)
    runner = _make_runner(bp)
    intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("4"))
    pre = BalanceSnapshot.now({"USDC": Decimal("100"), "ETH": Decimal("0")})

    recon = await runner._reconcile_post_execution_balances(
        _strategy(), intent, _exec_result_with_block(RECEIPT_BLOCK), pre_snapshot=pre
    )

    assert recon is not None  # reconciled despite the legacy provider
    # H2: the unpinned fallback is reported honestly, not as a clean pin.
    assert recon["reconciliation_degraded"] is True
    assert recon["reconciliation_pre_anchored"] is False
    kwargs_seen = [c.kwargs for c in bp.get_balance.await_args_list]
    # tried as_of_block first, then fell back to force_refresh
    assert any("as_of_block" in k for k in kwargs_seen), "should have tried pinning first"
    assert any("force_refresh" in k and "as_of_block" not in k for k in kwargs_seen), "should have fallen back"


# =============================================================================
# VIB-3350 Item 2: confirmation-depth wait before the pinned post-read
# =============================================================================


class TestResolveConfirmationDepth:
    """RunnerConfig.reconciliation_confirmation_depth -> effective per-chain depth."""

    def test_none_is_off(self):
        from almanak.framework.runner.runner_state import _resolve_confirmation_depth

        assert _resolve_confirmation_depth("ethereum", None) == 0

    def test_zero_is_off(self):
        from almanak.framework.runner.runner_state import _resolve_confirmation_depth

        assert _resolve_confirmation_depth("ethereum", 0) == 0

    def test_positive_override_applies_to_every_chain(self):
        from almanak.framework.runner.runner_state import _resolve_confirmation_depth

        assert _resolve_confirmation_depth("base", 4) == 4
        assert _resolve_confirmation_depth("ethereum", 4) == 4

    def test_negative_uses_per_chain_descriptor_depth(self):
        """`-1` reads ChainDescriptor.reorg_safe_depth (single source of truth)."""
        from almanak.framework.runner.runner_state import _resolve_confirmation_depth

        assert _resolve_confirmation_depth("ethereum", -1) == 12
        assert _resolve_confirmation_depth("polygon", -1) == 10
        assert _resolve_confirmation_depth("avalanche", -1) == 5
        # chains without a declared reorg_safe_depth fall back to the generic-L2 default
        assert _resolve_confirmation_depth("base", -1) == 3
        assert _resolve_confirmation_depth("arbitrum", -1) == 3
        assert _resolve_confirmation_depth(None, -1) == 3
        assert _resolve_confirmation_depth("not-a-real-chain", -1) == 3


class TestWaitForConfirmationDepth:
    @pytest.mark.asyncio
    async def test_no_wait_when_depth_zero(self):
        from almanak.framework.runner.runner_state import _wait_for_confirmation_depth

        bp = MagicMock()
        bp.get_block_number = AsyncMock(return_value=RECEIPT_BLOCK)
        confirmed, head = await _wait_for_confirmation_depth(bp, RECEIPT_BLOCK, 0, timeout_seconds=1)
        assert confirmed is True
        assert head is None
        bp.get_block_number.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_skips_when_provider_has_no_head_reader(self):
        from almanak.framework.runner.runner_state import _wait_for_confirmation_depth

        bp = MagicMock(spec=[])  # no get_block_number attribute
        confirmed, head = await _wait_for_confirmation_depth(bp, RECEIPT_BLOCK, 3, timeout_seconds=1)
        assert confirmed is True
        assert head is None

    @pytest.mark.asyncio
    async def test_confirms_once_head_reaches_target(self):
        from almanak.framework.runner.runner_state import _wait_for_confirmation_depth

        bp = MagicMock()
        # head lags, lags, then reaches receipt+3
        bp.get_block_number = AsyncMock(side_effect=[RECEIPT_BLOCK, RECEIPT_BLOCK + 1, RECEIPT_BLOCK + 3])
        confirmed, head = await _wait_for_confirmation_depth(
            bp, RECEIPT_BLOCK, 3, timeout_seconds=5, poll_interval_seconds=0.0
        )
        assert confirmed is True
        assert head == RECEIPT_BLOCK + 3
        assert bp.get_block_number.await_count == 3

    @pytest.mark.asyncio
    async def test_timeout_proceeds_unconfirmed(self):
        from almanak.framework.runner.runner_state import _wait_for_confirmation_depth

        bp = MagicMock()
        bp.get_block_number = AsyncMock(return_value=RECEIPT_BLOCK)  # never advances
        confirmed, head = await _wait_for_confirmation_depth(
            bp, RECEIPT_BLOCK, 3, timeout_seconds=0.0, poll_interval_seconds=0.0
        )
        assert confirmed is False
        assert head == RECEIPT_BLOCK

    @pytest.mark.asyncio
    async def test_head_read_failure_does_not_block(self):
        from almanak.framework.runner.runner_state import _wait_for_confirmation_depth

        bp = MagicMock()
        bp.get_block_number = AsyncMock(side_effect=RuntimeError("rpc down"))
        confirmed, head = await _wait_for_confirmation_depth(bp, RECEIPT_BLOCK, 3, timeout_seconds=5)
        assert confirmed is False
        assert head is None

    @pytest.mark.asyncio
    async def test_head_poll_is_bounded_by_remaining_budget(self):
        """VIB-3350 (CodeRabbit): each head poll receives a `timeout` bounded by
        the remaining wait budget so one stalled eth_blockNumber cannot outlive
        the caller deadline."""
        from almanak.framework.runner.runner_state import _wait_for_confirmation_depth

        bp = MagicMock()
        bp.get_block_number = AsyncMock(return_value=RECEIPT_BLOCK + 3)
        confirmed, head = await _wait_for_confirmation_depth(
            bp, RECEIPT_BLOCK, 3, timeout_seconds=5, poll_interval_seconds=0.0
        )
        assert confirmed is True
        passed_timeout = bp.get_block_number.await_args.kwargs.get("timeout")
        assert passed_timeout is not None
        assert 0.0 <= passed_timeout <= 5.0

    @pytest.mark.asyncio
    async def test_legacy_reader_without_timeout_kwarg_still_works(self):
        """A provider whose get_block_number predates the `timeout` kwarg must
        still be pollable via the no-arg fallback (TypeError-guarded)."""
        from almanak.framework.runner.runner_state import _wait_for_confirmation_depth

        calls = {"n": 0}

        async def legacy_get_block_number():  # no timeout kwarg
            calls["n"] += 1
            return RECEIPT_BLOCK + 3

        bp = MagicMock()
        bp.get_block_number = legacy_get_block_number
        confirmed, head = await _wait_for_confirmation_depth(
            bp, RECEIPT_BLOCK, 3, timeout_seconds=5, poll_interval_seconds=0.0
        )
        assert confirmed is True
        assert head == RECEIPT_BLOCK + 3
        assert calls["n"] == 1


@pytest.mark.asyncio
async def test_no_confirmation_wait_by_default():
    """Default config (depth None) -> no wait, confirmed flag stays None."""
    bp = _swap_provider()
    bp.get_block_number = AsyncMock(return_value=RECEIPT_BLOCK + 100)
    runner = _make_runner(bp)
    intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("4"), chain="base")
    pre = BalanceSnapshot.now({"USDC": Decimal("100"), "ETH": Decimal("0")})

    recon = await runner._reconcile_post_execution_balances(
        _strategy(), intent, _exec_result_with_block(RECEIPT_BLOCK), pre_snapshot=pre
    )

    assert recon["reconciliation_confirmation_depth"] == 0
    assert recon["reconciliation_confirmed"] is None
    assert recon["reconciliation_head_block"] is None
    bp.get_block_number.assert_not_awaited()  # no head poll when wait is off


@pytest.mark.asyncio
async def test_confirmation_wait_runs_when_configured():
    """With depth configured + head already ahead, the wait confirms immediately
    and the reads are still pinned to the receipt block."""
    bp = _swap_provider()
    bp.get_block_number = AsyncMock(return_value=RECEIPT_BLOCK + 10)
    runner = _make_runner(bp, reconciliation_confirmation_depth=3)
    intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("4"), chain="base")
    pre = BalanceSnapshot.now({"USDC": Decimal("100"), "ETH": Decimal("0")})

    recon = await runner._reconcile_post_execution_balances(
        _strategy(), intent, _exec_result_with_block(RECEIPT_BLOCK), pre_snapshot=pre
    )

    assert recon["reconciliation_confirmation_depth"] == 3
    assert recon["reconciliation_confirmed"] is True
    assert recon["reconciliation_head_block"] == RECEIPT_BLOCK + 10
    bp.get_block_number.assert_awaited()
    # reads still pinned to the receipt block
    for call in bp.get_balance.await_args_list:
        assert call.kwargs.get("as_of_block") == RECEIPT_BLOCK


@pytest.mark.asyncio
async def test_confirmation_wait_timeout_still_reconciles():
    """Head never advances -> wait times out -> reconciliation proceeds with the
    pinned read and is flagged unconfirmed (not degraded — still pinned)."""
    bp = _swap_provider()
    bp.get_block_number = AsyncMock(return_value=RECEIPT_BLOCK)  # never reaches +3
    runner = _make_runner(
        bp,
        reconciliation_confirmation_depth=3,
        reconciliation_confirmation_timeout_seconds=0.0,
    )
    intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("4"), chain="base")
    pre = BalanceSnapshot.now({"USDC": Decimal("100"), "ETH": Decimal("0")})

    recon = await runner._reconcile_post_execution_balances(
        _strategy(), intent, _exec_result_with_block(RECEIPT_BLOCK), pre_snapshot=pre
    )

    assert recon["reconciliation_confirmed"] is False
    assert recon["reconciliation_degraded"] is False  # still pinned to receipt block
    assert recon["reconciliation_block"] == RECEIPT_BLOCK
    for call in bp.get_balance.await_args_list:
        assert call.kwargs.get("as_of_block") == RECEIPT_BLOCK


# =============================================================================
# VIB-3350 field-report regression — Base USDC->WETH false incident
#
# Reported on base-clp-full (a9e54a85), SDK 2.16.1rc9 (pre-fix). A SWAP of
# ~$4.31 USDC -> WETH executed and parsed successfully (tx 0xcb92..dae0), yet
# reconciliation logged an incident because BOTH legs read actual delta = 0:
#
#   USDC actual 0.000000, expected [-4.326784290, -4.283731710]
#   WETH actual 0E-17,    expected [0.002158482570528123105, 0.002180175862694234895]
#
# delta == 0 on both legs == the post-read was served PRE-tx state by a lagging
# replica answering "latest". These tests reproduce that exact case on Base and
# prove (A) pinning the post-read to the receipt block resolves it, and (B) the
# unpinned path that produced the field report is now at least flagged degraded
# so the enforcement gate refuses to enforce against it.
# =============================================================================

# Enriched in/out amounts implied by the field-report expected ranges (midpoints
# of the [-4.3268, -4.2837] USDC and [0.0021585, 0.0021802] WETH bands, 0.5% slip).
_FIELD_AMOUNT_IN_USDC = Decimal("4.305258")
_FIELD_AMOUNT_OUT_WETH = Decimal("0.00216932921661118")
# A clean pre-swap baseline; the swap moves exactly amount_in / amount_out.
_PRE_USDC = Decimal("100")
_PRE_WETH = Decimal("0")
_POST_USDC = _PRE_USDC - _FIELD_AMOUNT_IN_USDC  # 95.694742 (true post-tx)
_POST_WETH = _PRE_WETH + _FIELD_AMOUNT_OUT_WETH  # 0.0021693… (true post-tx)


def _field_swap_intent() -> SwapIntent:
    return SwapIntent(from_token="USDC", to_token="WETH", amount=_FIELD_AMOUNT_IN_USDC, chain="base")


def _field_exec_result(block: int) -> SimpleNamespace:
    """Execution result with enriched swap_amounts (so expected ranges populate)
    and a receipt at ``block`` — the shape build_reconciliation_report consumes."""
    return SimpleNamespace(
        transaction_results=[{"success": True, "receipt": {"block_number": block}}],
        swap_amounts=SimpleNamespace(
            amount_in_decimal=_FIELD_AMOUNT_IN_USDC,
            amount_out_decimal=_FIELD_AMOUNT_OUT_WETH,
        ),
        total_gas_cost_wei=0,  # USDC is not the native gas token -> bound not stretched
    )


def _bal(value: Decimal) -> MagicMock:
    b = MagicMock()
    b.balance = value
    return b


@pytest.mark.asyncio
async def test_base_field_stale_latest_pin_resolves_false_incident():
    """A lagging-replica provider answers "latest" with PRE-tx state but answers
    the pinned receipt block with the true POST-tx state. With the post-reads
    pinned (default), the deltas land in their expected ranges and NO incident is
    raised — the field-report false positive is resolved at its root."""

    def get_balance(token, *, force_refresh=False, as_of_block=None):
        pinned = as_of_block is not None and as_of_block > 0
        if token == "USDC":
            return _bal(_POST_USDC if pinned else _PRE_USDC)  # latest == stale pre-tx
        return _bal(_POST_WETH if pinned else _PRE_WETH)

    bp = MagicMock()
    bp.get_balance = AsyncMock(side_effect=get_balance)
    runner = _make_runner(bp)
    pre = BalanceSnapshot.now({"USDC": _PRE_USDC, "WETH": _PRE_WETH})

    recon = await runner._reconcile_post_execution_balances(
        _strategy(), _field_swap_intent(), _field_exec_result(RECEIPT_BLOCK), pre_snapshot=pre
    )

    assert recon is not None
    # The pin observed the just-landed swap -> real deltas, not the stale zeros.
    assert recon["actual_deltas"]["USDC"] == str(-_FIELD_AMOUNT_IN_USDC)
    assert recon["actual_deltas"]["WETH"] == str(_FIELD_AMOUNT_OUT_WETH)
    assert recon["incident"] is False  # no false incident
    assert not recon["mismatches"]
    assert recon["reconciliation_block"] == RECEIPT_BLOCK
    assert recon["reconciliation_degraded"] is False  # clean pinned reconciliation
    # every post-read was pinned to the receipt block (not "latest")
    for call in bp.get_balance.await_args_list:
        assert call.kwargs.get("as_of_block") == RECEIPT_BLOCK


@pytest.mark.asyncio
async def test_base_field_unpinned_latest_reproduces_incident_but_degrades():
    """Reproduce the pre-fix field report: a legacy provider that can only answer
    "latest" and is served stale PRE-tx state. The zero-delta incident still
    surfaces (USDC/WETH actual == 0), BUT because the read could not be pinned the
    report is now flagged degraded — so the enforcement gate refuses to enforce
    against it (it cannot prove the read was anchored to the receipt block)."""

    async def legacy_get_balance(token, *, force_refresh=False):  # no as_of_block kwarg
        return _bal(_PRE_USDC if token == "USDC" else _PRE_WETH)  # always stale "latest"

    bp = MagicMock()
    bp.get_balance = AsyncMock(side_effect=legacy_get_balance)
    runner = _make_runner(bp)
    pre = BalanceSnapshot.now({"USDC": _PRE_USDC, "WETH": _PRE_WETH})

    recon = await runner._reconcile_post_execution_balances(
        _strategy(), _field_swap_intent(), _field_exec_result(RECEIPT_BLOCK), pre_snapshot=pre
    )

    assert recon is not None
    # Exactly the field-report signature: both legs read a zero delta.
    assert Decimal(recon["actual_deltas"]["USDC"]) == Decimal("0")
    assert Decimal(recon["actual_deltas"]["WETH"]) == Decimal("0")
    assert recon["incident"] is True  # the stale read still trips the check...
    # ...but the unpinned fallback is reported honestly so enforcement won't fire.
    assert recon["reconciliation_degraded"] is True
