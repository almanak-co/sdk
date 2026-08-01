"""A filled GMX close must settle on the first poll, with no baseline (VIB-6297).

Observed on Arbitrum mainnet: a `PERP_CLOSE` whose keeper filled in ~3s was reported
`OBSERVATION_FAILED` **360.079 seconds** later, on a close that had already succeeded.

The mechanism is not a slow read. `_capture_settlement_baseline` is reachable only while
every requested order is still pending, and an order that has left the pending set can
never re-enter it. So once any order is gone with no baseline captured, **no future poll
can ever capture one** and every subsequent poll is information-free. The barrier was not
waiting for anything — it re-derived the same dead verdict 68 times.

These tests drive the REAL barrier against the REAL connector, mocking only the two
gateway reads. Mocking the registry would test the mock.

Two assertions carry the bug, and both are needed:

- `attempts == 1` — the burn lives in the barrier's loop, so a status-only assertion
  passes on a fix that returns the right answer on poll #40.
- `asyncio.sleep` must never be called — this is what lets the test run against the REAL
  360s policy without risking a 360s hang. An information-free sleep fails immediately
  instead of being waited out.

The negative controls exist because the fail-safe polarity is asymmetric. A false
`SETTLED` on a close completes teardown with a live position — a silent strand. A false
`TERMINAL_FAILED` on an open makes a strategy believe an open failed while a position
exists, which can double the position. Neither is allowed; only pessimism about a *close*
is safe.
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from almanak.connectors._strategy_base.runner_hook_registry import AsyncSettlementStatus
from almanak.framework.runner.async_settlement import await_async_settlement

_KEY = "0x" + "ab" * 32
_SECOND_KEY = "0x" + "cd" * 32
_MARKET = "0x" + "11" * 20
_COLLATERAL = "0x" + "33" * 20
_RAW_USD = 10**30


def _order(*, order_id: str = _KEY, size_delta_usd: str = "100") -> SimpleNamespace:
    return SimpleNamespace(
        protocol="gmx_v2",
        order_id=order_id,
        kind=SimpleNamespace(value="DECREASE"),
        market=_MARKET,
        collateral_token=_COLLATERAL,
        is_long=True,
        size_delta_usd=Decimal(size_delta_usd),
    )


def _intent(intent_type: str) -> SimpleNamespace:
    return SimpleNamespace(intent_type=SimpleNamespace(value=intent_type))


def _pending(*keys: str) -> SimpleNamespace:
    return SimpleNamespace(ok=True, order_keys=list(keys), orders=[], truncated=False)


def _positions(*, size_usd: int) -> SimpleNamespace:
    """`size_usd=0` is a measured-flat account: the position is gone."""
    if size_usd <= 0:
        return SimpleNamespace(ok=True, positions=(), truncated=False)
    return SimpleNamespace(
        ok=True,
        truncated=False,
        positions=(
            SimpleNamespace(
                is_active=True,
                market=_MARKET,
                collateral_token=_COLLATERAL,
                is_long=True,
                size_in_usd=size_usd,
            ),
        ),
    )


async def _run(*, pending, positions, orders, intent_type: str):
    """Drive the real barrier and real connector; fail loudly on any sleep.

    A sleep here means the barrier decided a further poll could add information. When no
    baseline can ever be captured, that is false by construction, so the sleep is the bug.
    """

    async def _no_sleep(_seconds: float) -> None:
        raise AssertionError(
            "barrier slept while waiting for a baseline that can never be captured — "
            "the order has already left the pending set"
        )

    with (
        patch("almanak.connectors.gmx_v2.runner_hooks.read_pending_orders", return_value=pending),
        patch("almanak.connectors.gmx_v2.runner_hooks.read_open_positions", return_value=positions),
        patch("almanak.framework.runner.async_settlement.asyncio.sleep", _no_sleep),
    ):
        return await await_async_settlement(
            gateway_client=object(),
            chain="arbitrum",
            wallet_address="0xabc",
            network="mainnet",
            orders=orders,
            intent=_intent(intent_type),
        )


@pytest.mark.asyncio
async def test_filled_close_settles_on_the_first_poll_without_a_baseline() -> None:
    """The mainnet case: keeper filled before poll #1, account measured flat."""
    result = await _run(
        pending=_pending(),  # order already gone — no baseline was ever possible
        positions=_positions(size_usd=0),  # measured flat, matching the mid-wait `cast` probe
        orders=(_order(),),
        intent_type="PERP_CLOSE",
    )

    assert result.status == AsyncSettlementStatus.SETTLED
    assert result.terminal is True
    assert result.attempts == 1


@pytest.mark.asyncio
async def test_close_whose_position_is_still_open_is_never_reported_settled() -> None:
    """Negative control on the forbidden direction.

    The order is gone, yet the position is still there. Claiming SETTLED here completes a
    teardown over a live position — the silent strand.

    SPEC CORRECTED after the #3533 audit panel; this test previously asserted
    ``TERMINAL_FAILED``. It is a changed specification, not a silenced failure, and the
    reason is that ``c > 0`` is **not** decidable without a baseline: a close of delta
    ``d`` against size ``b`` that filled PERFECTLY leaves ``c = b - d > 0``, which is
    byte-identical in this read to a close that was cancelled. Asserting failure there
    was wrong, and the claim that "a retry of an already-done close is a cheap no-op"
    holds only for a FULL close — retrying a partial closes ``d`` again and OVER-CLOSES.

    So the only safe answer is an honest unknown, which is also the pre-existing
    behaviour. The assertion that matters is unchanged and is the one this test is for:
    never SETTLED.
    """

    async def _sleep(_seconds: float) -> None:
        return None

    with (
        patch("almanak.connectors.gmx_v2.runner_hooks.read_pending_orders", return_value=_pending()),
        patch(
            "almanak.connectors.gmx_v2.runner_hooks.read_open_positions",
            return_value=_positions(size_usd=100 * _RAW_USD),
        ),
        patch("almanak.framework.runner.async_settlement.asyncio.sleep", _sleep),
    ):
        result = await await_async_settlement(
            gateway_client=object(),
            chain="arbitrum",
            wallet_address="0xabc",
            network="mainnet",
            orders=(_order(),),
            intent=_intent("PERP_CLOSE"),
            timeout_seconds=2,
            poll_interval_seconds=1,
        )

    assert result.status is not AsyncSettlementStatus.SETTLED
    assert result.status is not AsyncSettlementStatus.TERMINAL_FAILED
    assert result.terminal is False


@pytest.mark.asyncio
async def test_a_flat_position_is_not_settled_while_a_sibling_order_is_still_live() -> None:
    """The gate the #3533 panel added, and the one my design note got wrong.

    I argued the optimistic branch needed no ``still_pending`` gate, "because if the
    position is already flat the goal holds regardless of what the sibling does". That is
    false. An unfilled GMX order **holds its collateral in the OrderVault**. Declaring the
    group settled lets teardown proceed to consolidation and sweep past that collateral,
    and blueprint 14 requires the exact accepted orders stay pending until terminal
    settlement. A flat position is not the whole goal state while a live order remains.

    Found independently by Codex (P1) and Grok on the same HEAD.
    """

    async def _sleep(_seconds: float) -> None:
        return None

    with (
        patch(
            "almanak.connectors.gmx_v2.runner_hooks.read_pending_orders",
            return_value=_pending(_SECOND_KEY),  # sibling still live
        ),
        patch(
            "almanak.connectors.gmx_v2.runner_hooks.read_open_positions",
            return_value=_positions(size_usd=0),  # target measured FLAT
        ),
        patch("almanak.framework.runner.async_settlement.asyncio.sleep", _sleep),
    ):
        result = await await_async_settlement(
            gateway_client=object(),
            chain="arbitrum",
            wallet_address="0xabc",
            network="mainnet",
            orders=(_order(), _order(order_id=_SECOND_KEY)),
            intent=_intent("PERP_CLOSE"),
            timeout_seconds=2,
            poll_interval_seconds=1,
        )

    assert result.status is not AsyncSettlementStatus.SETTLED, (
        "a live sibling order still holds collateral in the OrderVault — the group is not settled"
    )
    assert result.terminal is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "read",
    [
        SimpleNamespace(ok=True, positions=(), truncated=True),
        SimpleNamespace(ok=True, positions=()),  # producer did not say -> must fail CLOSED
    ],
    ids=["explicitly-truncated", "flag-absent"],
)
async def test_a_possibly_truncated_read_cannot_prove_absence(read) -> None:
    """Truncation must not read as "flat".

    The account read requests a fixed window, so a full page may have been cut
    short and a requested position beyond it would be missing from the measured
    sizes — manufacturing SETTLED over live exposure. The baseline path is immune
    (its ``b > 0`` guard makes truncation fail loud); this path is not.

    The ``flag-absent`` case pins the polarity of the default: a producer that does
    not report completeness must be treated as possibly-truncated, never as
    complete. For a consumer reasoning about ABSENCE, "False" is the fail-open
    value, so the missing-attribute default is ``True``.
    """

    async def _sleep(_seconds: float) -> None:
        return None

    with (
        patch("almanak.connectors.gmx_v2.runner_hooks.read_pending_orders", return_value=_pending()),
        patch("almanak.connectors.gmx_v2.runner_hooks.read_open_positions", return_value=read),
        patch("almanak.framework.runner.async_settlement.asyncio.sleep", _sleep),
    ):
        result = await await_async_settlement(
            gateway_client=object(),
            chain="arbitrum",
            wallet_address="0xabc",
            network="mainnet",
            orders=(_order(),),
            intent=_intent("PERP_CLOSE"),
            timeout_seconds=2,
            poll_interval_seconds=1,
        )

    assert result.status is not AsyncSettlementStatus.SETTLED
    assert result.terminal is False


@pytest.mark.asyncio
async def test_one_order_gone_while_another_is_pending_must_not_be_judged_yet() -> None:
    """The multi-order gate.

    This branch fires when *at least one* order has left the pending set, not all of them.
    A still-pending sibling may yet fill, so a pessimistic verdict here would be a false
    failure. Only the optimistic conclusion (position already flat) is safe while a
    sibling is outstanding — and it is not available here, because the position is open.

    Not reachable from the single-order mainnet trace; found by reasoning about the
    multi-order case, which is why it is pinned.

    This case is the one place where continuing to poll IS informative — the sibling's
    fate is still unknown — so it deliberately does not use the no-sleep harness. Waiting
    here is correct; waiting for an uncapturable baseline is not.
    """

    async def _sleep(_seconds: float) -> None:
        return None

    with (
        patch(
            "almanak.connectors.gmx_v2.runner_hooks.read_pending_orders",
            return_value=_pending(_SECOND_KEY),  # first order gone, second still pending
        ),
        patch(
            "almanak.connectors.gmx_v2.runner_hooks.read_open_positions",
            return_value=_positions(size_usd=100 * _RAW_USD),
        ),
        patch("almanak.framework.runner.async_settlement.asyncio.sleep", _sleep),
    ):
        result = await await_async_settlement(
            gateway_client=object(),
            chain="arbitrum",
            wallet_address="0xabc",
            network="mainnet",
            orders=(_order(), _order(order_id=_SECOND_KEY)),
            intent=_intent("PERP_CLOSE"),
            timeout_seconds=2,
            poll_interval_seconds=1,
        )

    assert result.status not in {
        AsyncSettlementStatus.SETTLED,
        AsyncSettlementStatus.TERMINAL_FAILED,
    }
    assert result.terminal is False


@pytest.mark.asyncio
async def test_open_without_a_baseline_is_never_judged_in_either_direction() -> None:
    """An open's target is a delta and is undecidable without a baseline.

    A pre-existing position of sufficient size satisfies any absolute reading while the
    order may in fact have been cancelled, so SETTLED would be a guess. TERMINAL_FAILED
    would be worse: a strategy that believes its open failed while a position exists can
    open a second one.

    This test asserts only that neither claim is made. It deliberately does NOT assert
    promptness — see the module docstring of the census test for why the undecidable case
    is still allowed to consume the barrier's budget.
    """

    async def _sleep(_seconds: float) -> None:
        return None

    with (
        patch("almanak.connectors.gmx_v2.runner_hooks.read_pending_orders", return_value=_pending()),
        patch(
            "almanak.connectors.gmx_v2.runner_hooks.read_open_positions",
            return_value=_positions(size_usd=500 * _RAW_USD),
        ),
        patch("almanak.framework.runner.async_settlement.asyncio.sleep", _sleep),
    ):
        result = await await_async_settlement(
            gateway_client=object(),
            chain="arbitrum",
            wallet_address="0xabc",
            network="mainnet",
            orders=(_order(),),
            intent=_intent("PERP_OPEN"),
            timeout_seconds=2,
            poll_interval_seconds=1,
        )

    assert result.status not in {
        AsyncSettlementStatus.SETTLED,
        AsyncSettlementStatus.TERMINAL_FAILED,
    }


@pytest.mark.asyncio
async def test_an_unreadable_position_state_stays_retryable_not_terminal() -> None:
    """Empty is not zero.

    A failed position read is UNMEASURED, not "measured flat". Treating it as flat would
    manufacture a SETTLED verdict out of a read error — the strand direction again. It
    must stay non-terminal, because unlike a missing baseline, a failed read genuinely can
    succeed on a later poll.
    """

    async def _sleep(_seconds: float) -> None:
        return None

    with (
        patch("almanak.connectors.gmx_v2.runner_hooks.read_pending_orders", return_value=_pending()),
        patch(
            "almanak.connectors.gmx_v2.runner_hooks.read_open_positions",
            return_value=SimpleNamespace(ok=False, positions=()),
        ),
        patch("almanak.framework.runner.async_settlement.asyncio.sleep", _sleep),
    ):
        result = await await_async_settlement(
            gateway_client=object(),
            chain="arbitrum",
            wallet_address="0xabc",
            network="mainnet",
            orders=(_order(),),
            intent=_intent("PERP_CLOSE"),
            timeout_seconds=2,
            poll_interval_seconds=1,
        )

    assert result.status is AsyncSettlementStatus.OBSERVATION_FAILED
    assert result.terminal is False


@pytest.mark.asyncio
async def test_a_key_that_never_named_a_position_is_not_closure() -> None:
    """The blocker the #3533 panel found: absence of a KEY is not absence of a POSITION.

    ``current_sizes.get(target, 0) == 0`` cannot distinguish "the target existed and
    went to zero" from "the target never existed — we read the wrong key". The
    baseline path had that evidence (``before.get(target, 0) > 0``) and deleting the
    baseline deleted the only existence check.

    The two keys come from different sources and can legitimately disagree: the
    requested key carries the order's ``initial_collateral_token`` (whatever the close
    intent resolved — ``full_close.py`` falls back to ``details["asset"]``), while the
    measured key carries the position's on-chain ``collateralToken``. USDC vs USDC.e on
    Arbitrum is the obvious trigger.

    Here the wallet holds a fully live ETH/USD long collateralised in token A, and the
    close order was built against token B. Under the keyed predicate alone this reads
    flat and settles — a silent strand over a live position. On `main` the same mistake
    fails loud.
    """
    other_collateral = "0x" + "44" * 20
    live_under_other_collateral = SimpleNamespace(
        ok=True,
        truncated=False,
        positions=(
            SimpleNamespace(
                is_active=True,
                market=_MARKET,
                collateral_token=other_collateral,  # NOT the order's collateral
                is_long=True,
                size_in_usd=100 * _RAW_USD,
            ),
        ),
    )

    async def _sleep(_seconds: float) -> None:
        return None

    with (
        patch("almanak.connectors.gmx_v2.runner_hooks.read_pending_orders", return_value=_pending()),
        patch(
            "almanak.connectors.gmx_v2.runner_hooks.read_open_positions",
            return_value=live_under_other_collateral,
        ),
        patch("almanak.framework.runner.async_settlement.asyncio.sleep", _sleep),
    ):
        result = await await_async_settlement(
            gateway_client=object(),
            chain="arbitrum",
            wallet_address="0xabc",
            network="mainnet",
            orders=(_order(),),  # collateral = _COLLATERAL, a different token
            intent=_intent("PERP_CLOSE"),
            timeout_seconds=2,
            poll_interval_seconds=1,
        )

    assert result.status is not AsyncSettlementStatus.SETTLED, (
        "an active position remains at the requested market/side under a different "
        "collateral token — key absence was treated as closure"
    )
    assert result.terminal is False


@pytest.mark.asyncio
async def test_a_position_in_another_market_does_not_block_settlement() -> None:
    """Non-vacuity control for the guard above.

    The guard must key on the requested (market, is_long), NOT on the wallet being
    empty. Demanding a wallet-wide zero would break every multi-market strategy: a
    live BTC position would prevent an ETH close from ever settling, re-creating the
    360s stall this ticket exists to remove.
    """
    unrelated_market = "0x" + "99" * 20
    other_market_live = SimpleNamespace(
        ok=True,
        truncated=False,
        positions=(
            SimpleNamespace(
                is_active=True,
                market=unrelated_market,
                collateral_token=_COLLATERAL,
                is_long=True,
                size_in_usd=100 * _RAW_USD,
            ),
        ),
    )

    result = await _run(
        pending=_pending(),
        positions=other_market_live,
        orders=(_order(),),
        intent_type="PERP_CLOSE",
    )

    assert result.status == AsyncSettlementStatus.SETTLED
    assert result.terminal is True
    assert result.attempts == 1


@pytest.mark.asyncio
async def test_a_second_position_under_different_collateral_blocks_settlement_deliberately() -> None:
    """Pin the KNOWN over-refusal so it is a decision, not an accident.

    GMX's position key is ``keccak(account, market, collateralToken, isLong)``, so
    collateral is part of the identity and a wallet can legitimately hold two
    distinct positions at the same ``(market, isLong)`` under different collateral.
    Closing one leaves the other live at that market/side, and the existence guard
    then refuses to settle a close that actually succeeded.

    That is the intended trade. The ambiguity is unresolvable in this branch — the
    other position is EITHER ours seen through a mis-resolved collateral (settling
    would strand it) OR genuinely separate (refusing costs a wait) — and the two
    errors are not symmetric.

    The cost is bounded to "no improvement", never "regression": this returns the
    same non-terminal verdict `main` returns for this entire branch today. If this
    test ever starts asserting SETTLED, someone has traded a silent strand for a
    speed-up.
    """
    other_collateral = "0x" + "77" * 20
    sibling_position_live = SimpleNamespace(
        ok=True,
        truncated=False,
        positions=(
            SimpleNamespace(
                is_active=True,
                market=_MARKET,  # same market
                collateral_token=other_collateral,  # different collateral => different GMX key
                is_long=True,  # same side
                size_in_usd=250 * _RAW_USD,
            ),
        ),
    )

    async def _sleep(_seconds: float) -> None:
        return None

    with (
        patch("almanak.connectors.gmx_v2.runner_hooks.read_pending_orders", return_value=_pending()),
        patch(
            "almanak.connectors.gmx_v2.runner_hooks.read_open_positions",
            return_value=sibling_position_live,
        ),
        patch("almanak.framework.runner.async_settlement.asyncio.sleep", _sleep),
    ):
        result = await await_async_settlement(
            gateway_client=object(),
            chain="arbitrum",
            wallet_address="0xabc",
            network="mainnet",
            orders=(_order(),),
            intent=_intent("PERP_CLOSE"),
            timeout_seconds=2,
            poll_interval_seconds=1,
        )

    assert result.status is not AsyncSettlementStatus.SETTLED
    assert result.terminal is False


@pytest.mark.asyncio
async def test_baseline_free_settled_carries_no_receipts() -> None:
    """The empty ``receipts`` tuple is what makes the attribution gap harmless (VIB-6334).

    This verdict proves a GOAL STATE — every requested order gone, every requested target
    measured flat — not an ATTRIBUTION. Without a baseline there is no before-image, so a
    target flattened by liquidation or by an operator's manual close is indistinguishable
    from one our order filled.

    That is safe for teardown, whose goal is the absence of exposure regardless of author.
    It would NOT be safe for accounting: booking a fill from it would attribute a trade
    that never executed.

    Nothing books from it today, and the reason is structural rather than a check —
    ``strategy_runner`` and ``teardown/runner_helpers`` both gate enrichment on
    ``if barrier.receipts:``, and this path supplies none. So the guard that has to hold
    is exactly this: **a baseline-free SETTLED must never carry receipts.** Attaching one
    here would silently arm both enrichment call sites with a fill nobody measured.

    Asserted on the barrier's own result rather than on the connector verdict, because it
    is the barrier's ``receipts`` that the two consumers read.
    """

    async def _sleep(_seconds: float) -> None:
        raise AssertionError("a decidable baseline-free verdict must not sleep")

    with (
        patch("almanak.connectors.gmx_v2.runner_hooks.read_pending_orders", return_value=_pending()),
        patch(
            "almanak.connectors.gmx_v2.runner_hooks.read_open_positions",
            return_value=_positions(size_usd=0),  # target measured FLAT
        ),
        patch("almanak.framework.runner.async_settlement.asyncio.sleep", _sleep),
    ):
        result = await await_async_settlement(
            gateway_client=object(),
            chain="arbitrum",
            wallet_address="0xabc",
            network="mainnet",
            orders=(_order(),),
            intent=_intent("PERP_CLOSE"),
            timeout_seconds=2,
            poll_interval_seconds=1,
        )

    assert result.status is AsyncSettlementStatus.SETTLED
    assert result.terminal is True
    assert not result.receipts, (
        "a baseline-free SETTLED proves flatness, not authorship — carrying receipts here "
        "would let both enrichment call sites book a fill that was never measured"
    )
