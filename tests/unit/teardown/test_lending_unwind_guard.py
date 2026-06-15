"""Unit tests for the VIB-5139 universal fresh-state lending unwind guard.

Acceptance cases (CLAUDE.md §Accounting — Empty ≠ Zero discipline):

1. No debt + no collateral (measured zero) -> no intents.
2. Debt + collateral -> repay before withdraw (repay-first ordering).
3. Stale cached nonzero but fresh measured zero -> no intents.
4. Fresh unknown/None -> degraded; no unsafe withdraw_all.
5. Zero REPAY never emitted (measured zero debt drops the repay).
6. Non-lending intents pass through untouched.

The guard reads fresh exposure via ``market.position_health`` (gateway-backed).
A failed read must yield ``None`` (unmeasured), NEVER ``Decimal("0")`` — and the
guard must never ACT on a ``None`` as if it were a measured zero.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import pytest

from almanak.framework.intents.vocabulary import Intent
from almanak.framework.teardown.lending_unwind_guard import (
    sanitize_lending_teardown_intents,
)

_PROTOCOL = "aave_v3"
_CHAIN = "arbitrum"


class _Health:
    def __init__(self, collateral_usd: Decimal | None, debt_usd: Decimal | None) -> None:
        self.collateral_value_usd = collateral_usd
        self.debt_value_usd = debt_usd


class _FakeMarket:
    """Market snapshot returning a fixed health (or raising) for position_health.

    ``raise_on_read`` simulates an unmeasured fresh read (the stale-state bug):
    the guard must treat the exposure as ``None``, NOT ``Decimal("0")``.
    """

    def __init__(
        self,
        collateral_usd: Decimal | None,
        debt_usd: Decimal | None,
        *,
        raise_on_read: bool = False,
        chain: str = _CHAIN,
    ) -> None:
        self._health = _Health(collateral_usd, debt_usd)
        self._raise = raise_on_read
        self.chain = chain  # snapshot's pinned (primary) chain (P1 chain-scoping)
        self.reads = 0

    def position_health(self, protocol: str, market_id: str, **kwargs: Any) -> _Health:
        self.reads += 1
        if self._raise:
            raise RuntimeError("position health unavailable (RPC failed)")
        return self._health


def _repay() -> Any:
    return Intent.repay(protocol=_PROTOCOL, token="USDC", repay_full=True, chain=_CHAIN)


def _withdraw_all() -> Any:
    return Intent.withdraw(protocol=_PROTOCOL, token="WETH", amount=Decimal("0"), withdraw_all=True, chain=_CHAIN)


def _swap_tail() -> Any:
    return Intent.swap(from_token="WETH", to_token="USDC", amount="all", chain=_CHAIN)


def _types(intents: list[Any]) -> list[str]:
    return [i.intent_type.value for i in intents]


# ---------------------------------------------------------------------------
# Case 1 + 3 + 5: measured zero debt AND zero collateral -> no intents
# ---------------------------------------------------------------------------


def test_case1_no_debt_no_collateral_emits_no_intents():
    """Fresh measured zero on both legs -> drop everything, mark no-op."""
    market = _FakeMarket(collateral_usd=Decimal("0"), debt_usd=Decimal("0"))
    result = sanitize_lending_teardown_intents([_repay(), _withdraw_all()], market)

    assert result.intents == []
    assert not result.degraded
    assert result.no_op_positions  # the position was flagged fully flat
    assert len(result.dropped) == 2


def test_case3_stale_cached_nonzero_but_fresh_zero_emits_no_intents():
    """The strategy emitted REPAY+WITHDRAW from STALE nonzero cache, but the
    FRESH read says both legs are flat -> guard drops both (the stale-state bug
    this ticket fixes)."""
    market = _FakeMarket(collateral_usd=Decimal("0.0"), debt_usd=Decimal("0.0"))
    # The intents themselves look like a real unwind (repay_full + withdraw_all);
    # only the fresh read reveals they are stale.
    result = sanitize_lending_teardown_intents([_repay(), _withdraw_all()], market)
    assert result.intents == []


def test_case5_zero_debt_drops_repay_but_keeps_withdraw():
    """Measured zero debt but live collateral -> REPAY 0 dropped, WITHDRAW kept."""
    market = _FakeMarket(collateral_usd=Decimal("1000"), debt_usd=Decimal("0"))
    result = sanitize_lending_teardown_intents([_repay(), _withdraw_all()], market)

    assert _types(result.intents) == ["WITHDRAW"]
    assert any("zero debt" in d for d in result.dropped)
    assert not result.degraded


def test_case5_dust_debt_treated_as_zero():
    """Debt below the dust floor is a measured zero -> no REPAY emitted."""
    market = _FakeMarket(collateral_usd=Decimal("1000"), debt_usd=Decimal("0.001"))
    result = sanitize_lending_teardown_intents([_repay(), _withdraw_all()], market)
    assert "REPAY" not in _types(result.intents)


# ---------------------------------------------------------------------------
# Case 2: debt + collateral -> repay before withdraw
# ---------------------------------------------------------------------------


def test_case2_debt_and_collateral_repays_before_withdraw():
    market = _FakeMarket(collateral_usd=Decimal("1000"), debt_usd=Decimal("500"))
    result = sanitize_lending_teardown_intents([_repay(), _withdraw_all()], market)

    assert _types(result.intents) == ["REPAY", "WITHDRAW"]
    assert not result.degraded
    assert not result.dropped


def test_case2_reorders_withdraw_after_repay_when_strategy_emits_wrong_order():
    """Strategy emitted WITHDRAW before REPAY (the unsafe ordering). The guard
    must reorder to repay-first."""
    market = _FakeMarket(collateral_usd=Decimal("1000"), debt_usd=Decimal("500"))
    result = sanitize_lending_teardown_intents([_withdraw_all(), _repay()], market)
    assert _types(result.intents) == ["REPAY", "WITHDRAW"]


def test_case2_reads_position_once_per_distinct_position():
    """Two intents on the same position trigger exactly one fresh read."""
    market = _FakeMarket(collateral_usd=Decimal("1000"), debt_usd=Decimal("500"))
    sanitize_lending_teardown_intents([_repay(), _withdraw_all()], market)
    assert market.reads == 1


# ---------------------------------------------------------------------------
# Case 4: fresh unknown/None -> degraded; no unsafe withdraw_all
# ---------------------------------------------------------------------------


def test_case4_unmeasured_read_keeps_repay_and_paired_withdraw_degraded():
    """A failed read is unmeasured (None), NOT zero. Keep the risk-reducing
    REPAY and allow the WITHDRAW because the repay-first clears debt on-chain
    before it runs. Mark degraded."""
    market = _FakeMarket(None, None, raise_on_read=True)
    result = sanitize_lending_teardown_intents([_repay(), _withdraw_all()], market)

    assert _types(result.intents) == ["REPAY", "WITHDRAW"]
    assert result.degraded
    assert not result.no_op_positions  # never flagged flat on an unmeasured read


def test_case4_unmeasured_read_drops_standalone_withdraw_all():
    """A withdraw_all with NO repay-first under an unmeasured read is the
    dangerous stale case — the guard refuses it (no wallet-wide action from
    stale assumptions)."""
    market = _FakeMarket(None, None, raise_on_read=True)
    result = sanitize_lending_teardown_intents([_withdraw_all()], market)

    assert result.intents == []
    assert result.degraded
    assert any("unmeasured" in d for d in result.dropped)


def test_case4_none_market_is_unmeasured_not_zero():
    """No market at all -> unmeasured. A standalone withdraw_all is refused;
    the read must never be coerced to a measured zero."""
    result = sanitize_lending_teardown_intents([_withdraw_all()], market=None)
    assert result.intents == []
    assert result.degraded


def test_case4_partial_none_collateral_is_unmeasured():
    """collateral None + debt measured nonzero -> exposure is unmeasured
    (Empty ≠ Zero): keep repay, keep paired withdraw, degraded."""
    market = _FakeMarket(collateral_usd=None, debt_usd=Decimal("500"))
    result = sanitize_lending_teardown_intents([_repay(), _withdraw_all()], market)
    assert _types(result.intents) == ["REPAY", "WITHDRAW"]
    assert result.degraded


# ---------------------------------------------------------------------------
# Case 6: non-lending intents pass through untouched
# ---------------------------------------------------------------------------


def test_case6_non_lending_intents_pass_through():
    """A swap-only teardown has no lending intents -> no reads, untouched."""
    market = _FakeMarket(collateral_usd=Decimal("0"), debt_usd=Decimal("0"))
    swap = _swap_tail()
    result = sanitize_lending_teardown_intents([swap], market)

    assert result.intents == [swap]
    assert market.reads == 0
    assert not result.degraded
    assert not result.dropped


def test_case6_swap_tail_preserved_after_lending_block():
    """A consolidation/close swap that trails the lending unwind keeps its
    position after the repay+withdraw block."""
    market = _FakeMarket(collateral_usd=Decimal("1000"), debt_usd=Decimal("500"))
    swap = _swap_tail()
    result = sanitize_lending_teardown_intents([_repay(), _withdraw_all(), swap], market)
    assert _types(result.intents) == ["REPAY", "WITHDRAW", "SWAP"]
    assert result.intents[-1] is swap


def test_empty_intent_list_is_noop():
    market = _FakeMarket(collateral_usd=Decimal("0"), debt_usd=Decimal("0"))
    result = sanitize_lending_teardown_intents([], market)
    assert result.intents == []
    assert market.reads == 0


def test_unknown_protocol_is_left_untouched():
    """A REPAY/WITHDRAW on a protocol the health reader does not cover is not a
    lending position the guard can validate -> pass through, no read attempt
    coerced into a drop."""
    repay = Intent.repay(protocol="not_a_real_lending_protocol", token="USDC", repay_full=True, chain=_CHAIN)
    market = _FakeMarket(collateral_usd=Decimal("0"), debt_usd=Decimal("0"))
    result = sanitize_lending_teardown_intents([repay], market)
    # Unknown protocol -> not treated as a lending unwind, so no read, untouched.
    assert result.intents == [repay]
    assert market.reads == 0


def test_deleverage_intent_treated_as_repay():
    """A DELEVERAGE (structurally a repay) on measured-zero debt is dropped like
    a REPAY 0."""
    delev = Intent.deleverage(protocol=_PROTOCOL, token="USDC", repay_full=True, chain=_CHAIN)
    market = _FakeMarket(collateral_usd=Decimal("0"), debt_usd=Decimal("0"))
    result = sanitize_lending_teardown_intents([delev], market)
    assert result.intents == []
    assert any("zero debt" in d for d in result.dropped)


def test_original_list_not_mutated():
    """The guard returns a new list and never mutates the caller's input."""
    market = _FakeMarket(collateral_usd=Decimal("0"), debt_usd=Decimal("0"))
    original = [_repay(), _withdraw_all()]
    snapshot = list(original)
    sanitize_lending_teardown_intents(original, market)
    assert original == snapshot


# ---------------------------------------------------------------------------
# P0: interleaved leveraged-loop staircase ORDER must be preserved
# ---------------------------------------------------------------------------


def _withdraw_slice(amount: str) -> Any:
    return Intent.withdraw(protocol=_PROTOCOL, token="WETH", amount=Decimal(amount), chain=_CHAIN)


def _swap_collat_to_borrow(amount: str) -> Any:
    return Intent.swap(from_token="WETH", to_token="USDC", amount=Decimal(amount), chain=_CHAIN)


def _repay_partial(amount: str) -> Any:
    return Intent.repay(protocol=_PROTOCOL, token="USDC", amount=Decimal(amount), chain=_CHAIN)


def _staircase() -> list[Any]:
    """Mirror generate_leverage_loop_teardown's interleaved output shape:
    WITHDRAW(slice) -> SWAP -> REPAY -> WITHDRAW(slice) -> SWAP -> REPAY -> WITHDRAW(all) -> SWAP."""
    return [
        _withdraw_slice("0.3"),
        _swap_collat_to_borrow("0.3"),
        _repay_partial("400"),
        _withdraw_slice("0.4"),
        _swap_collat_to_borrow("0.4"),
        _repay_partial("500"),
        _withdraw_all(),
        _swap_tail(),
    ]


def test_p0_interleaved_staircase_order_is_preserved():
    """A live leveraged-loop staircase (debt + collateral measured nonzero) must
    pass through with its EXACT order intact — the first intent stays a WITHDRAW,
    never a front-loaded REPAY (which would revert: no borrow token in wallet yet)."""
    market = _FakeMarket(collateral_usd=Decimal("2000"), debt_usd=Decimal("900"))
    staircase = _staircase()
    result = sanitize_lending_teardown_intents(staircase, market)

    # Order preserved exactly, nothing dropped (everything live).
    assert result.intents == staircase
    assert _types(result.intents)[0] == "WITHDRAW"
    assert _types(result.intents) == ["WITHDRAW", "SWAP", "REPAY", "WITHDRAW", "SWAP", "REPAY", "WITHDRAW", "SWAP"]
    assert not result.dropped


def test_p0_staircase_drops_genuinely_zero_leg_in_place():
    """In an order-locked staircase, a measured-zero leg is dropped IN PLACE
    (order of the survivors preserved), NOT globally reordered.

    Here the position reads measured-zero DEBT but live collateral: the partial
    REPAYs (zero debt) drop, the WITHDRAWs/SWAPs keep their relative order, and
    the first surviving intent is still the WITHDRAW(slice), not a REPAY."""
    market = _FakeMarket(collateral_usd=Decimal("2000"), debt_usd=Decimal("0"))
    staircase = _staircase()
    result = sanitize_lending_teardown_intents(staircase, market)

    # The two partial REPAYs are dropped (measured zero debt); everything else
    # keeps its original relative order.
    assert _types(result.intents) == ["WITHDRAW", "SWAP", "WITHDRAW", "SWAP", "WITHDRAW", "SWAP"]
    assert _types(result.intents)[0] == "WITHDRAW"
    assert all("zero debt" in d for d in result.dropped)
    assert len(result.dropped) == 2


def test_p0_multi_round_no_interleave_is_still_order_locked():
    """Two REPAYs back-to-back then two WITHDRAWs (multi-round, no passthrough
    between) is still order-locked — more than one round means we must not assume
    a simple single-round reorder is safe."""
    market = _FakeMarket(collateral_usd=Decimal("2000"), debt_usd=Decimal("900"))
    plan = [_repay_partial("400"), _repay_partial("500"), _withdraw_slice("0.3"), _withdraw_all()]
    result = sanitize_lending_teardown_intents(plan, market)
    assert result.intents == plan  # preserved in place


def test_p0_simple_single_round_still_reorders():
    """The simple non-interleaved single-round case (the hand-rolled strategies
    this ticket targets) is NOT order-locked and still gets the repay-first
    reorder."""
    market = _FakeMarket(collateral_usd=Decimal("1000"), debt_usd=Decimal("500"))
    # Strategy emitted WITHDRAW before REPAY (wrong order), single round, no
    # interleaving -> guard reorders to repay-first.
    result = sanitize_lending_teardown_intents([_withdraw_all(), _repay()], market)
    assert _types(result.intents) == ["REPAY", "WITHDRAW"]


# ---------------------------------------------------------------------------
# P1: multi-chain wrong-chain exposure read must NOT drop a live intent
# ---------------------------------------------------------------------------


def test_p1_intent_on_other_chain_not_dropped_by_primary_chain_read():
    """The snapshot is pinned to chain[0] (arbitrum) and reads measured-zero
    there. A lending intent on chain[1] (optimism) must NOT be dropped on the
    strength of the unrelated chain[0] read — it degrades to unmeasured instead."""
    # Snapshot pinned to arbitrum, reading a flat position there.
    market = _FakeMarket(collateral_usd=Decimal("0"), debt_usd=Decimal("0"), chain="arbitrum")
    # The intents target optimism (a different chain).
    repay_op = Intent.repay(protocol=_PROTOCOL, token="USDC", repay_full=True, chain="optimism")
    withdraw_op = Intent.withdraw(
        protocol=_PROTOCOL, token="WETH", amount=Decimal("0"), withdraw_all=True, chain="optimism"
    )
    result = sanitize_lending_teardown_intents([repay_op, withdraw_op], market)

    # The chain[0] measured-zero must NOT drop the chain[1] intents.
    assert _types(result.intents) == ["REPAY", "WITHDRAW"]
    assert result.degraded  # forced unmeasured (wrong chain)
    assert not result.no_op_positions
    # The read happens, but its result is discarded (wrong chain) — at minimum
    # the live intents survive.
    assert repay_op in result.intents and withdraw_op in result.intents


def test_p1_intent_on_matching_chain_is_trusted():
    """When the intent's chain matches the snapshot's pinned chain, the read IS
    trusted (measured-zero drops apply)."""
    market = _FakeMarket(collateral_usd=Decimal("0"), debt_usd=Decimal("0"), chain="arbitrum")
    repay = Intent.repay(protocol=_PROTOCOL, token="USDC", repay_full=True, chain="arbitrum")
    result = sanitize_lending_teardown_intents([repay], market)
    assert result.intents == []  # measured zero debt -> dropped
    assert not result.degraded


def test_p1_snapshot_without_chain_attr_forces_unmeasured():
    """A market that cannot report its chain must not be trusted for drops."""

    class _NoChainMarket:
        def position_health(self, protocol: str, market_id: str, **kwargs: Any) -> _Health:
            return _Health(Decimal("0"), Decimal("0"))

    repay = _repay()
    result = sanitize_lending_teardown_intents([repay], _NoChainMarket())
    assert result.intents == [repay]  # not dropped — read not trusted
    assert result.degraded


# ---------------------------------------------------------------------------
# HIGH #3: refuse WITHDRAW on MEASURED active debt with no repay-first
# ---------------------------------------------------------------------------


def test_high3_measured_active_debt_standalone_withdraw_is_dropped():
    """A WITHDRAW with MEASURED active debt and NO repay-first reverts on-chain
    (Aave HealthFactorLowerThanLiquidationThreshold — the ALM-2811 failure).
    The guard must refuse it."""
    market = _FakeMarket(collateral_usd=Decimal("1000"), debt_usd=Decimal("500"))
    result = sanitize_lending_teardown_intents([_withdraw_all()], market)

    assert result.intents == []
    assert any("active debt and no repay-first" in d for d in result.dropped)
    assert not result.degraded  # measured read, not unmeasured


def test_high3_measured_active_debt_withdraw_kept_when_repay_present():
    """The SAME measured-active-debt position is fine to withdraw when a
    repay-first exists for it — has_repay is True, branch not taken."""
    market = _FakeMarket(collateral_usd=Decimal("1000"), debt_usd=Decimal("500"))
    result = sanitize_lending_teardown_intents([_repay(), _withdraw_all()], market)
    assert _types(result.intents) == ["REPAY", "WITHDRAW"]
    assert not result.dropped


def test_high3_staircase_withdraws_not_dropped_by_active_debt_branch():
    """Regression: the interleaved leveraged-loop staircase HAS repays for the
    position (has_repay True over the whole plan), so the new active-debt branch
    must NOT drop its withdraws — and the order is preserved end-to-end. This is
    the exact case the active-debt guard must not break."""
    market = _FakeMarket(collateral_usd=Decimal("2000"), debt_usd=Decimal("900"))
    staircase = _staircase()
    result = sanitize_lending_teardown_intents(staircase, market)

    # Order preserved, NOTHING dropped — the first WITHDRAW(slice) survives even
    # though it precedes the repays in execution order (has_repay is per-position
    # over the whole plan).
    assert result.intents == staircase
    assert _types(result.intents)[0] == "WITHDRAW"
    assert not result.dropped


# ---------------------------------------------------------------------------
# HIGH #1: protocol-name normalization for grouping + reads
# ---------------------------------------------------------------------------


def test_high1_protocol_aliases_group_as_one_position():
    """``Aave_V3`` and ``aave_v3`` must normalise to the SAME position so they
    share ONE fresh read and are treated as one position."""
    market = _FakeMarket(collateral_usd=Decimal("1000"), debt_usd=Decimal("500"))
    repay = Intent.repay(protocol="Aave_V3", token="USDC", repay_full=True, chain=_CHAIN)
    withdraw = Intent.withdraw(
        protocol="aave_v3", token="WETH", amount=Decimal("0"), withdraw_all=True, chain=_CHAIN
    )
    result = sanitize_lending_teardown_intents([repay, withdraw], market)

    # One distinct position → exactly one fresh read; repay-first kept together.
    assert market.reads == 1
    assert _types(result.intents) == ["REPAY", "WITHDRAW"]


def test_high1_morpho_alias_normalizes_and_groups():
    """``morpho`` (alias) and ``morpho_blue`` group as one position. Morpho
    carries a required market_id (validated at intent construction), so the key
    stays distinct per market but the protocol leg folds to one canonical key."""
    market = _FakeMarket(collateral_usd=Decimal("1000"), debt_usd=Decimal("500"))
    mid = "0x" + "ab" * 32
    repay = Intent.repay(protocol="morpho", token="USDC", repay_full=True, market_id=mid, chain=_CHAIN)
    withdraw = Intent.withdraw(
        protocol="morpho_blue", token="WETH", amount=Decimal("0"), withdraw_all=True, market_id=mid, chain=_CHAIN
    )
    result = sanitize_lending_teardown_intents([repay, withdraw], market)
    assert market.reads == 1  # one canonical position
    assert _types(result.intents) == ["REPAY", "WITHDRAW"]
