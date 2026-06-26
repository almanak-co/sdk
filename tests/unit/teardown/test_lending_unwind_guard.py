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
from almanak.framework.teardown.models import TeardownMode

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


def test_p0_multi_round_no_interleave_is_not_order_locked():
    """Two REPAYs then two WITHDRAWs with NO interleaved swap is NOT a genuine
    staircase (only an interleaved collateral→debt SWAP makes one). It is NOT
    order-locked — it routes through the normal decide→reorder path so the strand
    safety check applies. On a price-less market no synthesis fires and the
    repay-first reorder is a no-op, so the plan is passed through unchanged."""
    market = _FakeMarket(collateral_usd=Decimal("2000"), debt_usd=Decimal("900"))
    plan = [_repay_partial("400"), _repay_partial("500"), _withdraw_slice("0.3"), _withdraw_all()]
    result = sanitize_lending_teardown_intents(plan, market)
    assert result.intents == plan  # reorder is a no-op (repays already precede withdraws)
    assert not result.synthesized_positions  # price-less fake market -> no strand proof


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


# ---------------------------------------------------------------------------
# VIB-4466 / VIB-589: synthesise the HF-safe staircase when the naive plan would
# STRAND (wallet cannot fully repay live debt -> dust debt -> withdraw-all reverts)
# ---------------------------------------------------------------------------


class _RichHealth:
    def __init__(self, collateral_usd: Decimal, debt_usd: Decimal, lltv: Decimal) -> None:
        self.collateral_value_usd = collateral_usd
        self.debt_value_usd = debt_usd
        self.lltv = lltv


class _Bal:
    def __init__(self, amount: Decimal) -> None:
        self.balance = amount


class _RichMarket:
    """Market exposing position_health (with lltv), price, and wallet balance —
    enough for the guard to PROVE a strand and for the staircase planner to size.

    Models a plain borrow: collateral_token (WETH) supplied, borrow_token (USDC)
    drawn. ``wallet_usdc`` is the live wallet balance of the debt token.
    """

    def __init__(
        self,
        *,
        collateral_usd: Decimal,
        debt_usd: Decimal,
        lltv: Decimal,
        wallet_usdc: Decimal,
        weth_price: Decimal = Decimal("1000"),
        usdc_price: Decimal = Decimal("1"),
        chain: str = _CHAIN,
    ) -> None:
        self._health = _RichHealth(collateral_usd, debt_usd, lltv)
        self._prices = {"WETH": weth_price, "USDC": usdc_price}
        self._wallet = {"USDC": wallet_usdc, "WETH": Decimal("0")}
        self.chain = chain
        self.reads = 0

    def position_health(self, protocol: str, market_id: str, **kwargs: Any) -> _RichHealth:
        self.reads += 1
        return self._health

    def price(self, token: str) -> Decimal:
        return self._prices.get(token, Decimal("0"))

    def balance(self, token: str) -> _Bal:
        return _Bal(self._wallet.get(token, Decimal("0")))


def _plain_borrow_plan() -> list[Any]:
    """The hand-rolled naive plain-borrow teardown: REPAY(all) -> WITHDRAW(all)."""
    return [
        Intent.repay(protocol=_PROTOCOL, token="USDC", repay_full=True, chain=_CHAIN),
        Intent.withdraw(protocol=_PROTOCOL, token="WETH", amount=Decimal("0"), withdraw_all=True, chain=_CHAIN),
    ]


def test_vib4466_wallet_cannot_cover_debt_synthesizes_staircase():
    """Wallet holds less debt token than the live debt (the bug) -> the naive
    REPAY->WITHDRAW(all) is replaced with the HF-safe unwind staircase, which
    sources the interest shortfall from collateral before the final withdraw-all."""
    # collateral 2.3 WETH = $2300; debt $1150; wallet 1140 USDC (< $1150 debt).
    market = _RichMarket(
        collateral_usd=Decimal("2300"), debt_usd=Decimal("1150"), lltv=Decimal("0.8"), wallet_usdc=Decimal("1140")
    )
    result = sanitize_lending_teardown_intents(_plain_borrow_plan(), market)

    # Synthesised: NOT the naive 2-intent plan — a real staircase ran.
    assert result.synthesized_positions, "expected the position to be synthesised"
    assert not result.degraded
    types = _types(result.intents)
    # withdraw->swap->repay rounds then a final withdraw-all (more than 2 intents).
    assert len(result.intents) > 2
    assert "SWAP" in types  # collateral->debt swap to clear the shortfall
    # The final withdraw is the full withdraw-all (debt is truly zero by then).
    withdraws = [i for i in result.intents if i.intent_type.value == "WITHDRAW"]
    assert getattr(withdraws[-1], "withdraw_all", False) is True


def test_vib4466_consolidate_to_collateral_not_debt_token():
    """Regression on the design correction: the synthesised residual sweep must
    swap the over-funded BORROW token back to COLLATERAL (consolidate_to=
    collateral), NOT swap the whole recovered collateral into the debt token
    (which would force a gratuitous collateral->debt->target round-trip)."""
    market = _RichMarket(
        collateral_usd=Decimal("2300"), debt_usd=Decimal("1150"), lltv=Decimal("0.8"), wallet_usdc=Decimal("1140")
    )
    result = sanitize_lending_teardown_intents(_plain_borrow_plan(), market)
    swaps = [i for i in result.intents if i.intent_type.value == "SWAP"]
    # The final (residual sweep) swap must end in the collateral token (WETH),
    # i.e. sweep stray borrow token back to collateral — never the reverse.
    last_swap = swaps[-1]
    assert last_swap.to_token == "WETH"
    assert last_swap.from_token == "USDC"


def test_vib4466_wallet_covers_debt_keeps_naive_plan():
    """When the wallet CAN fully repay the live debt, the naive plan works (Aave
    caps the repay at the debt, clearing it, so withdraw-all is safe) — the guard
    must NOT synthesise an unnecessary staircase."""
    # wallet 2000 USDC > $1150 debt -> covers it.
    market = _RichMarket(
        collateral_usd=Decimal("2300"), debt_usd=Decimal("1150"), lltv=Decimal("0.8"), wallet_usdc=Decimal("2000")
    )
    result = sanitize_lending_teardown_intents(_plain_borrow_plan(), market)
    assert not result.synthesized_positions
    assert _types(result.intents) == ["REPAY", "WITHDRAW"]


def test_vib4466_planner_failure_degrades_to_repay_only_no_unsafe_withdraw():
    """When the strand is proven but the staircase planner cannot size a safe
    unwind (here: lltv unreadable -> ValueError), the guard degrades to a
    risk-reducing partial REPAY and NEVER emits the reverting withdraw_all."""
    # lltv=0 makes generate_leverage_loop_teardown raise ValueError (cannot size).
    market = _RichMarket(
        collateral_usd=Decimal("2300"), debt_usd=Decimal("1150"), lltv=Decimal("0"), wallet_usdc=Decimal("1140")
    )
    result = sanitize_lending_teardown_intents(_plain_borrow_plan(), market)

    assert result.degraded
    assert not result.synthesized_positions
    types = _types(result.intents)
    assert "WITHDRAW" not in types  # the unsafe withdraw_all is withheld
    assert types == ["REPAY"]  # only the risk-reducing partial repay survives
    # The kept repay is an explicit partial (never repay_full on degrade).
    assert result.intents[0].repay_full is False
    assert any("staircase unavailable" in d for d in result.dropped)


def test_vib4466_dust_debt_below_floor_does_not_synthesize():
    """A position whose live debt is below the dust floor is measured-zero debt
    (collateral-only) -> existing path keeps the withdraw_all, no synthesis."""
    market = _RichMarket(
        collateral_usd=Decimal("2300"), debt_usd=Decimal("0.005"), lltv=Decimal("0.8"), wallet_usdc=Decimal("0")
    )
    result = sanitize_lending_teardown_intents(_plain_borrow_plan(), market)
    assert not result.synthesized_positions
    assert _types(result.intents) == ["WITHDRAW"]  # zero-debt -> repay dropped, withdraw kept


def test_vib4466_order_locked_staircase_is_never_resynthesized():
    """An already-correct interleaved staircase (order-locked) must pass through
    untouched even on a rich market — never double-synthesised."""
    market = _RichMarket(
        collateral_usd=Decimal("2300"), debt_usd=Decimal("900"), lltv=Decimal("0.8"), wallet_usdc=Decimal("0")
    )
    staircase = _staircase()
    result = sanitize_lending_teardown_intents(staircase, market)
    assert result.intents == staircase
    assert not result.synthesized_positions


def test_vib4466_two_independent_positions_each_synthesized_not_order_locked():
    """Two INDEPENDENT single-round borrow positions in one simple teardown must
    NOT be mistaken for an order-locked staircase (round count is per-position).
    Each provable strand is synthesised. (CodeRabbit major: a global round-count
    would order-lock two independent positions and skip both.)"""
    market = _RichMarket(
        collateral_usd=Decimal("2300"), debt_usd=Decimal("1150"), lltv=Decimal("0.8"), wallet_usdc=Decimal("1140")
    )
    mid = "0x" + "cd" * 32
    plan = [
        Intent.repay(protocol="aave_v3", token="USDC", repay_full=True, chain=_CHAIN),
        Intent.withdraw(protocol="aave_v3", token="WETH", amount=Decimal("0"), withdraw_all=True, chain=_CHAIN),
        Intent.repay(protocol="morpho_blue", token="USDC", repay_full=True, market_id=mid, chain=_CHAIN),
        Intent.withdraw(
            protocol="morpho_blue", token="WETH", amount=Decimal("0"), withdraw_all=True, market_id=mid, chain=_CHAIN
        ),
    ]
    result = sanitize_lending_teardown_intents(plan, market)
    # Both independent positions are strand-proven and synthesised (not order-locked).
    assert len(result.synthesized_positions) == 2
    assert not result.degraded


def test_vib4466_partial_repay_not_covering_debt_synthesizes_even_when_wallet_covers():
    """An explicit PARTIAL repay that does not cover the debt, followed by
    WITHDRAW(all), strands even when the wallet could cover the debt — the repay
    only pays the partial amount, leaving residual debt. Must synthesise.
    (CodeRabbit major.)"""
    market = _RichMarket(
        collateral_usd=Decimal("2300"), debt_usd=Decimal("1150"), lltv=Decimal("0.8"), wallet_usdc=Decimal("2000")
    )
    plan = [
        Intent.repay(protocol=_PROTOCOL, token="USDC", amount=Decimal("100"), chain=_CHAIN),  # partial < debt
        Intent.withdraw(protocol=_PROTOCOL, token="WETH", amount=Decimal("0"), withdraw_all=True, chain=_CHAIN),
    ]
    result = sanitize_lending_teardown_intents(plan, market)
    assert result.synthesized_positions  # residual debt after the partial repay -> staircase


def test_vib4466_partial_repay_covering_debt_keeps_naive_when_wallet_covers():
    """A partial repay that DOES cover the measured debt (amount*price >= debt) with
    the wallet covering it is safe — no synthesis."""
    market = _RichMarket(
        collateral_usd=Decimal("2300"), debt_usd=Decimal("1150"), lltv=Decimal("0.8"), wallet_usdc=Decimal("2000")
    )
    plan = [
        Intent.repay(protocol=_PROTOCOL, token="USDC", amount=Decimal("1200"), chain=_CHAIN),  # covers 1150 debt
        Intent.withdraw(protocol=_PROTOCOL, token="WETH", amount=Decimal("0"), withdraw_all=True, chain=_CHAIN),
    ]
    result = sanitize_lending_teardown_intents(plan, market)
    assert not result.synthesized_positions


def test_vib4466_partial_withdraw_not_all_is_never_synthesized():
    """A plan that withdraws a SPECIFIC amount (not withdraw_all) keeps HF headroom
    and cannot hit the withdraw-all revert — never replaced with a full unwind,
    even under a provable wallet shortfall."""
    market = _RichMarket(
        collateral_usd=Decimal("2300"), debt_usd=Decimal("1150"), lltv=Decimal("0.8"), wallet_usdc=Decimal("1140")
    )
    plan = [
        Intent.repay(protocol=_PROTOCOL, token="USDC", repay_full=True, chain=_CHAIN),
        Intent.withdraw(protocol=_PROTOCOL, token="WETH", amount=Decimal("0.5"), chain=_CHAIN),  # partial, not all
    ]
    result = sanitize_lending_teardown_intents(plan, market)
    assert not result.synthesized_positions


def test_vib4466_multi_partial_repay_then_withdraw_all_does_not_bypass_strand_guard():
    """Fail-closed (CodeRabbit major): a non-interleaved
    ``partial REPAY → partial REPAY → WITHDRAW(all)`` for one position must NOT be
    treated as a known-safe staircase and bypass synthesis. With a provable strand
    it is replaced with the HF-safe staircase (not passed through with the unsafe
    withdraw-all). Two partials summing under the debt still leave residual debt."""
    market = _RichMarket(
        collateral_usd=Decimal("2300"), debt_usd=Decimal("1150"), lltv=Decimal("0.8"), wallet_usdc=Decimal("1140")
    )
    plan = [
        Intent.repay(protocol=_PROTOCOL, token="USDC", amount=Decimal("100"), chain=_CHAIN),
        Intent.repay(protocol=_PROTOCOL, token="USDC", amount=Decimal("100"), chain=_CHAIN),
        Intent.withdraw(protocol=_PROTOCOL, token="WETH", amount=Decimal("0"), withdraw_all=True, chain=_CHAIN),
    ]
    result = sanitize_lending_teardown_intents(plan, market)
    assert result.synthesized_positions  # strand-proven -> staircase, not a bare withdraw-all
    # The raw multi-partial plan is gone; the dispatched plan is the synthesized staircase.
    assert result.intents != plan


def test_vib4466_mode_soft_vs_hard_changes_synthesized_slippage():
    """The SOFT/HARD mode threaded into the guard must reach the synthesized
    staircase: HARD uses a wider per-swap slippage cap than SOFT (CodeRabbit:
    mode plumbing was previously untested)."""
    market = _RichMarket(
        collateral_usd=Decimal("2300"), debt_usd=Decimal("1150"), lltv=Decimal("0.8"), wallet_usdc=Decimal("1140")
    )

    def staircase_swap_slippages(mode):
        result = sanitize_lending_teardown_intents(_plain_borrow_plan(), market, mode=mode)
        assert result.synthesized_positions, f"expected synthesis for mode={mode}"
        return [i.max_slippage for i in result.intents if i.intent_type.value == "SWAP" and i.max_slippage is not None]

    soft = staircase_swap_slippages(TeardownMode.SOFT)
    hard = staircase_swap_slippages(TeardownMode.HARD)
    assert soft and hard, "both modes should emit collateral->debt swaps with a slippage cap"
    # HARD widens the slippage cap relative to SOFT (emergency unwind makes progress).
    assert max(hard) > max(soft)
