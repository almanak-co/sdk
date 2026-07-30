"""A deferred-refresh refusal must never escalate the teardown slippage ladder.

VIB-6228. The execution change that made `refresh_deferred_bundle` fail closed
created a cross-lane defect that green CI could not see, because the two lanes
classify failures through completely different code:

* the **iteration lane** reaches `inner_runner._is_retryable`, a substring
  blocklist defaulting to retryable — the refusal messages were written for it;
* the **teardown lane** never reaches that function. `teardown_manager` compiles
  and calls `orchestrator.execute` directly, then classifies with
  `classify_teardown_failure`.

Measured before the fix: **7 of 8** refusal messages landed on
`UNKNOWN -> ESCALATE` at the classifier's step 7. Escalating walks the slippage
ladder (`EscalatingSlippageManager.DEFAULT_LEVELS`) to level 3, which is
`auto_approve=False` — so an unreachable LiFi/Enso endpoint asked an operator to
approve **5% slippage**, and where no approval callback is wired the manager
returns `paused_awaiting_approval`, which `teardown_manager` turns into an early
return that **abandons every remaining risk-reducing intent**. That inverts
AGENTS.md §Teardown's first rule.

Reachable, not hypothetical: `enso` and `lifi` are both in
`teardown/consolidation.py:_GENERAL_PURPOSE_SWAP_ROUTERS`, and
`demo_catalog/almanak_rsi` emits exactly one teardown intent — an Enso swap — so
a refused refresh there reduces zero on-chain risk.

Found by the `pr-auditor` on PR #3503 and verified independently before fixing.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from almanak.framework.execution.interfaces import DeferredRefreshError
from almanak.framework.teardown.error_taxonomy import (
    Disposition,
    RevertClass,
    classify_teardown_failure,
)
from almanak.framework.teardown.slippage_manager import EscalatingSlippageManager

# Every reason `refresh_deferred_bundle` can refuse with, paired with the
# `recoverable` flag it actually passes. Kept as (reason, recoverable) rather than
# pre-built strings so the tag encoding is exercised, not bypassed.
_TRANSIENT_REASONS = [
    "the fresh route request failed: Read timed out",
    "the fresh route request failed: 502 Server Error: Bad Gateway",
    "the fresh route request failed: HTTPSConnectionPool(host='api.enso.finance', port=443)",
    "the refresh provider returned no transaction data, so the stale route data cannot be replaced",
    "the refreshed route is missing required transaction field(s) ['data'], so it cannot replace the stale calldata",
    "the refreshed route's calldata is not a 0x-prefixed hex payload (None), so it cannot be signed",
]
_PERMANENT_REASONS = [
    "the bundle carries no route_params, so no fresh route can be requested",
    "no deferred-refresh provider is registered for this protocol, so its stale route data cannot be replaced",
    "the bundle declares a deferred swap but carries no transaction with a '_deferred' tx_type to replace",
    "the bundle carries 2 transactions with a '_deferred' tx_type, but a single refresh response can only make one",
    "the refreshed route named 12345 as its approval spender, which is not a 20-byte address",
    "the bundle's approval calldata is not a well-formed approve(address,uint256) payload (12 chars, expected 138)",
]


class TestNoRefusalEverEscalates:
    """The invariant, stated once: no refusal reaches the slippage ladder."""

    @pytest.mark.parametrize("reason", _TRANSIENT_REASONS)
    def test_transient_refusal_retries_at_the_same_level(self, reason):
        exc = DeferredRefreshError(reason, protocol="enso")
        revert_class, disposition = classify_teardown_failure(str(exc))

        assert revert_class == RevertClass.ROUTE_REFRESH_REFUSED
        assert disposition == Disposition.RETRY_SAME_LEVEL, (
            "a transient route-API failure must retry at the SAME slippage level — "
            "bumping slippage cannot make an unreachable endpoint resolve"
        )

    @pytest.mark.parametrize("reason", _PERMANENT_REASONS)
    def test_permanent_refusal_is_non_retryable(self, reason):
        exc = DeferredRefreshError(reason, protocol="lifi", recoverable=False)
        revert_class, disposition = classify_teardown_failure(str(exc))

        assert revert_class == RevertClass.ROUTE_REFRESH_REFUSED
        assert disposition == Disposition.NON_RETRYABLE, (
            "a bundle/config defect cannot be fixed by retrying — surface it and let "
            "teardown proceed to the next risk-reducing intent"
        )

    @pytest.mark.parametrize("reason", _TRANSIENT_REASONS + _PERMANENT_REASONS)
    def test_no_refusal_escalates(self, reason):
        """The one assertion that matters, over every reason and both flags."""
        for recoverable in (True, False):
            exc = DeferredRefreshError(reason, protocol="enso", recoverable=recoverable)
            _, disposition = classify_teardown_failure(str(exc))
            assert disposition != Disposition.ESCALATE, (
                f"refusal escalated (recoverable={recoverable}): {exc}. Escalating asks an "
                f"operator to approve slippage for a failure where no transaction was built."
            )


class TestOrderingAndDegradation:
    """The branch's placement and its failure mode are both load-bearing."""

    def test_an_upstream_error_mentioning_slippage_still_does_not_escalate(self):
        """The refusal embeds the upstream error verbatim, for diagnosability.

        So a route API whose own message says "slippage" would hit the classifier's
        step-1 slippage branch and escalate — on a failed HTTP fetch. This is why
        the refusal branch must precede step 1, not merely step 7.
        """
        exc = DeferredRefreshError(
            "the fresh route request failed: 400 Bad Request: slippage too low for this route",
            protocol="enso",
        )
        revert_class, disposition = classify_teardown_failure(str(exc))

        assert revert_class == RevertClass.ROUTE_REFRESH_REFUSED
        assert disposition == Disposition.RETRY_SAME_LEVEL

    def test_an_untagged_refusal_degrades_to_retry_not_escalate(self):
        """If a future reword drops the tag, the fallback must be the safe one.

        Polarity matters: an untagged refusal reading as RETRY costs a few wasted
        attempts, while reading as ESCALATE re-creates the operator-approval defect
        this test file exists for.
        """
        revert_class, disposition = classify_teardown_failure(
            "Deferred enso bundle refresh refused: something we forgot to tag"
        )

        assert revert_class == RevertClass.ROUTE_REFRESH_REFUSED
        assert disposition == Disposition.RETRY_SAME_LEVEL

    def test_the_tag_is_actually_present_in_the_exception_string(self):
        """Anti-vacuity: the tests above are only meaningful if the tag exists.

        Without this, a producer that stopped emitting the tag would leave every
        assertion above passing via the untagged fallback, and the permanent/
        transient distinction would be silently gone.
        """
        assert "[transient]" in str(DeferredRefreshError("x", protocol="enso", recoverable=True))
        assert "[permanent]" in str(DeferredRefreshError("x", protocol="enso", recoverable=False))
        # ...and adjacent to the matched phrase, since the classifier keys on both.
        assert "bundle refresh refused [permanent]" in str(
            DeferredRefreshError("x", protocol="enso", recoverable=False)
        )


class TestNegativeControls:
    """A classifier that answered RETRY_SAME_LEVEL to everything would pass above."""

    def test_a_genuine_slippage_shortfall_still_escalates(self):
        revert_class, disposition = classify_teardown_failure(
            "execution reverted: Too little received (slippage minimum violated)"
        )
        assert revert_class == RevertClass.SLIPPAGE_MINIMUM_VIOLATED
        assert disposition == Disposition.ESCALATE, (
            "the refusal branch must not swallow real slippage failures — those are "
            "exactly what the ladder exists for"
        )

    def test_an_unrelated_unknown_error_still_escalates(self):
        _, disposition = classify_teardown_failure("execution reverted")
        assert disposition == Disposition.ESCALATE

    def test_insufficient_balance_is_still_non_retryable(self):
        revert_class, disposition = classify_teardown_failure("Insufficient USDC: need 100, have 5 (deficit: 95)")
        assert revert_class == RevertClass.INSUFFICIENT_BALANCE
        assert disposition == Disposition.NON_RETRYABLE


class TestTheEscalationLadderIsWhatWeClaim:
    """Pin the ladder shape the rationale above depends on.

    If level 3 ever became `auto_approve=True`, the argument for this whole file
    would change — better to fail here than to leave the reasoning implicit.
    """

    def test_level_3_requires_operator_approval(self):
        levels = EscalatingSlippageManager.DEFAULT_LEVELS
        by_slippage = {str(lv["slippage"]): lv for lv in levels}

        assert "0.05" in by_slippage, "expected a 5% rung on the ladder"
        assert by_slippage["0.05"]["auto_approve"] is False, (
            "the 5% rung is the operator-approval gate a refusal must never reach"
        )
        # The first two rungs auto-approve, which is why ESCALATE looks harmless
        # until you follow it to rung 3.
        assert by_slippage["0.02"]["auto_approve"] is True
        assert by_slippage["0.03"]["auto_approve"] is True


class TestTheLoopPropertyItself:
    """The classifier is a means; the property that matters is loop continuation.

    Everything above proves `classify_teardown_failure` returns the right
    disposition. That is NOT the same claim as "teardown proceeds to the next
    risk-reducing intent" — the disposition has to survive
    `EscalatingSlippageManager.execute_with_escalation` and then
    `teardown_manager`'s status handling. Raised by an auditor as the cheapest
    remaining hardening: the §Teardown requirement is a loop property, and it was
    proven only by reading the code.
    """

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("label", "recoverable", "expected_status"),
        [
            ("permanent refusal", False, "failed_non_retryable"),
            # Both of these fall through to `failed += 1` and the next `while work:`
            # iteration in `teardown_manager`; neither is the early-return status.
            ("transient refusal", True, "failed_rpc_unreachable"),
        ],
    )
    async def test_a_refusal_never_returns_the_status_that_abandons_the_teardown(
        self, label, recoverable, expected_status
    ):
        """Whatever the refusal, the status must NOT be `paused_awaiting_approval`.

        That is the single value `teardown_manager` turns into an early
        `return TeardownResult(...)`, abandoning every remaining risk-reducing
        intent. Asserted through the real `EscalatingSlippageManager`, because the
        disposition only matters if the manager honours it.
        """
        from almanak.framework.teardown.error_taxonomy import classify_teardown_failure as _classify
        from almanak.framework.teardown.slippage_manager import ExecutionAttempt

        exc = DeferredRefreshError(
            "the fresh route request failed: upstream exploded", protocol="enso", recoverable=recoverable
        )
        _, disposition = _classify(str(exc))

        manager = EscalatingSlippageManager()
        calls: list = []

        async def execute(intent, slippage):
            calls.append(slippage)
            return ExecutionAttempt(
                success=False,
                slippage_used=slippage,
                error=str(exc),
                # The teardown manager sets both from the classifier; mirror that.
                retryable=(disposition != Disposition.NON_RETRYABLE),
                disposition=str(disposition),
            )

        result = await manager.execute_with_escalation(
            intent=object(),
            position_value=Decimal("1000"),
            execute_func=execute,
            # on_approval_needed left None on purpose: that is the configuration in
            # which ESCALATE produced `paused_awaiting_approval` and abandoned the
            # remaining intents, so it is the configuration worth testing.
            teardown_id="t-1",
            deployment_id="d-1",
        )

        assert result.success is False
        assert result.status != "paused_awaiting_approval", (
            f"{label}: a refused route refresh reached the status that ABANDONS the "
            f"remaining risk-reducing intents"
        )
        assert result.status == expected_status, f"{label}: got {result.status}"
        # And it never climbed to the operator-approval rung.
        assert max(calls) <= Decimal("0.03"), (
            f"{label}: escalated to {max(calls)} — past the 2%/3% auto-approve rungs"
        )

    @pytest.mark.asyncio
    async def test_a_genuine_slippage_failure_still_climbs_the_ladder(self):
        """Negative control: the manager must still escalate real slippage failures.

        Without this, a manager that returned `failed_non_retryable` for everything
        would satisfy the assertions above.
        """
        from almanak.framework.teardown.slippage_manager import ExecutionAttempt

        manager = EscalatingSlippageManager()
        calls: list = []

        async def execute(intent, slippage):
            calls.append(slippage)
            return ExecutionAttempt(
                success=False,
                slippage_used=slippage,
                error="execution reverted: Too little received",
                retryable=True,
                disposition="escalate",
            )

        await manager.execute_with_escalation(
            intent=object(),
            position_value=Decimal("1000"),
            execute_func=execute,
            teardown_id="t-2",
            deployment_id="d-2",
        )

        assert max(calls) > Decimal("0.02"), (
            "a genuine slippage shortfall must still climb past the first rung — "
            "that is what the ladder exists for"
        )
