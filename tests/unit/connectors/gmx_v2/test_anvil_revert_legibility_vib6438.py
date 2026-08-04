"""VIB-6438: a reverted GMX keeper transaction must be legible, and honestly classified.

Two defects shared one failure path. Neither lost money alone; together they made a
user-facing GMX failure undiagnosable:

1. the revert reason was discarded at the raise site — the entire diagnostic output
   for a stranded position was ``reverted: 0xec66… (gas_used=…, gas_limit=…)``;
2. the failure was classified ``INFRASTRUCTURE_UNSUPPORTED``, whose own comment
   defines it as structural ("no keeper role, venue rejected the order, wrong
   network"), so the operator was told their infrastructure was unsupported when the
   truth was "this one order reverted, here is why".

Every test here is a NEGATIVE CONTROL: each fails on unmodified origin/main, either by
asserting a reason that main never produces, an enum member main does not define, or a
masking behaviour main exhibits.
"""

from __future__ import annotations

from contextlib import AbstractContextManager
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from almanak.connectors._strategy_base.runner_hook_registry import (
    AsyncSettlementPolicy,
    AsyncSettlementStatus,
    AsyncSettlementVerdict,
)
from almanak.connectors.gmx_v2.anvil_order_executor import (
    GmxAnvilHarnessTransactionRevertedError,
    GmxAnvilOrderExecutionError,
    GmxAnvilOrderExecutionResult,
    GmxAnvilOrderRejectedError,
    GmxAnvilTransactionRevertedError,
    _GmxDependencies,
    _is_transient_execution_error,
    _replay_once,
    _replay_revert_reason,
    _send_transaction,
)
from almanak.connectors.gmx_v2.runner_hooks import GmxV2RunnerHookConnector
from almanak.framework.runner.async_settlement import await_async_settlement

_ORDER_HANDLER = "0x" + "66" * 20
_ORACLE = "0x" + "77" * 20
_TX = "0xec66cba5"

# ``Error(string)`` payload for the GMX message the reported run could not surface.
_REVERT_MESSAGE = "OrderNotFulfillableAtAcceptablePrice"

_DEPS = _GmxDependencies(
    order_handler=_ORDER_HANDLER,
    oracle=_ORACLE,
    role_store="0x" + "88" * 20,
    data_store="0x" + "99" * 20,
    reader="0x" + "aa" * 20,
)


def _error_string_blob(message: str) -> str:
    """A REAL ``Error(string)`` payload, padded to the 32-byte ABI boundary.

    The padding is not cosmetic. ``_REVERT_MESSAGE`` is 36 bytes, so an earlier
    ``.ljust(64, "0")`` was a no-op and the fixture ended mid-word: 200 hex
    characters where the venue emits 256. Strict ``eth_abi.decode(["string"], …)``
    rejects that blob, so the tests below were asserting against a shape GMX
    never emits and only passed because the production decoder slices by length.
    """
    body = message.encode()
    padded = body.hex().ljust(((len(body) + 31) // 32) * 64, "0")
    return "0x08c379a0" + format(32, "064x") + format(len(body), "064x") + padded


def _reverted_send(provider: MagicMock) -> AbstractContextManager[Any]:
    """Drive ``_send_transaction`` to a mined-but-reverted receipt."""
    web3 = MagicMock()
    web3.eth.wait_for_transaction_receipt.return_value = {
        "status": 0,
        "gasUsed": 3_213_345,
        "blockNumber": 4_242,
    }
    return patch.multiple(
        "almanak.connectors.gmx_v2.anvil_order_executor",
        _rpc=MagicMock(side_effect=("0x3e17de", "0x1", "0xffffffffffffffff", _TX)),
        Web3=MagicMock(return_value=web3),
    )


def _rpc_by_method(_provider: object, method: str, _params: list) -> object:
    """Dispatch by JSON-RPC method so the fake cannot silently run out of answers.

    A fixed ``side_effect`` sequence raises ``StopIteration`` — whose ``str()`` is
    empty — the moment the executor makes one more call than expected, which
    reads as an unrelated failure rather than a broken fixture.
    """
    return {
        "eth_estimateGas": "0x3e17de",
        "eth_gasPrice": "0x1",
        "eth_getBalance": "0xffffffffffffffff",
        "eth_sendTransaction": _TX,
    }.get(method, "0x")


def _run_reverted(provider: MagicMock) -> GmxAnvilTransactionRevertedError:
    with _reverted_send(provider), pytest.raises(GmxAnvilTransactionRevertedError) as excinfo:
        _send_transaction(provider, _ORDER_HANDLER, _ORACLE, "0x1234", kind="order")
    return excinfo.value


# ---------------------------------------------------------------------------
# Defect 1 — the revert reason must reach the raised error
# ---------------------------------------------------------------------------


def test_mined_revert_replays_and_carries_the_decoded_reason() -> None:
    """NEGATIVE CONTROL: main raises gas numbers only, never the reason."""
    provider = MagicMock()
    provider.make_request.return_value = {
        "error": {"code": 3, "message": "execution reverted", "data": _error_string_blob(_REVERT_MESSAGE)}
    }

    message = str(_run_reverted(provider))

    # The datum that identifies the root cause in seconds.
    assert _REVERT_MESSAGE in message
    assert "eth_call replay reverted" in message
    # The gas numbers main already had must not be lost.
    assert "gas_used=3213345" in message
    assert _TX in message

    # The replay targets the PARENT block: eth_call at block N runs against
    # post-N state, but the transaction ran against post-(N-1) state.
    replay = provider.make_request.call_args_list[0]
    assert replay.args[0] == "eth_call"
    assert replay.args[1][1] == hex(4_241)
    # A replay must never resubmit: eth_call only, no eth_sendTransaction.
    assert all(c.args[0] != "eth_sendTransaction" for c in provider.make_request.call_args_list)


def test_replay_carries_the_transactions_own_gas_limit() -> None:
    """The faithful probe must not let the node substitute unbounded gas.

    Dropping ``gas`` is what turns the VIB-6437 shape into a false negative.
    """
    provider = MagicMock()
    provider.make_request.return_value = {
        "error": {"code": 3, "message": "execution reverted", "data": _error_string_blob(_REVERT_MESSAGE)}
    }

    _run_reverted(provider)

    first_call = provider.make_request.call_args_list[0].args[1][0]
    # The submitted limit is the estimate (0x3e17de) plus the VIB-6450 drift
    # headroom — the replay must carry what was actually SUBMITTED, which is
    # exactly what this invariant protects.
    from almanak.connectors.gmx_v2.anvil_order_executor import _submitted_gas_limit

    assert first_call["gas"] == hex(_submitted_gas_limit(0x3E17DE)), (
        "the faithful replay must reuse the submitted gas limit"
    )


def test_a_returned_transport_error_is_inconclusive_not_a_measured_revert() -> None:
    """`GatewayWeb3Provider` RETURNS transport failures as `-32603` error objects.

    `make_request` does not raise for most transport failures
    (`gateway_provider.py:132-139`), so an error object is NOT proof of a revert.
    Reporting a dropped channel as `eth_call replay reverted: …` would tell the
    operator the venue rejected their order when nothing was measured at all.
    """
    provider = MagicMock()
    provider.make_request.return_value = {
        "error": {"code": -32603, "message": "Gateway RPC call failed: channel closed"}
    }

    message = str(_run_reverted(provider))

    assert "replay unavailable" in message
    assert "no answer from the node" in message
    # Empty != Zero: an unmeasured replay is neither a revert nor a non-revert.
    assert "replay reverted" not in message
    assert "SUCCEEDED" not in message
    # The original failure is still intact underneath.
    assert _TX in message
    assert "gas_used=3213345" in message


def test_a_replay_that_succeeds_at_the_submitted_gas_rules_gas_out_honestly() -> None:
    """No guessing: same gas, same state, no revert ⇒ say what is actually known."""
    provider = MagicMock()
    provider.make_request.return_value = {"result": "0x"}

    message = str(_run_reverted(provider))

    assert "SUCCEEDED at the submitted gas limit" in message
    assert "nor gas exhaustion" in message
    assert "state ordering" in message


def test_a_failing_diagnostic_never_masks_the_original_revert() -> None:
    """THE TRAP: an exception inside the diagnostic must not replace the real error.

    Diagnostics are added to an error path. If the probe raises, the caller must
    still receive the revert — losing it would be strictly worse than no
    diagnosis at all.
    """
    provider = MagicMock()
    provider.make_request.side_effect = RuntimeError("gateway channel dropped mid-diagnosis")

    error = _run_reverted(provider)
    message = str(error)

    # The original failure survived, intact.
    assert isinstance(error, GmxAnvilOrderExecutionError)
    assert _TX in message
    assert "gas_used=3213345" in message
    assert "measured_gas_limit=4069342" in message
    # And the diagnostic's own failure is disclosed rather than silently dropped.
    assert "replay unavailable (RuntimeError)" in message
    # The probe's exception never becomes the raised exception.
    assert "gateway channel dropped mid-diagnosis" not in message


def test_undecodable_revert_data_still_degrades_to_a_useful_message() -> None:
    provider = MagicMock()
    provider.make_request.return_value = {"error": {"code": 3, "message": "execution reverted", "data": "0x"}}

    message = str(_run_reverted(provider))

    assert "no decodable reason" in message
    assert _TX in message


# ---------------------------------------------------------------------------
# Defect 2 — an order-level rejection is not a structural failure
# ---------------------------------------------------------------------------


def test_a_mined_revert_is_never_classified_transient() -> None:
    """The decoded reason now rides on the message; a contract's own error text
    must never be pattern-matched against the transport markers."""
    assert not _is_transient_execution_error(
        GmxAnvilTransactionRevertedError("reverted — eth_call replay reverted: error sending request for url")
    )


def test_result_cannot_claim_both_transient_and_order_rejected() -> None:
    with pytest.raises(ValueError):
        GmxAnvilOrderExecutionResult(ok=False, transient=True, order_rejected=True)


def _classify(result: GmxAnvilOrderExecutionResult) -> AsyncSettlementVerdict:
    baseline = AsyncSettlementVerdict(
        status=AsyncSettlementStatus.PENDING,
        terminal=False,
        observation_state=object(),
    )
    connector = GmxV2RunnerHookConnector()
    with (
        patch("almanak.connectors.gmx_v2.runner_hooks.execute_pending_orders_on_anvil", return_value=result),
        patch.object(connector, "observe_async_orders", return_value=baseline),
    ):
        return connector.execute_pending_orders_for_test(
            gateway_client=object(),
            chain="arbitrum",
            wallet_address="0xabc",
            orders=(SimpleNamespace(order_key="0x01", protocol="gmx_v2"),),
            intent=SimpleNamespace(intent_type="PERP_CLOSE"),
            network="anvil",
        )


def test_reverted_order_is_order_rejected_not_infrastructure_unsupported() -> None:
    """NEGATIVE CONTROL: main maps this to INFRASTRUCTURE_UNSUPPORTED."""
    verdict = _classify(
        GmxAnvilOrderExecutionResult(
            ok=False,
            reason=f"GmxAnvilTransactionRevertedError: … — eth_call replay reverted: {_REVERT_MESSAGE}",
            order_rejected=True,
        )
    )

    assert verdict.status is AsyncSettlementStatus.ORDER_REJECTED
    assert verdict.status is not AsyncSettlementStatus.INFRASTRUCTURE_UNSUPPORTED
    # The reason the operator reads now names the cause.
    assert _REVERT_MESSAGE in (verdict.reason or "")
    # MUST-NOT-CHANGE: still a loud, non-terminal, non-retryable failure.
    assert verdict.terminal is False


def test_a_real_mined_revert_reaches_the_classifier_as_order_rejected() -> None:
    """REACHABILITY: the whole path, not a hand-built result.

    The unit tests above construct ``GmxAnvilOrderExecutionResult`` directly, which
    proves the classifier but not that anything ever SETS ``order_rejected``. This
    drives an actual reverted receipt through ``execute_pending_orders_on_anvil``
    and asserts both the flag and the decoded reason survive the whole way to the
    settlement verdict — the property that makes the fix non-inert.
    """
    from almanak.connectors.gmx_v2 import anvil_order_executor as mod

    provider = MagicMock()
    provider.make_request.return_value = {
        "error": {"code": 3, "message": "execution reverted", "data": _error_string_blob(_REVERT_MESSAGE)}
    }
    web3 = MagicMock()
    web3.eth.wait_for_transaction_receipt.return_value = {
        "status": 0,
        "gasUsed": 3_213_345,
        "blockNumber": 4_242,
    }
    orders = SimpleNamespace(
        ok=True,
        order_keys=["0x" + "11" * 32],
        orders=(SimpleNamespace(order_key="0x" + "11" * 32, market="0x" + "44" * 20),),
        truncated=False,
        error=None,
    )
    with (
        patch.object(mod, "read_pending_orders", return_value=orders),
        patch.object(mod, "GatewayWeb3Provider", return_value=provider),
        patch.object(mod, "_load_dependencies", return_value=_DEPS),
        patch.object(mod, "_has_role", return_value=True),
        patch.object(mod, "_find_order_keeper", return_value="0x" + "bb" * 20),
        patch.object(mod, "_oracle_price_count", return_value=0),
        patch.object(mod, "_seed_oracle_prices", return_value=[]),
        patch.object(mod, "_clear_seeded_oracle_prices"),
        patch.object(mod, "_rpc", side_effect=_rpc_by_method),
        patch.object(mod, "Web3", return_value=web3),
    ):
        result = mod.execute_pending_orders_on_anvil(
            gateway_client=object(),
            chain="arbitrum",
            wallet_address="0x" + "33" * 20,
            orders=(SimpleNamespace(order_id="0x" + "11" * 32),),
            network="anvil",
        )

    assert result.ok is False
    assert result.order_rejected is True
    assert result.transient is False
    assert _REVERT_MESSAGE in (result.reason or "")

    # ... and that result classifies as ORDER_REJECTED, reason intact.
    verdict = _classify(result)
    assert verdict.status is AsyncSettlementStatus.ORDER_REJECTED
    assert _REVERT_MESSAGE in (verdict.reason or "")


def test_genuinely_structural_failures_stay_infrastructure_unsupported() -> None:
    """MUST-NOT-CHANGE: the split must not drain the structural bucket."""
    verdict = _classify(GmxAnvilOrderExecutionResult(ok=False, reason="no authorized keeper"))
    assert verdict.status is AsyncSettlementStatus.INFRASTRUCTURE_UNSUPPORTED


def test_transient_failures_stay_retryable() -> None:
    """MUST-NOT-CHANGE: the transient carve-out is untouched."""
    verdict = _classify(GmxAnvilOrderExecutionResult(ok=False, reason="cold fork", transient=True))
    assert verdict.status is AsyncSettlementStatus.OBSERVATION_FAILED


# ---------------------------------------------------------------------------
# The barrier must treat the new status exactly like the old one
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_order_rejected_stops_immediately_and_never_resubmits() -> None:
    """The whole point of the split is legibility, NOT retryability.

    Without ``ORDER_REJECTED`` in the barrier's immediate-stop set the lane falls
    through to ``return None`` and polls for the full 360s budget for an order
    that is already definitively finished.
    """
    registry = MagicMock()
    registry.async_settlement_policy.return_value = AsyncSettlementPolicy(360, 5, True, True)
    registry.execute_pending_orders_for_test.return_value = AsyncSettlementVerdict(
        status=AsyncSettlementStatus.ORDER_REJECTED,
        terminal=False,
        reason=f"eth_call replay reverted: {_REVERT_MESSAGE}",
    )
    with patch(
        "almanak.connectors._strategy_runner_hook_registry.STRATEGY_RUNNER_HOOK_REGISTRY",
        registry,
    ):
        result = await await_async_settlement(
            gateway_client=object(),
            chain="arbitrum",
            wallet_address="0xabc",
            network="anvil",
            orders=(SimpleNamespace(protocol="gmx_v2", order_key="0x01"),),
            intent=object(),
        )

    # Identical stop semantics to INFRASTRUCTURE_UNSUPPORTED: one attempt, no
    # second execution, budget untouched.
    assert result.status is AsyncSettlementStatus.ORDER_REJECTED
    assert result.attempts == 1
    assert result.terminal is False
    assert registry.execute_pending_orders_for_test.call_count == 1
    # The reason survives into the barrier result the operator sees.
    assert _REVERT_MESSAGE in (result.reason or "")
    # It is a failure, so every `== "SETTLED"` consumer keeps failing closed.
    assert result.status.value != "SETTLED"


def test_every_settlement_status_has_an_explicit_barrier_decision() -> None:
    """Census — a new member must not silently acquire the fall-through path.

    ``AsyncSettlementStatus`` has no ``assert_never`` anywhere, so an added member
    is accepted silently by both mypy and the runtime while falling through the
    managed-Anvil lane into a full-timeout poll. This test is the tripwire: adding
    a member without deciding its stop/retry semantics fails here.
    """
    from almanak.framework.runner.async_settlement import _IMMEDIATE_STOP_STATUSES

    # Statuses a connector can return from execute_pending_orders_for_test, and
    # what the barrier must do with each.
    retryable = {AsyncSettlementStatus.OBSERVATION_FAILED}
    immediate = {
        AsyncSettlementStatus.INFRASTRUCTURE_UNSUPPORTED,
        AsyncSettlementStatus.ORDER_REJECTED,
    }
    # Terminal outcomes stop via the `verdict.terminal` flag, not the set.
    terminal_by_flag = {
        AsyncSettlementStatus.SETTLED,
        AsyncSettlementStatus.TERMINAL_FAILED,
        AsyncSettlementStatus.PENDING,
        AsyncSettlementStatus.PENDING_SETTLEMENT_TIMEOUT,
    }

    classified = retryable | immediate | terminal_by_flag
    unclassified = set(AsyncSettlementStatus) - classified
    assert not unclassified, (
        f"AsyncSettlementStatus member(s) {sorted(s.value for s in unclassified)} have no declared "
        "barrier semantics. Decide explicitly: add to _IMMEDIATE_STOP_STATUSES in "
        "almanak/framework/runner/async_settlement.py (stops the managed-Anvil lane at once) "
        "or document why it is terminal-by-flag. Omitting it means a silent full-timeout poll."
    )
    assert _IMMEDIATE_STOP_STATUSES == immediate | retryable


def test_a_harness_oracle_revert_is_not_an_order_rejection() -> None:
    """VIB-6438 blocker 1: only the ORDER call may be an order-level rejection.

    ``_send_transaction`` has four call sites and three are harness oracle
    setup/cleanup. Deriving ``order_rejected`` from the exception type alone told
    the operator "the venue rejected THIS order" when our own plumbing failed --
    the same mislabel this ticket fixes, pointed the other way. Worst reachable
    shape: ``executeOrder`` FILLS, then ``clearAllPrices`` reverts.
    """
    # The replay response is set EXPLICITLY. A bare ``MagicMock()`` provider
    # returns a MagicMock from ``make_request``, which the old unsafe default
    # read as a successful replay -- so this test's transient assertion below
    # was green on a diagnosis that could not contain a marker, i.e. it asserted
    # the right property on a path that could never break it (PANEL finding).
    provider = MagicMock()
    provider.make_request.return_value = {
        "error": {"code": 3, "message": "execution reverted", "data": _error_string_blob("HarnessOracleRejected")}
    }
    with _reverted_send(provider), pytest.raises(GmxAnvilOrderExecutionError) as excinfo:
        _send_transaction(provider, _ORDER_HANDLER, _ORACLE, "0x1234", kind="harness")

    exc = excinfo.value
    assert not isinstance(exc, GmxAnvilOrderRejectedError), (
        "a harness oracle revert must NOT classify as an order-level rejection"
    )
    assert "harness transaction reverted" in str(exc)
    # And it must still be non-retryable: a mined revert is a definitive answer.
    assert _is_transient_execution_error(exc) is False


def test_the_error_string_fixture_is_a_payload_the_venue_could_actually_emit() -> None:
    """PANEL (CodeRabbit, minor): the fixture must be REAL ABI, not merely decodable.

    Without this, ``_error_string_blob`` silently produced a 200-hex-char blob
    where GMX emits 256, and every revert-reason test above asserted against a
    shape no venue can send. They passed only because the production decoder
    slices by length -- so a decoder that required valid ABI would have been
    declared working by a suite that never fed it valid ABI.

    NEGATIVE CONTROL: restore the ``.ljust(64, "0")`` no-op and this fails.
    """
    from eth_abi import decode as abi_decode
    from eth_abi import encode as abi_encode

    blob = _error_string_blob(_REVERT_MESSAGE)

    # Byte-identical to what eth_abi itself would produce for Error(string).
    assert blob == "0x08c379a0" + abi_encode(["string"], [_REVERT_MESSAGE]).hex()
    # And strictly decodable, which the unpadded blob was not.
    assert abi_decode(["string"], bytes.fromhex(blob[10:])) == (_REVERT_MESSAGE,)


def test_a_harness_revert_quoting_a_transport_phrase_is_still_not_transient() -> None:
    """PANEL (CodeRabbit, major): the diagnosis text can quote a transient marker.

    The test above passes even without a dedicated harness type, because its
    provider mock never yields an error object -- the replay reports SUCCEEDED
    and the message contains no marker. That is the vulnerable path left
    unexercised.

    Here the replay hits a transport failure whose text is ``error sending
    request``, marker 4 of 4 in ``_TRANSIENT_ERROR_MARKERS``, and the executor
    embeds it verbatim in the raised message. Before ``kind="harness"`` had its
    own exception type, ``_is_transient_execution_error`` fell through to the
    substring scan and returned True: a mined, deterministic harness revert was
    retried as a transport blip until the barrier's budget ran out, and the
    operator was handed a timeout instead of the revert.

    NEGATIVE CONTROL: raising the bare parent here makes this assertion fail.
    """
    provider = MagicMock()
    provider.make_request.return_value = {
        "error": {"code": -32603, "message": "error sending request for url (http://127.0.0.1:8545/)"}
    }

    with _reverted_send(provider), pytest.raises(GmxAnvilHarnessTransactionRevertedError) as excinfo:
        _send_transaction(provider, _ORDER_HANDLER, _ORACLE, "0x1234", kind="harness")

    exc = excinfo.value
    # The marker really is present in the message -- otherwise this test would
    # pass for the wrong reason, exactly like the one above.
    assert "error sending request" in str(exc)
    assert _is_transient_execution_error(exc) is False, (
        "a mined harness revert must never be retried just because its diagnosis quotes a transport phrase"
    )
    # It is definitive, but it is still NOT an order-level rejection.
    assert not isinstance(exc, GmxAnvilOrderRejectedError)


def test_a_malformed_error_object_reads_as_unmeasured_not_as_a_successful_replay() -> None:
    """PANEL (blocker): success is the ABSENCE of an error, not an unparseable one.

    ``_replay_once`` classified anything whose ``error`` was not a dict as
    ``success``. That is a fabricated measurement: a lost answer reported as "the
    replay SUCCEEDED", telling the operator the mined failure is not reproducible
    when nothing was measured at all. The PR's transport tests covered only the
    ``{"code": -32603, ...}`` dict shape.

    Reachable: the gateway forwards upstream JSON-RPC errors verbatim and its own
    code guards ``isinstance(rpc_error, dict)``, so a non-dict error is a shape
    the transport really produces.

    NEGATIVE CONTROL: restore ``if not isinstance(error, dict): return
    _ReplayProbe(outcome="success")`` and this reports SUCCEEDED.

    The malformed error is returned on the probe that ACTUALLY RUNS. An earlier
    revision returned it only on the ``"gas" not in params`` branch -- which was
    the second, unbounded-gas probe. When that probe was deleted the branch became
    unreachable, and this test kept passing with the defect restored while its
    docstring still claimed to control it. A test that names a negative control it
    does not perform is worse than no test, because it is read as coverage.
    """
    provider = MagicMock()
    # The single surviving probe always carries ``gas`` (_send_transaction submits
    # hex(_submitted_gas_limit(gas_estimate)) — estimate plus the bounded
    # VIB-6450 headroom), so this is the shape it really receives.
    provider.make_request.return_value = {"error": "upstream exploded"}

    message = str(_run_reverted(provider))

    assert "replay unavailable" in message
    assert "no answer from the node" in message
    # The fabricated verdict the old default produced.
    assert "SUCCEEDED" not in message
    # The original failure survives underneath the failed diagnosis.
    assert _TX in message


def test_every_non_dict_error_shape_reads_as_unmeasured() -> None:
    """Empty != Zero across every shape ``json.loads`` can hand back."""
    for shape in ("upstream exploded", ["a"], 42, None):
        provider = MagicMock()
        provider.make_request.return_value = {"error": shape}
        probe = _replay_once(provider, {}, "0x1")
        assert probe.outcome == "transport", f"{shape!r} must be unmeasured, not a measured non-revert"

    # And a genuine success -- no ``error`` key at all -- is still a success.
    provider = MagicMock()
    provider.make_request.return_value = {"jsonrpc": "2.0", "id": 1, "result": "0x"}
    assert _replay_once(provider, {}, "0x1").outcome == "success"


def test_an_out_of_gas_replay_is_a_measured_failure_not_a_lost_answer() -> None:
    """Gas exhaustion is MEASURED, and must not read as "no answer from the node".

    Anvil 1.5.1 returns ``{"code": -32603, "message": "EVM error OutOfGas"}``:
    no revert data, no ``revert`` substring, and the SAME ``-32603`` the gateway
    uses for real transport failures, so every arm of ``looks_like_revert``
    misses it. Classified as transport, the operator is told nothing was
    measured when the node answered plainly.

    Reachable because ``_send_transaction`` submits ``hex(gas_estimate)`` with
    ZERO buffer.

    NEGATIVE CONTROL: drop ``_is_out_of_gas`` from the executed-failure guard and
    this reports "replay unavailable" instead.
    """
    provider = MagicMock()
    provider.make_request.return_value = {"error": {"code": -32603, "message": "EVM error OutOfGas"}}

    message = str(_run_reverted(provider))

    assert "replay reverted" in message
    # The node's own words must survive: the ABI decoders have nothing to decode
    # here, so falling through to them would discard the phrase that names it.
    assert "OutOfGas" in message
    # Empty != Zero: a measured outcome must not read as an unmeasured one.
    assert "no answer from the node" not in message


def test_an_upstream_body_merely_mentioning_gas_is_not_an_executed_failure() -> None:
    """VIB-6481: the out-of-gas match is an ALLOWLIST, not a substring scan.

    The gateway embeds raw upstream text into TRANSPORT error messages
    (``rpc_service.py``: ``f"HTTP {status}: {error_text}"``). An unanchored
    ``"outofgas" in message`` therefore classified an upstream 5xx body that
    merely mentions gas as an EXECUTED revert -- reporting a lost answer as a
    measured one, the exact inversion this module exists to prevent.

    NEGATIVE CONTROL: replace the allowlist with the old
    ``"outofgas" in message.lower().replace(" ", "")`` and this fails.
    """
    provider = MagicMock()
    provider.make_request.return_value = {
        "error": {"code": -32603, "message": "HTTP 502: <html>upstream proxy ran out of gas budget</html>"}
    }

    message = str(_run_reverted(provider))

    assert "replay unavailable" in message, "an upstream transport body is not a measured revert"
    assert "no answer from the node" in message
    assert "replay reverted" not in message


def test_a_venue_cancellation_is_an_order_rejection_even_though_the_tx_succeeded() -> None:
    """VIB-6438 blocker 2: the DOMINANT GMX rejection is a cancel, not a revert.

    GMX cancels or freezes the order inside a SUCCESSFUL ``executeOrder``
    transaction for acceptable-price bounds, open-interest caps and collateral
    floors, so this -- not the revert -- is the commonest real order-level
    rejection. Raising the plain parent here left it classified
    INFRASTRUCTURE_UNSUPPORTED, the exact defect this ticket fixes.

    Drives the REAL ``GMXv2ReceiptParser`` over a properly ABI-encoded
    EventEmitter log: topic[1] is the true ``OrderCancelled`` name hash from
    ``EVENT_TOPICS`` and the payload is encoded with the production
    ``_EVENT_LOG_DATA_ABI_TYPE``. An earlier version patched ``parse_logs``
    wholesale, so it proved only that the branch fires on a shape the test
    itself invented -- it could not have caught a parser/branch mismatch, which
    is the way this fix would actually be inert in production.
    """
    from eth_abi import encode as abi_encode

    from almanak.connectors.gmx_v2.anvil_order_executor import _verify_execution_outcome
    from almanak.connectors.gmx_v2.receipt_parser import _EVENT_LOG_DATA_ABI_TYPE, EVENT_TOPICS

    key = "0x" + "1a" * 32
    empty: tuple[list, list] = ([], [])
    payload = abi_encode(
        ["address", "string", _EVENT_LOG_DATA_ABI_TYPE],
        [
            "0x" + "11" * 20,
            "OrderCancelled",
            (
                empty,
                empty,
                empty,
                empty,
                ([("key", bytes.fromhex(key[2:]))], []),
                empty,
                ([("reason", _REVERT_MESSAGE)], []),
            ),
        ],
    )
    receipt = {
        "logs": [
            {
                "topics": ["0x" + "ee" * 32, EVENT_TOPICS["OrderCancelled"], key],
                "data": "0x" + payload.hex(),
                "address": _ORDER_HANDLER,
                "logIndex": 0,
            }
        ]
    }

    with pytest.raises(GmxAnvilOrderExecutionError) as excinfo:
        _verify_execution_outcome(receipt, key, _TX)

    exc = excinfo.value
    assert isinstance(exc, GmxAnvilOrderRejectedError), (
        "a venue cancellation IS an order-level rejection; raising the plain parent "
        "leaves the dominant GMX rejection classified INFRASTRUCTURE_UNSUPPORTED"
    )
    # The venue's own reason must survive to the operator, decoded from the payload.
    assert _REVERT_MESSAGE in str(exc)
    assert _is_transient_execution_error(exc) is False


def test_the_replay_carries_the_fee_fields_that_can_decide_the_outcome() -> None:
    """VIB-6438: GMX validates executionFee >= gasLimit * tx.gasprice.

    Dropping ``gasPrice`` lets a fee-caused revert replay as a success and
    exonerate the true cause -- the same false negative as dropping ``gas``.
    """
    provider = MagicMock()
    provider.make_request.return_value = {
        "error": {"code": 3, "message": "execution reverted", "data": _error_string_blob(_REVERT_MESSAGE)}
    }
    _run_reverted(provider)

    first_call = provider.make_request.call_args_list[0].args[1][0]
    assert "gasPrice" in first_call, "the faithful replay must carry the submitted gasPrice"


def test_an_unknown_receipt_block_is_inconclusive_not_a_head_replay() -> None:
    """VIB-6438: replaying at head measures POST-transaction state.

    A missing/unparsable ``blockNumber`` previously fell back to ``latest``, so
    the probe could report a confident "SUCCEEDED" measured against the wrong
    state -- the exact off-by-one the docstring rules out.
    """
    provider = MagicMock()
    message = _replay_revert_reason(provider, {"from": "0x1", "to": "0x2", "data": "0x"}, 0)

    assert "unavailable" in message
    assert "post-transaction state" in message
    provider.make_request.assert_not_called()
