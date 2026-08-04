"""The `strat test` teardown verdict must not contradict its own chain evidence (ALM-3109).

`_teardown_step_ok` decided PASS/FAIL from `strategy_instance.get_open_positions()`
— the strategy's own bookkeeping — while the teardown lane's on-chain
reconciliation ran alongside it and was not an input at all. Perp strategies track
state from *requested* sizes (the SDK exposes no strategy-side chain read of a perp
position), so after a fill divergence the cache is wrong and the harness published
it as a measurement. The reported artifact contains both
``TD-08 Plan-A post-teardown reconciliation: 1/1 known positions chain-confirmed
CLOSED ... 0 still open`` and ``"teardown_passed": false`` with a $406.35 phantom
position, in the same file.

The fix lets a POSITIVELY MEASURED chain verdict override a contradicting cache —
and the whole risk of that fix is the opposite direction, a false PASS certifying a
teardown that really did strand a position. So the fail-closed half of the
behaviour is tested at least as hard as the reported defect:

* an unmeasured / partial / absent chain read never certifies (Empty ≠ Zero);
* the chain measuring a position OPEN never certifies;
* the VIB-6285 unmeasured-cache-read guard is not weakened by chain certification.
"""

import json
import re
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.cli._run_modes import _chain_certified_closure, _teardown_step_ok
from almanak.framework.cli.run_helpers import _run_test_lifecycle
from almanak.framework.runner.runner_models import IterationResult, IterationStatus
from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary, TeardownProfile

# ---------------------------------------------------------------------------
# Harness (mirrors tests/unit/cli/test_strat_test_teardown_residual.py)
# ---------------------------------------------------------------------------


def _parse_last_json_object(stream: str) -> dict:
    decoder = json.JSONDecoder()
    for m in reversed(list(re.finditer(r"^\{", stream, re.MULTILINE))):
        try:
            payload, _ = decoder.raw_decode(stream[m.start() :])
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            return payload
    raise AssertionError(f"No JSON object found in stream:\n{stream}")


def _make_runner(iteration: IterationResult, closure_evidence: object = None) -> MagicMock:
    runner = MagicMock()
    runner.setup_gateway_integration = MagicMock()
    runner.teardown_gateway_integration = MagicMock()
    runner._emit_iteration_summary = MagicMock()
    runner.run_iteration = AsyncMock(side_effect=[iteration])
    runner.config = MagicMock(enable_state_persistence=False)
    runner._capture_portfolio_snapshot = AsyncMock()
    # The real lane resets this to None at the top of every teardown and only the
    # verify lane writes it. ``None`` is the honest default for "this lane
    # produced no chain measurement".
    runner._teardown_closure_verification = closure_evidence
    return runner


def _make_strategy(open_positions: list[PositionInfo] | None = None, error: Exception | None = None) -> MagicMock:
    s = MagicMock(
        spec=[
            "deployment_id",
            "STRATEGY_NAME",
            "chain",
            "force_action",
            "load_state_async",
            "_wallet_activity_provider",
            "flush_pending_saves",
            "get_open_positions",
            "get_teardown_profile",
        ]
    )
    s.deployment_id = "TestStrategy:abc"
    s.STRATEGY_NAME = "TestStrategy"
    s.chain = "arbitrum"
    s.force_action = ""
    s.load_state_async = AsyncMock(return_value=False)
    s._wallet_activity_provider = None
    s.flush_pending_saves = AsyncMock()
    if error is not None:
        s.get_open_positions = MagicMock(side_effect=error)
    else:
        s.get_open_positions = MagicMock(
            return_value=TeardownPositionSummary(
                deployment_id="TestStrategy:abc",
                timestamp=datetime.now(UTC),
                positions=open_positions or [],
            )
        )
    s.get_teardown_profile = MagicMock(return_value=TeardownProfile())
    return s


def _phantom_perp(value_usd: str = "406.3525") -> PositionInfo:
    """The exact shape of the reported phantom: a perp the strategy still believes open."""
    return PositionInfo(
        position_type=PositionType.PERP,
        position_id="gmx-v2-ARB/USD-short",
        chain="arbitrum",
        protocol="gmx_v2",
        value_usd=Decimal(value_usd),
    )


def _chain_says_closed(**overrides) -> dict:
    """The evidence a teardown emits when the chain MEASURED the known set closed.

    Mirrors ``_teardown_helpers.closure_chain_evidence`` for a one-position GMX
    perp teardown: Plan-A's PERP arm returned ``DIVERGED_CLOSED``, so ``gmx_v2``
    is a measured-closed protocol and the VIB-6285 ratchet is satisfied.
    ``verification_status`` stays ``unverified`` on purpose — GMX has no TD-14
    post-condition for every position, so CHAIN_VERIFIED is unreachable here and
    gating on it would make the fix inert for the very lifecycle that reported it.
    """
    evidence = {
        "verification_status": "unverified",
        "all_closed": True,
        "closure_unknown": False,
        "has_position_breakdown": True,
        "positions_total": 1,
        "positions_closed": 1,
        "protocols_to_prove": ["gmx_v2"],
        "measured_closed_protocols": ["gmx_v2"],
        "unproven_protocols": [],
    }
    evidence.update(overrides)
    return evidence


def _run(strategy, runner, monkeypatch) -> int:
    monkeypatch.setattr(
        "almanak.framework.teardown.get_teardown_state_manager",
        lambda *a, **k: MagicMock(create_request=MagicMock()),
    )
    return _run_test_lifecycle(
        runner=runner,
        strategy_instance=strategy,
        state_manager=MagicMock(),
        cleanup_fn=AsyncMock(),
        actions=[],
        teardown=True,
        json_output=True,
    )


# ---------------------------------------------------------------------------
# 1. The reported defect: chain-confirmed CLOSED + phantom cache row
# ---------------------------------------------------------------------------


def test_chain_confirmed_closure_beats_a_phantom_cached_position(capsys, monkeypatch):
    """ALM-3109 ask 3: chain measured the perp CLOSED; the cache still shows $406.35.

    Before the fix this run printed ``teardown_passed: false`` in the same
    artifact that said ``chain-confirmed CLOSED ... 0 still open``.
    """
    strategy = _make_strategy(open_positions=[_phantom_perp()])
    runner = _make_runner(
        IterationResult(status=IterationStatus.TEARDOWN, deployment_id="TestStrategy:abc"),
        closure_evidence=_chain_says_closed(),
    )
    exit_code = _run(strategy, runner, monkeypatch)
    payload = _parse_last_json_object(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["summary"]["teardown_passed"] is True
    step = payload["steps"][0]
    # The stale cache rows are NOT dropped — the strategy-side bookkeeping bug is
    # real and must stay visible — but they are explicitly labelled.
    assert step["open_positions_after_teardown"][0]["position_id"] == "gmx-v2-ARB/USD-short"
    assert "gmx_v2" in step["open_positions_chain_disagreement"]
    assert "not a chain read" in step["open_positions_chain_disagreement"]
    # The chain evidence itself rides the artifact, so the verdict is auditable.
    assert step["teardown_closure"]["measured_closed_protocols"] == ["gmx_v2"]


def test_disagreement_message_degrades_instead_of_raising_on_a_missing_protocol_list(capsys, monkeypatch):
    """The formatter must not subscript a key the GATE never validates.

    ``_chain_certified_closure`` proves ``teardown_closure`` is a dict with
    int-able counters. It reads neither ``measured_closed_protocols`` nor
    ``unproven_protocols``, so certification is NOT proof that either key is
    present. The disagreement string is a MESSAGE about the verdict, not the
    verdict itself: on a producer that omitted the list it must degrade to
    "none", never raise and take an otherwise-correct PASS down with it.

    Raised independently by two panel reviewers on PR #3584. The shipped
    ``closure_chain_evidence`` always emits all nine keys, so this pins the
    formatter's contract rather than a reachable defect.
    """
    evidence = _chain_says_closed()
    del evidence["measured_closed_protocols"]
    strategy = _make_strategy(open_positions=[_phantom_perp()])
    runner = _make_runner(
        IterationResult(status=IterationStatus.TEARDOWN, deployment_id="TestStrategy:abc"),
        closure_evidence=evidence,
    )
    exit_code = _run(strategy, runner, monkeypatch)
    payload = _parse_last_json_object(capsys.readouterr().out)

    # The verdict is unchanged — the gate never needed that key.
    assert exit_code == 0
    assert payload["summary"]["teardown_passed"] is True
    step = payload["steps"][0]
    # The message still lands, naming the absence rather than blowing up.
    assert "protocols proven: none" in step["open_positions_chain_disagreement"]
    assert "not a chain read" in step["open_positions_chain_disagreement"]


def test_agreeing_clean_teardown_still_passes_unchanged(capsys, monkeypatch):
    """Chain and cache agree on flat: pass, and no disagreement marker is invented."""
    strategy = _make_strategy(open_positions=[])
    runner = _make_runner(
        IterationResult(status=IterationStatus.TEARDOWN, deployment_id="TestStrategy:abc"),
        closure_evidence=_chain_says_closed(),
    )
    exit_code = _run(strategy, runner, monkeypatch)
    payload = _parse_last_json_object(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["summary"]["teardown_passed"] is True
    assert "open_positions_chain_disagreement" not in payload["steps"][0]


# ---------------------------------------------------------------------------
# 2. Unmeasured / partial chain reads never certify (Empty ≠ Zero)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("label", "evidence"),
    [
        ("no chain evidence at all", None),
        ("chain lane produced a non-dict", object()),
        ("closure UNPROVEN for some protocol", _chain_says_closed(closure_unknown=True, unproven_protocols=["gmx_v2"])),
        ("ratchet never armed (vacuous)", _chain_says_closed(protocols_to_prove=[], measured_closed_protocols=[])),
        ("no real pre-execution snapshot", _chain_says_closed(has_position_breakdown=False)),
        ("empty position breakdown", _chain_says_closed(positions_total=0, positions_closed=0)),
        ("only some positions closed", _chain_says_closed(positions_total=2, positions_closed=1)),
        ("verification never ran", _chain_says_closed(verification_status="not_run", positions_total=0)),
    ],
)
def test_unmeasured_or_partial_chain_read_never_overrides_the_cache(label, evidence, capsys, monkeypatch):
    """ALM-3109 acceptance 2. An ABSENCE of chain proof leaves the cache in charge.

    Every row here is a way the chain lane can fail to establish closure. None of
    them may certify: the pre-fix behaviour (cached residual fails the run) must
    survive intact, because the alternative is a teardown that stranded a real
    position reported as passed.
    """
    strategy = _make_strategy(open_positions=[_phantom_perp()])
    runner = _make_runner(
        IterationResult(status=IterationStatus.TEARDOWN, deployment_id="TestStrategy:abc"),
        closure_evidence=evidence,
    )
    exit_code = _run(strategy, runner, monkeypatch)
    payload = _parse_last_json_object(capsys.readouterr().out)

    assert exit_code == 1, label
    assert payload["summary"]["teardown_passed"] is False, label
    step = payload["steps"][0]
    assert step["open_positions_after_teardown"], label
    assert "open_positions_chain_disagreement" not in step, label


# ---------------------------------------------------------------------------
# 3. The dangerous direction: cache flat, chain says STILL OPEN
# ---------------------------------------------------------------------------


def test_chain_measured_open_fails_even_with_a_flat_cache(capsys, monkeypatch):
    """ALM-3109 acceptance 3, the severe direction.

    A KNOWN position the chain still reports OPEN makes TD-15 set
    ``all_closed=False``; ``_resolve_closure_refusal`` then sets
    ``success=False`` and ``map_teardown_result`` returns ``STRATEGY_ERROR``. The
    verdict fails on iteration status, and the chain-override disjunct is ANDed
    with that clause so it cannot rescue it. Both facts are asserted: the run
    fails, AND the evidence that reached the artifact is itself non-certifying.
    """
    strategy = _make_strategy(open_positions=[])  # cache believes flat
    runner = _make_runner(
        IterationResult(
            status=IterationStatus.STRATEGY_ERROR,
            error="Post-teardown verification failed: positions still open. Manual check required.",
            deployment_id="TestStrategy:abc",
        ),
        closure_evidence=_chain_says_closed(
            all_closed=False,
            verification_status="failed",
            positions_closed=0,
        ),
    )
    exit_code = _run(strategy, runner, monkeypatch)
    payload = _parse_last_json_object(capsys.readouterr().out)

    assert exit_code == 1
    assert payload["summary"]["teardown_passed"] is False
    assert _chain_certified_closure(payload["steps"][0]) is False


def test_chain_open_cannot_be_overridden_even_at_teardown_status():
    """Belt-and-braces on the pass criterion itself, independent of the lane.

    Should a future lane ever return TEARDOWN status alongside a chain report of
    residual risk, the criterion must still refuse — the override is gated on
    ``all_closed``, not merely on the presence of chain evidence.
    """
    step = {
        "status": IterationStatus.TEARDOWN.value,
        "open_positions_after_teardown": [{"position_id": "gmx-v2-ARB/USD-short", "value_usd": "406.35"}],
        "teardown_closure": _chain_says_closed(all_closed=False, verification_status="failed"),
    }
    assert _teardown_step_ok(step) is False


# ---------------------------------------------------------------------------
# 4. VIB-6285 is not weakened
# ---------------------------------------------------------------------------


def test_unmeasured_cache_read_still_fails_under_full_chain_certification(capsys, monkeypatch):
    """ALM-3109 acceptance 4. Chain certification does NOT excuse an unmeasured check.

    "The chain proved closure" and "the harness could not run its own check" are
    different facts. VIB-6285 exists because an unmeasured post-teardown read once
    yielded ``teardown_passed = True``; the override is ANDed OUTSIDE the
    ``open_positions_check`` clause so it can never rescue that case. Widening a
    guard while fixing its neighbour is how a regression ships as an improvement.
    """
    strategy = _make_strategy(error=RuntimeError("gateway gone"))
    runner = _make_runner(
        IterationResult(status=IterationStatus.TEARDOWN, deployment_id="TestStrategy:abc"),
        closure_evidence=_chain_says_closed(),
    )
    exit_code = _run(strategy, runner, monkeypatch)
    payload = _parse_last_json_object(capsys.readouterr().out)

    assert exit_code == 1
    assert payload["summary"]["teardown_passed"] is False
    step = payload["steps"][0]
    assert "unmeasured" in step["open_positions_check"]
    # Still never fabricated into a residual (the VIB-6198 false-failure class).
    assert "open_positions_after_teardown" not in step


# ---------------------------------------------------------------------------
# 5. The gate predicate, fail-closed on every malformed shape
# ---------------------------------------------------------------------------


class TestChainCertifiedClosureFailsClosed:
    """``_chain_certified_closure`` can only turn a FAIL into a PASS, so it must be
    impossible to satisfy by omission, by wrong type, or by truthiness."""

    def test_certifies_only_on_a_complete_positive_measurement(self):
        assert _chain_certified_closure({"teardown_closure": _chain_says_closed()}) is True

    @pytest.mark.parametrize(
        "step",
        [
            {},
            {"teardown_closure": None},
            {"teardown_closure": []},
            {"teardown_closure": "chain-confirmed CLOSED"},
            {"teardown_closure": {}},
        ],
        ids=["absent", "none", "list", "string", "empty-dict"],
    )
    def test_missing_or_mistyped_evidence_never_certifies(self, step):
        assert _chain_certified_closure(step) is False

    @pytest.mark.parametrize(
        "missing",
        ["all_closed", "closure_unknown", "has_position_breakdown", "positions_total", "positions_closed"],
    )
    def test_any_missing_key_never_certifies(self, missing):
        evidence = _chain_says_closed()
        del evidence[missing]
        assert _chain_certified_closure({"teardown_closure": evidence}) is False

    @pytest.mark.parametrize(
        ("field", "value"),
        [
            ("all_closed", 1),  # truthy but not the boolean True
            ("all_closed", "yes"),
            ("closure_unknown", None),  # falsey but not the boolean False
            ("has_position_breakdown", "true"),
            ("positions_total", "one"),
            ("positions_closed", None),
        ],
    )
    def test_truthy_or_mistyped_values_never_certify(self, field, value):
        """Deliberately stricter than truthiness: a JSON round-trip or a duck-typed
        producer that yields ``1`` instead of ``True`` must not be read as a
        measurement. ``is True`` / ``is False`` and an int() coercion are the
        contract."""
        assert _chain_certified_closure({"teardown_closure": _chain_says_closed(**{field: value})}) is False


# ---------------------------------------------------------------------------
# 6. Producer side — the evidence the runner lane stashes
# ---------------------------------------------------------------------------


class TestClosureChainEvidence:
    def test_serializes_the_derived_vib6285_signals(self):
        """``closure_unknown`` / ``unproven_protocols`` are derived properties on
        ``ClosureVerification``. Materializing them here is what stops a consumer
        re-deriving the VIB-6285 rule and getting it subtly wrong."""
        from almanak.framework.runner._teardown_helpers import closure_chain_evidence
        from almanak.framework.teardown.models import ClosureVerification, VerificationStatus

        verification = ClosureVerification(
            all_closed=True,
            positions_total=2,
            positions_closed=2,
            has_position_breakdown=True,
            verification_status=VerificationStatus.UNVERIFIED,
            protocols_to_prove=("aave_v3", "gmx_v2"),
            measured_closed_protocols=("gmx_v2",),
        )
        evidence = closure_chain_evidence(verification)

        assert evidence["closure_unknown"] is True
        assert evidence["unproven_protocols"] == ["aave_v3"]
        assert evidence["verification_status"] == "unverified"
        # JSON-serializable, because it rides the `strat test --json` artifact.
        assert json.loads(json.dumps(evidence)) == evidence
        # And an unproven protocol must never certify.
        assert _chain_certified_closure({"teardown_closure": evidence}) is False

    def test_a_fully_measured_closure_certifies(self):
        from almanak.framework.runner._teardown_helpers import closure_chain_evidence
        from almanak.framework.teardown.models import ClosureVerification, VerificationStatus

        evidence = closure_chain_evidence(
            ClosureVerification(
                all_closed=True,
                positions_total=1,
                positions_closed=1,
                has_position_breakdown=True,
                verification_status=VerificationStatus.UNVERIFIED,
                protocols_to_prove=("gmx_v2",),
                measured_closed_protocols=("gmx_v2",),
            )
        )
        assert evidence["closure_unknown"] is False
        assert _chain_certified_closure({"teardown_closure": evidence}) is True

    def test_default_constructed_verification_never_certifies(self):
        """A ``ClosureVerification`` built by a lane that never reached the chain
        verifier derives ``closure_unknown=False`` VACUOUSLY (both evidence tuples
        empty). ``protocols_to_prove`` being empty is what distinguishes a
        never-armed ratchet from a satisfied one."""
        from almanak.framework.runner._teardown_helpers import closure_chain_evidence
        from almanak.framework.teardown.models import ClosureVerification

        evidence = closure_chain_evidence(ClosureVerification(all_closed=True))
        assert evidence["closure_unknown"] is False
        assert _chain_certified_closure({"teardown_closure": evidence}) is False
