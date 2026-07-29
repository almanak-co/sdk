"""Tests for the preflight manifest-coverage gate (VIB-6018).

The gate's whole value rests on one judgement: **what counts as a "core" grant
versus infrastructure**. Get that wrong in either direction and the gate is
worthless in a way that looks fine:

* too strict (approve counted as core) → every triple passes, because
  ``_extract_token_permissions`` puts ERC-20 approves on essentially every
  manifest regardless of whether the protocol compiled at all. The gate then
  ratifies exactly the manifests it exists to catch.
* too loose (a router's approve entry dragging the router out of "core") → real
  coverage reads as a gap and the break set is inflated with noise.

So the classifier is unit-tested directly rather than only exercised through the
sweep, which is slow and network-bound.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from almanak.connectors.uniswap_v4.sdk import PERMIT2_ADDRESS  # noqa: E402
from almanak.framework.execution.signer.safe.constants import MULTISEND_ADDRESSES  # noqa: E402
from almanak.framework.intents.compiler import ERC20_APPROVE_SELECTOR  # noqa: E402
from almanak.framework.permissions.models import ContractPermission, FunctionPermission  # noqa: E402
from scripts.ci.check_permission_manifest_coverage import (  # noqa: E402
    BaselineFormatError,  # noqa: E402
    TripleResult,  # noqa: E402
    _declared_triples,
    _hosted_relevant,
    _load_accepted_gaps,
    classify_permissions,
    summarise,
)

_CHAIN = "arbitrum"
_MULTISEND = MULTISEND_ADDRESSES[_CHAIN]
_SWAP_SELECTOR = "0x04e45aaf"  # exactInputSingle


def _perm(target: str, *selectors: str) -> ContractPermission:
    return ContractPermission(
        target=target,
        label="test",
        function_selectors=[FunctionPermission(selector=s, label=s) for s in selectors],
    )


class TestClassifyPermissions:
    def test_multisend_is_infrastructure(self) -> None:
        core_addrs, infra = classify_permissions(_CHAIN, [_perm(_MULTISEND, "0x8d80ff0a")])
        assert (len(core_addrs), infra) == (0, 1)

    def test_multisend_matches_case_insensitively(self) -> None:
        """The manifest lower-cases targets; the constant is checksummed."""
        core_addrs, infra = classify_permissions(_CHAIN, [_perm(_MULTISEND.lower(), "0x8d80ff0a")])
        assert (len(core_addrs), infra) == (0, 1)

    def test_approve_only_target_is_infrastructure(self) -> None:
        """A bare ERC-20 approve authorises none of the protocol's own calls."""
        core_addrs, infra = classify_permissions(_CHAIN, [_perm("0xtoken", ERC20_APPROVE_SELECTOR)])
        assert (len(core_addrs), infra) == (0, 1)

    def test_router_with_approve_and_a_real_selector_is_core(self) -> None:
        """The approve rule is per-TARGET, not per-selector.

        A router that authorises both ``approve`` and ``exactInputSingle`` is
        genuine protocol coverage. Filtering approve out selector-by-selector
        would still classify it core here, but would also wrongly demote a
        router whose only compiled call happened to be an approve — the
        per-target rule is the one that matches "does this manifest authorise
        the protocol at all".
        """
        core_addrs, infra = classify_permissions(_CHAIN, [_perm("0xrouter", ERC20_APPROVE_SELECTOR, _SWAP_SELECTOR)])
        assert (len(core_addrs), infra) == (1, 0)

    def test_wildcard_target_with_no_selectors_is_core(self) -> None:
        """A clearance-1 target (empty selector list) authorises the whole contract."""
        core_addrs, infra = classify_permissions(_CHAIN, [_perm("0xdelegate")])
        assert (len(core_addrs), infra) == (1, 0)

    def test_infra_only_manifest_is_the_gap_shape(self) -> None:
        """MultiSend + config-derived approves = the exact shape VIB-6046 measured."""
        core_addrs, infra = classify_permissions(
            _CHAIN,
            [
                _perm(_MULTISEND, "0x8d80ff0a"),
                _perm("0xusdc", ERC20_APPROVE_SELECTOR),
                _perm("0xweth", ERC20_APPROVE_SELECTOR),
            ],
        )
        assert core_addrs == []
        assert infra == 3

    def test_empty_manifest_is_a_gap(self) -> None:
        assert classify_permissions(_CHAIN, []) == ([], 0)

    def test_permit2_is_infrastructure(self) -> None:
        """Permit2's selector is NOT the ERC-20 approve selector.

        ``approve(address,address,uint160,uint48)`` = ``0x87517c45``, so the
        approve rule alone classifies Permit2 as core. It authorises a *spender*,
        not a protocol action — a manifest of Permit2 + ERC-20 approves and no
        protocol contract authorises nothing the protocol needs, and would have
        read as covered. This is the false-negative the gate exists to prevent.
        """
        permit2_approve = "0x87517c45"
        assert permit2_approve != ERC20_APPROVE_SELECTOR
        core_addrs, infra = classify_permissions(_CHAIN, [_perm(PERMIT2_ADDRESS, permit2_approve)])
        assert (core_addrs, infra) == ([], 1)

    def test_permit2_only_manifest_is_a_gap(self) -> None:
        """The exact shape that would otherwise pass: Permit2 + approves, no protocol."""
        core_addrs, _ = classify_permissions(
            _CHAIN,
            [
                _perm(_MULTISEND, "0x8d80ff0a"),
                _perm(PERMIT2_ADDRESS, "0x87517c45"),
                _perm("0xusdc", ERC20_APPROVE_SELECTOR),
            ],
        )
        assert core_addrs == []

    def test_permit2_alongside_a_real_router_still_yields_core(self) -> None:
        """Regression guard the other way: uniswap_v4's real shape must still pass."""
        core_addrs, _ = classify_permissions(
            _CHAIN,
            [_perm(PERMIT2_ADDRESS, "0x87517c45"), _perm("0xuniversalrouter", "0x3593564c")],
        )
        assert core_addrs == ["0xuniversalrouter"]

    def test_weth_wrap_unwrap_target_counts_as_core(self) -> None:
        """Stated explicitly because it is a judgement, not a fact.

        A target authorised only for ``deposit()`` / ``withdraw(uint256)`` on the
        wrapped-native contract IS coverage — those are real protocol calls the
        strategy must make.
        """
        core_addrs, infra = classify_permissions(_CHAIN, [_perm("0xweth", "0xd0e30db0", "0x2e1a7d4d")])
        assert (core_addrs, infra) == (["0xweth"], 0)

    def test_chain_without_multisend_does_not_misclassify(self) -> None:
        """``MULTISEND_ADDRESSES.get`` misses → the empty-string guard must not
        match a permission whose target is also falsy."""
        core_addrs, infra = classify_permissions("solana", [_perm("SomeProgram1111", "0xdeadbeef")])
        assert (len(core_addrs), infra) == (1, 0)


class TestHostedRelevance:
    @pytest.mark.parametrize("chain", ["arbitrum", "base", "ethereum", "optimism", "polygon"])
    def test_evm_chains_with_safe_stack_are_hosted_relevant(self, chain: str) -> None:
        assert _hosted_relevant(chain) is True

    def test_solana_is_not_hosted_relevant(self) -> None:
        """Zodiac/Safe is EVM-only — enforcement cannot break a Solana triple."""
        assert _hosted_relevant("solana") is False

    def test_unknown_chain_is_not_hosted_relevant(self) -> None:
        assert _hosted_relevant("definitely_not_a_chain") is False


class TestTripleEnumeration:
    def test_universe_comes_from_connector_manifests(self) -> None:
        """Driving off the synthetic-intent membership sets instead would let a
        connector that fell out of them fall out of the inventory too — which is
        the VIB-5990 failure mode the inventory exists to detect."""
        triples = _declared_triples()
        assert triples
        keys = {t.key() for t in triples}
        # Curve declares SWAP/LP_OPEN/LP_CLOSE on 5 chains in its connector
        # manifest, so all three verbs must be enumerated even while curve's
        # permission_hints only declares SWAP.
        assert "curve:LP_OPEN:arbitrum" in keys
        assert "curve:LP_CLOSE:arbitrum" in keys

    def test_enumeration_does_not_depend_on_incidental_connector_imports(self) -> None:
        """The swept universe must not shrink with import order.

        A connector missing from the descriptor registry drops out of the sweep
        ENTIRELY, and an absent triple reads as "no gap here" rather than as an
        error — the same absence-as-success shape this gate has been bitten by
        repeatedly. ``_declared_triples`` therefore hydrates the registry
        explicitly rather than relying on this module's import chain to have
        pulled every connector in as a side effect.

        Measured 2026-07-29: hydration is currently a no-op (375 triples across
        41 connectors either way), so this pins the invariant, not a live fix.
        """
        from almanak.connectors._strategy_base.registry import _import_all_connectors

        before = {t.key() for t in _declared_triples()}
        _import_all_connectors()
        after = {t.key() for t in _declared_triples()}
        assert before == after, (
            "the declared universe grew after explicit registry hydration — the sweep was "
            f"silently under-enumerating by {len(after - before)} triples: {sorted(after - before)[:10]}"
        )

    def test_triples_are_deterministically_ordered(self) -> None:
        """Compare against ``sorted``, not against a second call.

        ``_declared_triples() == _declared_triples()`` passes whenever the
        registry merely returns a stable insertion order, so deleting the
        explicit sort would not fail it — it cannot detect the thing it names.
        The sweep's output ordering is what makes two runs diffable, so pin the
        actual ordering contract.
        """
        triples = _declared_triples()
        assert triples == sorted(triples, key=lambda t: (t.connector, t.chain, t.intent))


class TestSummarise:
    def _result(self, connector: str, intent: str, chain: str, *, core: int, hosted: bool) -> TripleResult:
        return TripleResult(
            connector=connector,
            intent=intent,
            chain=chain,
            hosted_relevant=hosted,
            core_targets=core,
            infra_targets=1,
            unwrappers=0,
            warnings=[],
        )

    def test_splits_hosted_from_unreachable(self) -> None:
        summary = summarise(
            [
                self._result("benqi", "SUPPLY", "avalanche", core=0, hosted=True),
                self._result("orca", "LP_OPEN", "solana", core=0, hosted=False),
                self._result("uniswap_v3", "SWAP", "arbitrum", core=2, hosted=True),
            ]
        )
        assert summary["totals"] == {
            "triples": 3,
            "gaps": 2,
            "hosted_relevant_gaps": 1,
            "declared_but_unreachable_gaps": 1,
            "silent_gaps": 2,
            # Counted separately from gaps: `is_gap` excludes both, so totals
            # derived from it alone read "not a gap" as "measured and covered".
            "unmeasured": 0,
            "generation_failed": 0,
        }
        assert summary["hosted_relevant_gap_keys"] == ["benqi:SUPPLY:avalanche"]
        assert "orca" in summary["declared_but_unreachable_gaps"]

    def test_silent_gap_count_excludes_warned_and_errored(self) -> None:
        """A gap that at least *said something* is a different (lesser) problem
        than one that returned a degraded manifest with ``warnings: []``.

        DELIBERATE SEMANTICS CHANGE (audit round 5): a result carrying an
        ``error`` is no longer a gap at all. It is a manifest-generation
        FAILURE, which is not a measurement of coverage in either direction, and
        it now lands in its own non-baselineable bucket. Counting it as a gap is
        what let the gate offer "add it to accepted_gaps" for a triple it never
        measured — a fail-open. So ``gaps`` here is 2, not 3.

        The warned-vs-silent distinction this test exists for is unchanged: a
        degraded manifest that produced no warning is still the worst case, and
        still the only one counted as silent.
        """
        warned = self._result("x", "SWAP", "arbitrum", core=0, hosted=True)
        warned.warnings = ["Compilation failed for x/SWAP"]
        errored = self._result("y", "SWAP", "arbitrum", core=0, hosted=True)
        errored.error = "ValueError: boom"
        silent = self._result("z", "SWAP", "arbitrum", core=0, hosted=True)
        summary = summarise([warned, errored, silent])
        assert summary["totals"]["gaps"] == 2, "a generation failure must not be counted as a coverage gap"
        assert errored.is_gap is False, "an errored result is a generation failure, not a gap"
        assert errored.generation_failed is True
        assert summary["totals"]["silent_gaps"] == 1


class TestAcceptedGapBaselineIsEnforced:
    """The baseline must not be able to park a gap anonymously.

    A bare list of keys records *that* someone silenced a triple, never *why* or
    under what ticket — which is how an exception list rots into an unreviewable
    allowlist (AGENTS.md §CI & Quality Guards). These tests pin the enforcement
    so the metadata requirement cannot be quietly dropped later.
    """

    def _write(self, tmp_path: Path, payload: object) -> Path:
        target = tmp_path / "baseline.json"
        target.write_text(json.dumps(payload))
        return target

    def test_valid_baseline_loads(self, tmp_path: Path) -> None:
        path = self._write(
            tmp_path,
            {"accepted_gaps": {"benqi:SUPPLY:avalanche": {"ticket": "VIB-6018", "reason": "empty hints"}}},
        )
        assert _load_accepted_gaps(path) == {"benqi:SUPPLY:avalanche"}

    def test_legacy_flat_list_is_rejected(self, tmp_path: Path) -> None:
        """The old format silently carried no reason — it must not keep working."""
        path = self._write(tmp_path, {"accepted_gap_keys": ["benqi:SUPPLY:avalanche"]})
        with pytest.raises(BaselineFormatError, match="legacy flat"):
            _load_accepted_gaps(path)

    @pytest.mark.parametrize(
        "meta",
        [
            {"ticket": "VIB-6018"},  # no reason
            {"reason": "because"},  # no ticket
            {"ticket": "", "reason": "because"},  # empty ticket
            {"ticket": "VIB-6018", "reason": "   "},  # whitespace-only reason
            "just a string",  # not an object
        ],
    )
    def test_entry_without_ticket_and_reason_is_rejected(self, tmp_path: Path, meta: object) -> None:
        path = self._write(tmp_path, {"accepted_gaps": {"benqi:SUPPLY:avalanche": meta}})
        with pytest.raises(BaselineFormatError):
            _load_accepted_gaps(path)

    def test_missing_baseline_fails_closed(self, tmp_path: Path) -> None:
        """ "Could not determine what is accepted" must never read as "nothing is new"."""
        with pytest.raises(BaselineFormatError, match="not found"):
            _load_accepted_gaps(tmp_path / "absent.json")

    def test_malformed_json_fails_closed(self, tmp_path: Path) -> None:
        target = tmp_path / "baseline.json"
        target.write_text("{not json")
        with pytest.raises(BaselineFormatError, match="invalid JSON"):
            _load_accepted_gaps(target)

    @pytest.mark.parametrize("payload", [["a:B:c"], "a string", 42, None])
    def test_non_object_baseline_fails_closed_not_with_a_traceback(self, tmp_path: Path, payload: object) -> None:
        """Valid JSON that is not an OBJECT must still be a policy failure.

        A list makes ``data.get`` raise AttributeError and a scalar makes ``in``
        raise TypeError. Neither is a ``BaselineFormatError``, so both escape
        ``_run``'s handler and reach the operator as a raw traceback — the
        fail-closed path losing its diagnostic exactly when it is needed.
        """
        target = self._write(tmp_path, payload)
        with pytest.raises(BaselineFormatError, match="top level"):
            _load_accepted_gaps(target)

    def test_shipped_baseline_satisfies_the_gate(self) -> None:
        """The real file must pass its own rule — otherwise CI is red on arrival."""
        shipped = _REPO_ROOT / "docs/internal/permissions/manifest-coverage-baseline.json"
        # LOADABILITY is the contract, not non-emptiness. The whole programme is
        # "the set must shrink monotonically", so an empty accepted set is the
        # GOAL STATE — asserting truthiness would turn the day the last gap is
        # closed into a red CI run for the wrong reason (CodeRabbit).
        assert isinstance(_load_accepted_gaps(shipped), set)

    def test_baseline_outside_repo_still_prints_the_policy_failure(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """`Path.relative_to` raises for a baseline outside the repo.

        Raising inside the f-string on the fail-closed path would replace the
        policy failure with a bare ValueError traceback — the operator would
        read a tool bug instead of "the gate could not determine what is
        accepted". Fail-closed must stay legible.
        """
        import scripts.ci.check_permission_manifest_coverage as gate

        outside = tmp_path / "outside-the-repo.json"
        outside.write_text('{"accepted_gap_keys": ["a:B:c"]}')
        # ``--connector lido`` keeps this a UNIT test. Without it, ``main()``
        # runs the full 375-triple compile sweep before reaching the assertion —
        # measured at 6m33s and hundreds of live public-RPC calls, inside a
        # suite AGENTS.md documents as "no chain". The baseline check happens
        # after the sweep either way, so scoping it costs no coverage.
        rc = gate.main(["--connector", "lido", "--baseline", str(outside)])
        out = capsys.readouterr().out
        assert rc == 1
        assert "FAIL" in out and "legacy flat" in out

    def test_connector_filtered_run_does_not_report_unrelated_entries_as_stale(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """`--connector` must not condemn baseline entries it never measured.

        `stale = accepted - measured` with an unscoped `accepted` reports every
        UNSELECTED baseline key as "no longer a gap" and instructs deleting it.
        Following that instruction strips the record that those connectors
        revert unauthorized under Safe once VIB-6057 enforcement is registered —
        the gate destroying the artifact it exists to protect.
        """
        import scripts.ci.check_permission_manifest_coverage as gate

        baseline = tmp_path / "baseline.json"
        baseline.write_text(
            json.dumps(
                {
                    "accepted_gaps": {
                        "lido:STAKE:ethereum": {"ticket": "VIB-6018", "reason": "empty hints"},
                        "lido:UNSTAKE:ethereum": {"ticket": "VIB-6018", "reason": "empty hints"},
                        # A different connector, deliberately NOT swept below.
                        "benqi:SUPPLY:avalanche": {"ticket": "VIB-6018", "reason": "empty hints"},
                    }
                }
            )
        )
        rc = gate.main(["--connector", "lido", "--baseline", str(baseline)])
        out = capsys.readouterr().out
        # rc and a positive marker both matter: if the connector filter ever
        # matched nothing, main() returns 2 and prints to STDERR, so an
        # out-only assertion would pass while testing nothing.
        assert rc == 0, f"expected a clean pass, got {rc}:\n{out}"
        # Positive proof that lido really was swept (the report prints rows as
        # columns, not as colon-joined keys). Without this, a filter that
        # matched nothing would return 2 and print to stderr, leaving the
        # negative assertion below trivially true.
        assert "declared triples ................ 2" in out, f"lido was not measured:\n{out}"
        assert "lido" in out and "STAKE" in out, "lido rows missing — the assertion below is vacuous"
        assert "benqi:SUPPLY:avalanche" not in out, (
            "a --connector lido run condemned an unmeasured benqi entry as stale"
        )

    def test_stale_entry_is_fatal(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A stale entry is a pre-authorised regression, so it must FAIL.

        Pins the exit-code contract: without this, reverting `stale` to
        non-fatal (or dropping it from the `if new_gaps or stale` guard) is
        green.

        The sweep is stubbed rather than run. Driving this through the real
        compiler made it network-bound *and* let it pass for the wrong reason:
        a 429 on ``traderjoe_v2:SWAP:arbitrum`` made the triple indeterminate,
        which under the old ``accepted - measured`` subtraction ALSO produced a
        "no longer gaps" line. The assertion could therefore be satisfied by the
        very defect the sibling test below now pins, so it proved nothing about
        genuine staleness.
        """
        import scripts.ci.check_permission_manifest_coverage as gate

        triple = gate.Triple(connector="traderjoe_v2", intent="SWAP", chain="arbitrum")
        monkeypatch.setattr(gate, "_declared_triples", lambda: [triple])
        # Genuinely covered: real core grants, clean measurement.
        monkeypatch.setattr(
            gate,
            "evaluate",
            lambda t: _stub_result(t, core_targets=3, core_target_addresses=["0xaa", "0xbb", "0xcc"]),
        )

        baseline = tmp_path / "baseline.json"
        baseline.write_text(
            json.dumps(
                {
                    "accepted_gaps": {
                        "traderjoe_v2:SWAP:arbitrum": {"ticket": "VIB-6018", "reason": "not actually a gap"}
                    }
                }
            )
        )
        rc = gate.main(["--baseline", str(baseline)])
        out = capsys.readouterr().out
        assert rc == 1, "a stale accepted-gap entry must fail the gate"
        assert "no longer gaps" in out

    def test_unmeasured_accepted_entry_is_never_reported_stale(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A transient 429 must NOT produce "delete this from accepted_gaps".

        ``is_gap`` is ``core_targets == 0 and not indeterminate``, so a
        transport failure removes the triple from the measured gap set WITHOUT
        it being covered. Subtracting (``accepted - measured``) then reads that
        absence as "no longer a gap" and instructs deletion of the baseline
        entry — on evidence that was never gathered, and in direct contradiction
        of the UNMEASURED block the same run prints a few lines earlier.

        Deleting that entry removes the record that the triple reverts
        unauthorized under Safe once VIB-6057 enforcement is registered: the
        gate destroying the artifact it exists to protect, triggered by nothing
        more than a rate limit.
        """
        import scripts.ci.check_permission_manifest_coverage as gate

        triple = gate.Triple(connector="benqi", intent="SUPPLY", chain="avalanche")
        monkeypatch.setattr(gate, "_declared_triples", lambda: [triple])
        monkeypatch.setattr(gate, "evaluate", lambda t: _stub_result(t, core_targets=0, error="429 Too Many Requests"))

        baseline = tmp_path / "baseline.json"
        baseline.write_text(
            json.dumps({"accepted_gaps": {triple.key(): {"ticket": "VIB-6018", "reason": "empty hints"}}})
        )
        rc = gate.main(["--baseline", str(baseline)])
        out = capsys.readouterr().out

        assert "could NOT be measured" in out, f"the unmeasured triple was not reported:\n{out}"
        assert "no longer gaps" not in out, (
            "a rate-limited triple was condemned as stale — following the printed instruction "
            f"would delete a still-required baseline entry:\n{out}"
        )
        # Unmeasured is still fatal: this is a fix, not a silencer. Making the
        # contradiction disappear by downgrading UNMEASURED would also satisfy
        # the negative assertion above, so pin the exit code too.
        assert rc == 1, f"an unmeasured hosted-relevant triple must still fail the gate, got {rc}:\n{out}"

    def test_orphaned_accepted_entry_is_reported(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An accepted key whose triple is no longer declared must still surface.

        Guards the narrowing introduced alongside the stale fix. Deriving
        ``stale`` from POSITIVE evidence of coverage means a key absent from
        ``results`` entirely can never satisfy it — correct for the unmeasured
        case, but it would also let an entry for a dropped intent/chain linger
        silently AND keep counting toward the exposure figure the PASS line
        prints, overstating the break set with triples that no longer exist.

        Orphans get their own bucket because their remedy genuinely differs: an
        unmeasured entry must NOT be deleted, an orphan safely can be — what it
        pre-authorised is gone.
        """
        import scripts.ci.check_permission_manifest_coverage as gate

        live = gate.Triple(connector="benqi", intent="SUPPLY", chain="avalanche")
        monkeypatch.setattr(gate, "_declared_triples", lambda: [live])
        monkeypatch.setattr(
            gate, "evaluate", lambda t: _stub_result(t, core_targets=0, warnings=["Compilation failed: no route"])
        )

        baseline = tmp_path / "baseline.json"
        baseline.write_text(
            json.dumps(
                {
                    "accepted_gaps": {
                        live.key(): {"ticket": "VIB-6018", "reason": "real gap"},
                        # Same connector (so it is in scope) but an intent no
                        # manifest declares any more.
                        "benqi:BORROW:avalanche": {"ticket": "VIB-6018", "reason": "orphan"},
                    }
                }
            )
        )
        rc = gate.main(["--baseline", str(baseline)])
        out = capsys.readouterr().out

        assert rc == 1, f"an orphaned accepted-gap entry must fail the gate, got {rc}:\n{out}"
        assert "no longer declared" in out, f"the orphan was not reported:\n{out}"
        # Isolate the orphan block so the assertions below cannot be satisfied
        # by the key appearing in some other section of the report.
        orphan_block = out.split("no longer declared:", 1)[1].split("\n\n", 1)[0]
        assert "benqi:BORROW:avalanche" in orphan_block, f"the orphan key was not named:\n{orphan_block}"
        assert "benqi:SUPPLY:avalanche" not in orphan_block, (
            f"the live accepted entry was misreported as an orphan:\n{orphan_block}"
        )

    def test_report_does_not_claim_an_empty_break_set_when_nothing_was_measured(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``(none)`` under the break-set heading asserts the set is EMPTY.

        `summarise` derives its totals from ``is_gap``, which excludes both
        unmeasured and failed. So a sweep where every triple raised reported
        ZERO gaps and printed "(none)" under "reverts under Safe the moment
        enforcement turns on" — an empty gap list means "we learned nothing",
        not "nothing is broken", and this is the line a reviewer reads to decide
        whether enforcement is safe to switch on.

        Fourth surface with this shape (stale detection, the progress line, the
        report, now the totals). Every one has been a claim made far from its
        discriminator, so ``totals`` now carries the two counters directly.
        """
        import almanak.framework.permissions.generator as generator
        import scripts.ci.check_permission_manifest_coverage as gate

        triple = gate.Triple(connector="uniswap_v3", intent="SWAP", chain="ethereum")
        monkeypatch.setattr(gate, "_declared_triples", lambda: [triple])

        def boom(**_kwargs: object) -> object:
            raise RuntimeError("compiler blew up")

        monkeypatch.setattr(generator, "generate_manifest", boom)

        baseline = tmp_path / "baseline.json"
        baseline.write_text(json.dumps({"accepted_gaps": {}}))
        gate.main(["--baseline", str(baseline)])
        out = capsys.readouterr().out

        break_set = out.split("-- BREAK SET", 1)[1]
        assert "(none)" not in break_set.split("\n\n", 1)[0], (
            f"the report claimed an empty break set when nothing was measured:\n{break_set}"
        )
        assert "nothing measured" in break_set, f"the report did not say the sweep measured nothing:\n{break_set}"
        assert "generation FAILED .............. 1" in out, f"totals do not count the failure:\n{out}"

    def test_summarise_counts_unmeasured_and_failed_separately_from_gaps(self) -> None:
        """``totals`` must carry the two states ``is_gap`` excludes."""
        rows = [
            _stub_result(TripleResult("a", "SWAP", "arbitrum", True, 0, 1, 0, []), core_targets=0),
            _stub_result(
                TripleResult("b", "SWAP", "arbitrum", True, 0, 1, 0, []),
                core_targets=0,
                error="429 Too Many Requests",
            ),
            _stub_result(
                TripleResult("c", "SWAP", "arbitrum", True, 0, 1, 0, []),
                core_targets=0,
                error="RuntimeError: boom",
            ),
        ]
        totals = summarise(rows)["totals"]
        assert totals["gaps"] == 1, "only the genuinely measured zero is a gap"
        assert totals["unmeasured"] == 1
        assert totals["generation_failed"] == 1

    def test_progress_line_does_not_claim_ok_for_a_generation_failure(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A triple whose generation RAISED must not render as ``ok``.

        ``is_gap`` is deliberately narrow — it excludes both unmeasured and
        failed — so any flag derived by falling through to ``else`` inherits
        that narrowness and claims success for a triple that produced no
        manifest at all. The gate does fail later, but the progress line is the
        only feedback during a multi-minute sweep, so an operator watching it
        would see ``ok`` for a connector that is completely broken.

        Third surface in this gate where narrowing ``is_gap`` leaked a false
        success claim; hence the flags are enumerated explicitly rather than
        implied by the absence of a gap.
        """
        import almanak.framework.permissions.generator as generator
        import scripts.ci.check_permission_manifest_coverage as gate

        triple = gate.Triple(connector="uniswap_v3", intent="SWAP", chain="ethereum")
        monkeypatch.setattr(gate, "_declared_triples", lambda: [triple])

        def boom(**_kwargs: object) -> object:
            raise RuntimeError("compiler blew up")

        monkeypatch.setattr(generator, "generate_manifest", boom)

        baseline = tmp_path / "baseline.json"
        baseline.write_text(json.dumps({"accepted_gaps": {}}))
        gate.main(["--baseline", str(baseline)])
        progress = capsys.readouterr().err

        line = next((ln for ln in progress.splitlines() if triple.key() in ln), None)
        assert line is not None, f"the progress line was not emitted:\n{progress}"
        flag = line.split("]", 1)[1].strip().split(None, 1)[0]
        assert flag != "ok", f"a generation failure was rendered as 'ok':\n{line}"
        assert flag == "ERR", f"a generation failure should carry the error flag, got {flag!r}:\n{line}"

    def test_orphan_detection_catches_a_deleted_connector_on_a_full_run(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A connector removed outright must not hide its baseline entries.

        ``accepted_in_scope`` filters by the connectors this run SWEPT — correct
        for staleness, wrong for orphans. A deleted or renamed connector
        contributes no triples, so its name never enters ``measured_connectors``
        and every one of its entries would drop out before orphan detection and
        linger indefinitely, which is the opposite of the monotonic-shrink rule
        the baseline is supposed to enforce.

        A full run therefore judges the whole accepted set; only ``--connector``
        narrows, because that run genuinely cannot know what it skipped.
        """
        import scripts.ci.check_permission_manifest_coverage as gate

        live = gate.Triple(connector="benqi", intent="SUPPLY", chain="avalanche")
        monkeypatch.setattr(gate, "_declared_triples", lambda: [live])
        monkeypatch.setattr(
            gate, "evaluate", lambda t: _stub_result(t, core_targets=0, warnings=["Compilation failed"])
        )

        baseline = tmp_path / "baseline.json"
        baseline.write_text(
            json.dumps(
                {
                    "accepted_gaps": {
                        live.key(): {"ticket": "VIB-6018", "reason": "real gap"},
                        # A connector that no longer exists at all — no triples,
                        # so it is absent from measured_connectors.
                        "deletedproto:SWAP:ethereum": {"ticket": "VIB-6018", "reason": "connector removed"},
                    }
                }
            )
        )
        rc = gate.main(["--baseline", str(baseline)])
        out = capsys.readouterr().out

        assert rc == 1, f"a baseline entry for a removed connector must fail the gate, got {rc}:\n{out}"
        assert "no longer declared" in out, f"the removed connector's entry was not reported:\n{out}"
        assert "deletedproto:SWAP:ethereum" in out, f"the orphan key was not named:\n{out}"

    def test_connector_filtered_run_does_not_condemn_unswept_entries_as_orphans(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The full-run widening must not leak into a ``--connector`` run.

        A filtered run cannot know what the connectors it skipped still declare,
        so judging the whole accepted set there would resurrect exactly the
        destructive instruction the connector-scoping was added to prevent.
        """
        import scripts.ci.check_permission_manifest_coverage as gate

        lido = gate.Triple(connector="lido", intent="STAKE", chain="ethereum")
        other = gate.Triple(connector="benqi", intent="SUPPLY", chain="avalanche")
        monkeypatch.setattr(gate, "_declared_triples", lambda: [lido, other])
        monkeypatch.setattr(gate, "evaluate", lambda t: _stub_result(t, core_targets=0, warnings=["no route"]))

        baseline = tmp_path / "baseline.json"
        baseline.write_text(
            json.dumps(
                {
                    "accepted_gaps": {
                        lido.key(): {"ticket": "VIB-6018", "reason": "real gap"},
                        other.key(): {"ticket": "VIB-6018", "reason": "real gap, not swept here"},
                    }
                }
            )
        )
        rc = gate.main(["--connector", "lido", "--baseline", str(baseline)])
        out = capsys.readouterr().out

        # Pin the exit code and the report shape directly. The previous form
        # (`key not in out.split(...)` with an `or`) could be satisfied without
        # the orphan branch running at all, so it would have stayed green if the
        # branch were deleted (CodeRabbit).
        assert rc == 0, f"a filtered run with only real, declared entries must pass, got {rc}:\n{out}"
        assert "no longer declared" not in out, (
            f"a --connector lido run condemned an unswept benqi entry as an orphan:\n{out}"
        )

    def test_broken_permission_hints_is_never_offered_as_an_acceptable_gap(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A broken connector must not become "add these to accepted_gaps".

        VIB-6018 makes an unusable ``permission_hints`` export RAISE. But
        ``_derive_membership_sets`` folds ``get_permission_hints`` over EVERY
        slug, so one gutted export propagates out of ``build_synthetic_intents``
        and fails *every* triple in the sweep — healthy connectors included.

        ``evaluate``'s bare ``except Exception`` then recorded each as
        ``core_targets=0`` ⇒ ``is_gap`` ⇒ a new gap, whose printed remedy is
        "add the key under 'accepted_gaps'". Following that for a sweep-wide
        failure permanently pre-authorises hundreds of *working* triples to hold
        zero core grants.

        That is a FAIL-OPEN, and strictly worse than the sibling false-red: a
        deleted baseline entry re-fails on the next run, an accepted one does
        not. A broken connector is a defect to fix, never a coverage result.
        """
        import almanak.framework.permissions.generator as generator
        import scripts.ci.check_permission_manifest_coverage as gate
        from almanak.framework.permissions.hints import PermissionHintsError

        healthy = gate.Triple(connector="uniswap_v3", intent="SWAP", chain="ethereum")
        monkeypatch.setattr(gate, "_declared_triples", lambda: [healthy])

        def boom(**_kwargs: object) -> object:
            raise PermissionHintsError("benqi permission_hints exports no usable PERMISSION_HINTS")

        # Patch the generator, NOT ``evaluate`` — the classification under test
        # lives in ``evaluate``'s except branch, so stubbing ``evaluate`` would
        # skip the code this test exists to pin. One gutted export raises for
        # EVERY triple, which is the blast radius being reproduced.
        monkeypatch.setattr(generator, "generate_manifest", boom)

        baseline = tmp_path / "baseline.json"
        baseline.write_text(json.dumps({"accepted_gaps": {}}))
        rc = gate.main(["--baseline", str(baseline)])
        out = capsys.readouterr().out

        assert rc == 1, "a broken permission_hints export must fail the gate"
        assert "manifest generation raised" in out, f"the failure was not reported as such:\n{out}"
        assert "do NOT add them to 'accepted_gaps'" in out, f"missing the do-not-baseline remedy:\n{out}"
        assert "ZERO core grants and no accepted-gap entry" not in out, (
            "a healthy triple was reported as a coverage gap because an unrelated connector's "
            f"hints are broken — following that remedy pre-authorises it to zero grants:\n{out}"
        )
        # Also assert on the counted total, not just the printed block. The
        # BREAK SET section renders space-padded columns rather than colon-joined
        # keys, so a key-substring check cannot see a triple listed there.
        assert "hosted-relevant (BREAK SET) .. 0" in out, (
            f"a generation failure was counted into the hosted-relevant break set:\n{out}"
        )

    @pytest.mark.parametrize(
        ("exc", "label"),
        [
            (ImportError("No module named 'some_dep'", name="some_dep"), "nested import inside a real hints module"),
            (RuntimeError("compiler blew up"), "an unclassifiable generation error"),
            (ValueError("connector declared unknown synthetic_discovery_intents"), "a membership-derivation error"),
        ],
    )
    def test_no_generation_exception_is_ever_baselineable(
        self,
        exc: Exception,
        label: str,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """ANY exception from generation is non-baselineable, not just known ones.

        The first version of this guard enumerated the failures that counted —
        ``PermissionHintsError`` plus ``ImportError`` whose ``name`` mentions
        ``permission_hints``. A *nested* import failure inside a real hints
        module is the exact VIB-6018 shape and carries ``exc.name == "some_dep"``,
        so it slipped through, was reported as a plain gap, and the printed
        remedy invited baselining it — a fail-open on the very failure the PR
        introduced.

        The invariant is therefore keyed on "generation raised at all", not on
        classifying which exception. Zero targets because generation blew up is
        not a measurement of zero coverage.
        """
        import almanak.framework.permissions.generator as generator
        import scripts.ci.check_permission_manifest_coverage as gate

        healthy = gate.Triple(connector="uniswap_v3", intent="SWAP", chain="ethereum")
        monkeypatch.setattr(gate, "_declared_triples", lambda: [healthy])

        def boom(**_kwargs: object) -> object:
            raise exc

        monkeypatch.setattr(generator, "generate_manifest", boom)

        baseline = tmp_path / "baseline.json"
        baseline.write_text(json.dumps({"accepted_gaps": {}}))
        rc = gate.main(["--baseline", str(baseline)])
        out = capsys.readouterr().out

        assert rc == 1, f"{label}: must fail the gate, got {rc}:\n{out}"
        assert "manifest generation raised" in out, f"{label}: not reported as a generation failure:\n{out}"
        assert "ZERO core grants and no accepted-gap entry" not in out, (
            f"{label}: offered as a baselineable gap — accepting it would permanently "
            f"pre-authorise zero core grants for a triple that was never measured:\n{out}"
        )

    def test_progress_line_does_not_claim_ok_for_an_unmeasured_triple(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``ok`` asserts "measured, and covered" — an indeterminate row is neither.

        Same root as the stale scoping: ``indeterminate`` must be consulted
        wherever a coverage claim is made, and the live progress line is the
        surface an operator actually watches during a 6-minute sweep.
        """
        import scripts.ci.check_permission_manifest_coverage as gate

        triple = gate.Triple(connector="benqi", intent="SUPPLY", chain="avalanche")
        monkeypatch.setattr(gate, "_declared_triples", lambda: [triple])
        monkeypatch.setattr(gate, "evaluate", lambda t: _stub_result(t, core_targets=0, error="429 Too Many Requests"))

        baseline = tmp_path / "baseline.json"
        baseline.write_text(
            json.dumps({"accepted_gaps": {triple.key(): {"ticket": "VIB-6018", "reason": "empty hints"}}})
        )
        gate.main(["--baseline", str(baseline)])
        progress = capsys.readouterr().err

        # Assert on the FLAG FIELD of this triple's line, not on the whole
        # stream: a bare `"ok" not in progress` is a substring match, and "ok"
        # occurs inside ordinary words like "token" — a future connector slug or
        # a stray warning would make it a spurious red.
        line = next((ln for ln in progress.splitlines() if triple.key() in ln), None)
        assert line is not None, f"the progress line was not emitted:\n{progress}"
        flag = line.split("]", 1)[1].strip().split(None, 1)[0]
        assert flag != "ok", f"an unmeasured triple was rendered as 'ok':\n{line}"
        assert flag == "????", f"an unmeasured triple should carry the unknown flag, got {flag!r}:\n{line}"


def _stub_result(
    triple: object,
    *,
    core_targets: int,
    error: str | None = None,
    warnings: list[str] | None = None,
    core_target_addresses: list[str] | None = None,
) -> TripleResult:
    """A deterministic ``TripleResult`` for tests that drive ``main()``.

    Tests that exercise gate POLICY (stale detection, exit codes, remediation
    text) must not depend on the live compiler: it is RPC-bound, so a public
    endpoint 429 silently changes which branch the assertion lands on. Stubbing
    ``evaluate`` pins the same contracts with no network — and, as the stale
    tests showed, stops a policy test from passing for the wrong reason.
    """
    return TripleResult(
        connector=triple.connector,  # type: ignore[attr-defined]
        intent=triple.intent,  # type: ignore[attr-defined]
        chain=triple.chain,  # type: ignore[attr-defined]
        hosted_relevant=True,
        core_targets=core_targets,
        infra_targets=1,
        unwrappers=0,
        warnings=warnings or [],
        core_target_addresses=core_target_addresses or [],
        error=error,
    )


class TestTransportFailuresAreNotGaps:
    """A 429 removes grants from a manifest, so it looks exactly like a gap.

    Folding it into ``is_gap`` makes the gate offer its "add this to
    accepted_gaps" remedy for a triple that is actually covered — permanently
    pre-authorising a real money-path grant to disappear. Observed live: two
    full sweeps minutes apart returned 82 and 80 hosted gaps, the two extras
    both carrying an explicit 429.
    """

    def _result(self, *, core: int, warnings: list[str] | None = None, error: str | None = None) -> TripleResult:
        return TripleResult(
            connector="uniswap_v4",
            intent="SWAP",
            chain="arbitrum",
            hosted_relevant=True,
            core_targets=core,
            infra_targets=1,
            unwrappers=0,
            warnings=warnings or [],
            error=error,
        )

    @pytest.mark.parametrize(
        "marker",
        [
            "429 Client Error: Too Many Requests for url: https://arbitrum-one-rpc.publicnode.com",
            "Rate limit exceeded",
            "Read timed out",
            "Connection aborted",
        ],
    )
    def test_transport_shaped_failure_is_indeterminate_not_a_gap(self, marker: str) -> None:
        result = self._result(core=0, warnings=[marker])
        assert result.indeterminate is True
        assert result.is_gap is False, "a transport failure must never be reported as a structural gap"

    def test_transport_marker_in_error_field_also_counts(self) -> None:
        result = self._result(core=0, error="HTTPError: 429 Too Many Requests")
        assert result.indeterminate is True
        assert result.is_gap is False

    def test_a_genuine_zero_core_result_is_still_a_gap(self) -> None:
        """The guard must not swallow real gaps — those have no transport marker."""
        result = self._result(core=0, warnings=["Compilation failed: protocol not supported on this chain"])
        assert result.indeterminate is False
        assert result.is_gap is True

    def test_a_covered_triple_is_neither(self) -> None:
        result = self._result(core=1)
        assert result.indeterminate is False
        assert result.is_gap is False


class TestMainRestoresGlobalLoggingState:
    """``logging.disable`` is process-global and does not self-restore.

    Three tests in this file call ``main()`` in-process, so a leak silences
    every later test in the same xdist worker that uses a bare ``caplog``
    (no ``set_level``/``at_level``) — an intermittent red in an unrelated file.
    """

    def test_disable_level_is_restored(self) -> None:
        import logging as _logging

        import scripts.ci.check_permission_manifest_coverage as gate

        before = _logging.root.manager.disable
        gate.main(["--connector", "lido", "--no-gate"])
        assert _logging.root.manager.disable == before, (
            "main() leaked logging.disable into the process; bare-caplog tests in this "
            "worker would silently stop seeing records"
        )
