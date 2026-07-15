"""Chain-aware consolidation-target resolution (VIB-5727).

Background: ``teardown request`` defaulted ``target_token`` to the literal
``"USDC"`` and persisted it. On a chain with no USDC (robinhood 4663 registers
USDG/USDe/WETH) the consolidation swap could not compile — ``Unknown token:
USDC`` — so a teardown whose risk-reducing phase fully succeeded still reported
``failed``, and the residual was never swept.

The centrepiece here is :class:`TestBackwardCompatibilityMatrix`, which asserts
the property the fix is only safe because of: **every chain that resolved USDC
before still resolves USDC**. It is table-driven over the live ChainRegistry
rather than a spot-check, so a future chain/registry change that would silently
re-target a working chain fails here instead of on-chain.
"""

import pytest

from almanak.core.chains import ChainRegistry
from almanak.framework.data.tokens.chain_stable import token_resolves_on_chain
from almanak.framework.teardown.consolidation import (
    resolve_chain_target_token,
    resolve_consolidation_targets,
)
from almanak.framework.teardown.models import (
    TARGET_TOKEN_CHAIN_DEFAULT,
    TeardownAssetPolicy,
    TeardownRequest,
    TeardownMode,
)

ALL_CHAINS = sorted(ChainRegistry.names())

# Chains with no resolvable USDC, and the target each must fall back to.
# Locked as an explicit table (not derived) so that a registry change which
# adds/removes a USDC has to be acknowledged here rather than silently drifting.
EXPECTED_FALLBACKS = {
    "robinhood": "USDG",  # declared canonical_stable; USDe is unroutable (VIB-5729)
    "plasma": "USDT0",  # only registered stable ("Fluid USDT Zero" is a wrapper)
    "zerog": "USDC.E",  # bridged USDC — a real dollar, NOT wrapped native
    "blast": "WETH",  # no registered stable at all → wrapped native
}


class TestBackwardCompatibilityMatrix:
    """THE regression guard. Do not weaken to a spot-check."""

    @pytest.mark.parametrize("chain", ALL_CHAINS)
    def test_usdc_chains_are_untouched(self, chain):
        """Any chain that resolves USDC keeps USDC, byte-for-byte."""
        if not token_resolves_on_chain("USDC", chain):
            pytest.skip(f"{chain} has no USDC — covered by the fallback table")
        target, warnings = resolve_chain_target_token(TARGET_TOKEN_CHAIN_DEFAULT, chain)
        assert target == "USDC", f"{chain} regressed from USDC to {target!r}"
        assert warnings == [], f"{chain} resolves USDC but emitted warnings: {warnings}"

    def test_every_chain_without_usdc_is_in_the_table(self):
        """No USDC-less chain may be silently unhandled."""
        missing = {c for c in ALL_CHAINS if not token_resolves_on_chain("USDC", c)}
        assert missing == set(EXPECTED_FALLBACKS), (
            "the set of USDC-less chains changed; update EXPECTED_FALLBACKS deliberately "
            f"(unexpected: {missing - set(EXPECTED_FALLBACKS)}, "
            f"stale: {set(EXPECTED_FALLBACKS) - missing})"
        )

    @pytest.mark.parametrize(("chain", "expected"), sorted(EXPECTED_FALLBACKS.items()))
    def test_usdc_less_chains_resolve_a_usable_target(self, chain, expected):
        target, warnings = resolve_chain_target_token(TARGET_TOKEN_CHAIN_DEFAULT, chain)
        assert target == expected
        assert token_resolves_on_chain(target, chain), f"{chain}: {target!r} does not resolve"
        assert warnings, "a substituted target must be surfaced to the operator, not silent"

    def test_robinhood_is_the_ticket_case(self):
        """VIB-5727 end to end: the default no longer picks an absent token."""
        assert not token_resolves_on_chain("USDC", "robinhood")
        target, warnings = resolve_chain_target_token(TARGET_TOKEN_CHAIN_DEFAULT, "robinhood")
        assert target == "USDG"
        assert any("USDC is not registered on robinhood" in w for w in warnings)


class TestExplicitOperatorInstruction:
    """An explicit -t is honoured or refused — never silently substituted."""

    def test_explicit_resolvable_target_is_honoured(self):
        assert resolve_chain_target_token("USDG", "robinhood")[0] == "USDG"
        assert resolve_chain_target_token("USDT", "arbitrum")[0] == "USDT"

    def test_explicit_unresolvable_target_skips_rather_than_substitutes(self):
        """The 'never guess a trade' contract.

        Substituting USDG for an operator who explicitly asked for USDC would
        route real money into an asset they did not choose.
        """
        target, warnings = resolve_chain_target_token("USDC", "robinhood")
        assert target is None
        assert any("not a registered token on robinhood" in w for w in warnings)
        assert any("skipping token consolidation" in w for w in warnings)

    def test_explicit_usdc_on_a_usdc_chain_is_indistinguishable_from_default(self):
        assert resolve_chain_target_token("USDC", "ethereum")[0] == "USDC"
        assert resolve_chain_target_token(TARGET_TOKEN_CHAIN_DEFAULT, "ethereum")[0] == "USDC"

    def test_wrapped_native_is_a_valid_explicit_target(self):
        assert resolve_chain_target_token("WETH", "robinhood")[0] == "WETH"


class TestTickerCollision:
    """USDG is two different tokens on two different chains."""

    def test_robinhood_usdg_is_global_dollar_not_xlayer_gravity_usd(self):
        from almanak.framework.data.tokens import get_token_resolver

        resolver = get_token_resolver()
        robinhood = resolver.resolve("USDG", "robinhood", skip_gateway=True)
        assert robinhood.address.lower() == "0x5fc5360d0400a0fd4f2af552add042d716f1d168"
        assert robinhood.decimals == 6

        xlayer = resolver.resolve("USDG", "xlayer", skip_gateway=True)
        assert xlayer.address.lower() == "0x4ae46a509f6b1d9056937ba4500cb143933d2dc8"
        assert robinhood.address.lower() != xlayer.address.lower()

    def test_xlayer_is_unaffected_by_robinhood_declaration(self):
        """xlayer has USDC and must not inherit robinhood's USDG."""
        assert resolve_chain_target_token(TARGET_TOKEN_CHAIN_DEFAULT, "xlayer")[0] == "USDC"


class TestLegacyPersistedRows:
    """Rows written before VIB-5727 carry a literal "USDC"."""

    def test_legacy_usdc_row_takes_the_explicit_path(self):
        """A pre-fix row means "USDC" and must behave exactly as it always did."""
        for chain in ("ethereum", "arbitrum", "base"):
            assert resolve_chain_target_token("USDC", chain)[0] == "USDC"

    def test_legacy_usdc_row_on_a_usdc_less_chain_skips_loudly(self):
        """The one honest degradation: a legacy row on robinhood cannot be
        distinguished from an explicit USDC request, so it is refused with a
        warning rather than silently re-pointed."""
        target, warnings = resolve_chain_target_token("USDC", "robinhood")
        assert target is None
        assert warnings

    def test_from_dict_round_trip_coerces_null_to_sentinel(self):
        """`None` used to survive to_dict→from_dict and crash `.upper()`."""
        base = TeardownRequest(deployment_id="deployment:abc", mode=TeardownMode.SOFT).to_dict()

        for bad in (None, ""):
            payload = {**base, "target_token": bad}
            assert TeardownRequest.from_dict(payload).target_token == TARGET_TOKEN_CHAIN_DEFAULT

        payload = {k: v for k, v in base.items() if k != "target_token"}
        assert TeardownRequest.from_dict(payload).target_token == TARGET_TOKEN_CHAIN_DEFAULT

        payload = {**base, "target_token": "USDC"}
        assert TeardownRequest.from_dict(payload).target_token == "USDC"


class TestWriterDefaults:
    """Every writer must express "no preference", not a token."""

    def test_teardown_request_default_is_the_sentinel(self):
        request = TeardownRequest(deployment_id="deployment:abc", mode=TeardownMode.SOFT)
        assert request.target_token == TARGET_TOKEN_CHAIN_DEFAULT

    def test_hosted_stop_bridge_lane_resolves_chain_aware(self):
        """The hosted Platform "Teardown button" never touches the CLI.

        `runner_gateway.lifecycle_handle_stop` constructs a TeardownRequest with
        no target_token, so it inherits the dataclass default. This is why a
        CLI-only fix would have left hosted broken — assert the hosted-shaped
        request resolves correctly on a USDC-less chain.
        """
        hosted = TeardownRequest(
            deployment_id="deployment:631ef7930421",
            mode=TeardownMode.SOFT,
            reason="Lifecycle STOP command",
            requested_by="lifecycle",
        )
        assert hosted.target_token == TARGET_TOKEN_CHAIN_DEFAULT
        assert resolve_chain_target_token(hosted.target_token, "robinhood")[0] == "USDG"
        assert resolve_chain_target_token(hosted.target_token, "arbitrum")[0] == "USDC"

    def test_runner_config_carries_the_sentinel_verbatim(self):
        """The config builder must NOT collapse the sentinel — it runs before
        the chain is known."""
        from almanak.framework.runner._teardown_helpers import _teardown_config_from_request

        request = TeardownRequest(deployment_id="deployment:abc", mode=TeardownMode.SOFT)
        cfg = _teardown_config_from_request(request)
        assert cfg.target_token == TARGET_TOKEN_CHAIN_DEFAULT
        assert cfg.token_consolidation.target_token == TARGET_TOKEN_CHAIN_DEFAULT

    def test_programmatic_config_default_is_chain_aware(self):
        from almanak.framework.teardown.config import TeardownConfig, TokenConsolidationConfig

        assert TeardownConfig().target_token == TARGET_TOKEN_CHAIN_DEFAULT
        assert TokenConsolidationConfig().target_token == TARGET_TOKEN_CHAIN_DEFAULT


class TestResolvedTargetSurvivesFailure:
    """The failure path must report the target it actually used (CodeRabbit).

    `run_token_consolidation` never raises — it folds exceptions into a
    ConsolidationOutcome. That outcome originally omitted `target=`, so a raise
    AFTER resolution reported `target_token: None` — "no target was resolved" —
    when one was resolved and swaps may have been attempted against it. Empty ≠
    Zero: `None` must mean unmeasured, and on that path the value is known.
    Worst of all it lied on the failure path, exactly where an operator is
    debugging.
    """

    @pytest.mark.asyncio
    async def test_raise_after_resolution_still_reports_the_target(self, monkeypatch):
        """Mutation-verified: dropping ``target=`` from the except path fails this.

        The raise must land AFTER ``resolve_chain_target_token`` — patching the
        planner is the tightest way to guarantee that. A market-level explosion
        does NOT work: the phase returns early via the no-intents branch (which
        already carries the target), so the except path is never entered and the
        test passes with the bug present. That first attempt was a tautology;
        this one is not.
        """
        from types import SimpleNamespace
        from unittest.mock import MagicMock

        from almanak.framework.teardown import consolidation as _consolidation
        from almanak.framework.teardown.config import TeardownConfig
        from almanak.framework.teardown.models import TeardownMode
        from almanak.framework.teardown.runner_helpers import TeardownRunnerHelpers
        from almanak.framework.teardown.teardown_manager import TeardownManager

        def _boom(*_args, **_kwargs):
            raise RuntimeError("boom — planner exploded after the target was resolved")

        monkeypatch.setattr(_consolidation, "plan_consolidation", _boom)

        manager = object.__new__(TeardownManager)
        manager.config = TeardownConfig()
        manager.runner_helpers = TeardownRunnerHelpers()

        outcome = await manager.run_token_consolidation(
            SimpleNamespace(deployment_id="deployment:abc", chain="robinhood", get_teardown_profile=MagicMock()),
            teardown_id="teardown-1",
            teardown_state=MagicMock(),
            market=MagicMock(),
            closing_intents=[SimpleNamespace(chain="robinhood", from_token="WETH")],
            positions=None,
            mode=TeardownMode.SOFT,
            is_auto_mode=False,
        )

        # The except path was genuinely taken...
        assert outcome.failed == 1
        assert any("token consolidation raised" in w for w in outcome.warnings)
        # ...and it must not claim the phase had no target.
        assert outcome.target == "USDG", (
            f"failure path reported target={outcome.target!r} — the resolved target was dropped, "
            "so result_json tells the operator no target was chosen precisely while they are "
            "debugging why it failed (VIB-5727)"
        )

    @pytest.mark.asyncio
    async def test_raise_before_resolution_reports_no_target(self):
        """The other direction: `None` must still mean "genuinely unresolved".

        Empty ≠ Zero cuts both ways — the fix must not invent a target for a
        phase that never chose one.
        """
        from types import SimpleNamespace
        from unittest.mock import MagicMock

        from almanak.framework.teardown.config import TeardownConfig
        from almanak.framework.teardown.models import TeardownMode
        from almanak.framework.teardown.teardown_manager import TeardownManager

        manager = object.__new__(TeardownManager)
        manager.config = TeardownConfig()
        # Explode inside the token-universe read, BEFORE resolution happens.
        helpers = MagicMock()
        helpers.has_token_universe = True
        helpers.get_token_universe.side_effect = RuntimeError("boom — before resolution")
        manager.runner_helpers = helpers

        outcome = await manager.run_token_consolidation(
            SimpleNamespace(deployment_id="deployment:abc", chain="robinhood", get_teardown_profile=MagicMock()),
            teardown_id="teardown-1",
            teardown_state=MagicMock(),
            market=MagicMock(),
            closing_intents=[SimpleNamespace(chain="robinhood", from_token="WETH")],
            positions=None,
            mode=TeardownMode.SOFT,
            is_auto_mode=False,
        )

        assert outcome.failed == 1
        assert outcome.target is None, "a phase that never resolved a target must not report one"


class TestOperatorFacingRendering:
    """The sentinel is an internal marker — operators must never see it."""

    def test_sentinel_renders_as_auto_not_raw(self):
        from almanak.framework.cli.teardown import format_target_token

        rendered = format_target_token(TARGET_TOKEN_CHAIN_DEFAULT)
        assert TARGET_TOKEN_CHAIN_DEFAULT not in rendered
        assert "auto" in rendered.lower()

    def test_sentinel_does_not_render_as_a_guessed_usdc(self):
        """Printing "USDC" for an unresolved target is the mis-report the
        ticket is about — it is exactly what the operator saw on robinhood."""
        from almanak.framework.cli.teardown import format_target_token

        assert "USDC" not in format_target_token(TARGET_TOKEN_CHAIN_DEFAULT)
        assert "USDC" not in format_target_token(None)

    def test_explicit_target_renders_verbatim(self):
        from almanak.framework.cli.teardown import format_target_token

        assert format_target_token("USDG") == "USDG"
        assert format_target_token("USDC") == "USDC"


class TestUnknownChain:
    """Chain unknown → never worse than the pre-fix behaviour."""

    def test_unspecified_falls_back_to_legacy_usdc(self):
        assert resolve_chain_target_token(TARGET_TOKEN_CHAIN_DEFAULT, None)[0] == "USDC"

    def test_explicit_is_preserved(self):
        assert resolve_chain_target_token("USDT", None)[0] == "USDT"


class TestNoFabricatedDefault:
    """The `or "USDC"` fallbacks must not resurrect the bug."""

    def test_target_policy_with_no_resolved_target_skips(self):
        targets, warnings = resolve_consolidation_targets(
            TeardownAssetPolicy.TARGET_TOKEN, None, object()
        )
        assert targets is None, "a None target must skip, not default to USDC"
        assert warnings

    def test_target_policy_with_resolved_target_uses_it(self):
        targets, warnings = resolve_consolidation_targets(
            TeardownAssetPolicy.TARGET_TOKEN, "USDG", object()
        )
        assert targets == {"USDG"}
        assert warnings == []

    def test_keep_outputs_still_skips(self):
        assert resolve_consolidation_targets(TeardownAssetPolicy.KEEP_OUTPUTS, "USDC", object()) == (
            None,
            [],
        )


class TestSentinelContainment:
    """The sentinel must never escape into a swap or a report."""

    def test_sentinel_is_not_a_resolvable_symbol_on_any_chain(self):
        for chain in ALL_CHAINS:
            assert not token_resolves_on_chain(TARGET_TOKEN_CHAIN_DEFAULT, chain)

    @pytest.mark.parametrize("chain", ALL_CHAINS)
    def test_resolution_never_returns_the_sentinel(self, chain):
        target, _ = resolve_chain_target_token(TARGET_TOKEN_CHAIN_DEFAULT, chain)
        assert target != TARGET_TOKEN_CHAIN_DEFAULT
        if target is not None:
            assert token_resolves_on_chain(target, chain)

    def test_noop_gate_resolves_the_sentinel_rather_than_losing_credit(self):
        """VIB-5494 Item 1 credit must survive the sentinel.

        A sentinel reaching the gate un-resolved matches no token, silently
        dropping the no-op credit and re-arming the recurring failed-teardown
        loop that credit exists to prevent.
        """
        from almanak.framework.teardown.completeness import resolve_consolidation_noop_target

        assert (
            resolve_consolidation_noop_target(
                TeardownAssetPolicy.TARGET_TOKEN, TARGET_TOKEN_CHAIN_DEFAULT, chain="robinhood"
            )
            == "USDG"
        )
        assert (
            resolve_consolidation_noop_target(
                TeardownAssetPolicy.TARGET_TOKEN, TARGET_TOKEN_CHAIN_DEFAULT, chain="ethereum"
            )
            == "USDC"
        )

    def test_noop_gate_keeps_strict_fallback_for_other_policies(self):
        from almanak.framework.teardown.completeness import resolve_consolidation_noop_target

        for policy in (TeardownAssetPolicy.ENTRY_TOKEN, TeardownAssetPolicy.KEEP_OUTPUTS):
            assert resolve_consolidation_noop_target(policy, "USDC", chain="ethereum") is None

    def test_manager_noop_target_derives_chain_from_intents(self):
        """The manager's TD-11 gate must resolve against the SAME chain the
        consolidation phase will use, else it credits the wrong token."""
        from types import SimpleNamespace

        from almanak.framework.teardown.config import TeardownConfig
        from almanak.framework.teardown.teardown_manager import TeardownManager

        manager = object.__new__(TeardownManager)
        manager.config = TeardownConfig()

        intents = [SimpleNamespace(chain="robinhood")]
        assert manager._consolidation_noop_target(None, intents) == "USDG"

        # Chain from the strategy when no intent declares one.
        assert manager._consolidation_noop_target(SimpleNamespace(chain="robinhood"), []) == "USDG"

        # A USDC chain is untouched.
        assert manager._consolidation_noop_target(None, [SimpleNamespace(chain="base")]) == "USDC"


class TestNoopTargetCallSites:
    """BOTH lanes must supply chain context — there are two, and only one was
    fixed initially (caught in review of PR #3285).

    `_consolidation_noop_target` has two call sites: the manager's own
    `execute` (teardown_manager.py) and the runner lane
    (`_teardown_helpers.execute_and_verify`). The runner lane called it with no
    arguments, so `chain` resolved to None → legacy USDC → on robinhood the
    TD-11 gate credited a token the wallet cannot hold while denying credit to
    USDG. Two lanes, two tests.
    """

    def test_noop_target_requires_explicit_chain_context(self):
        """The params are REQUIRED, not defaulted.

        This is the structural fix: a defaulted param turns "the caller forgot"
        into a silently wrong answer (legacy USDC); a required one turns it into
        a TypeError at the call site. `None` remains a valid *value* — a caller
        with genuinely no chain must say so explicitly.
        """
        from almanak.framework.teardown.config import TeardownConfig
        from almanak.framework.teardown.teardown_manager import TeardownManager

        manager = object.__new__(TeardownManager)
        manager.config = TeardownConfig()

        with pytest.raises(TypeError):
            manager._consolidation_noop_target()  # type: ignore[call-arg]

    def test_runner_lane_resolves_the_chain_token_not_legacy_usdc(self):
        """The runner lane, driven with the arguments it actually passes."""
        from types import SimpleNamespace

        from almanak.framework.teardown.config import TeardownConfig
        from almanak.framework.teardown.teardown_manager import TeardownManager

        manager = object.__new__(TeardownManager)
        manager.config = TeardownConfig()

        strategy = SimpleNamespace(chain="robinhood")
        teardown_intents = [SimpleNamespace(chain="robinhood")]
        assert manager._consolidation_noop_target(strategy, teardown_intents) == "USDG"

    @pytest.mark.asyncio
    async def test_execute_and_verify_hands_the_gate_its_chain_context(self):
        """THE regression test: drive the real caller and spy on what it passes.

        Verified by mutation — reinstating the bare ``_consolidation_noop_target()``
        call makes this fail. That is the bar the two tests above do NOT meet:
        both exercise the *manager*, which was already correct, so both pass with
        the bug present. The defect lived in the CALLER dropping the context, and
        only driving the caller can catch it.
        """
        from unittest.mock import MagicMock

        from almanak.framework.runner import _teardown_helpers as _h
        from almanak.framework.teardown.models import TeardownMode

        from .test_teardown_consolidation_flow import (
            _make_positions,
            _make_state,
            _make_strategy,
            _mgr_mock_for_runner_lane,
            _result,
        )

        mgr = _mgr_mock_for_runner_lane(closure_result=_result(success=True))
        mgr._consolidation_noop_target = MagicMock(return_value="USDG")

        strategy = _make_strategy()
        teardown_intents = [{"intent_type": "LP_CLOSE"}]

        await _h.execute_and_verify(
            MagicMock(),  # runner
            mgr,
            MagicMock(),  # teardown_state_adapter
            _make_state(pending=[{"intent_type": "LP_CLOSE"}], completed=0),
            strategy,
            teardown_intents,
            _make_positions(),
            TeardownMode.SOFT,
            None,  # teardown_market
            True,  # is_auto_mode
            None,  # price_oracle
            MagicMock(),  # request
            MagicMock(),  # state_manager
        )

        mgr._consolidation_noop_target.assert_called_once_with(strategy, teardown_intents)
        args, _kwargs = mgr._consolidation_noop_target.call_args
        assert args, (
            "execute_and_verify called _consolidation_noop_target() with NO arguments — the chain "
            "is dropped and the sentinel silently resolves to legacy USDC, crediting the wrong "
            "token on a USDC-less chain (VIB-5727)"
        )

    def test_runner_lane_call_site_passes_chain_context(self):
        """Belt-and-braces source pin, cheap and independent of the mock harness."""
        import inspect

        from almanak.framework.runner._teardown_helpers import execute_and_verify

        src = inspect.getsource(execute_and_verify)
        assert "_consolidation_noop_target()" not in src, (
            "execute_and_verify must not call _consolidation_noop_target() with no arguments — "
            "chain context is dropped and the target silently resolves to legacy USDC (VIB-5727)"
        )
        assert "_consolidation_noop_target(strategy, teardown_intents)" in src, (
            "execute_and_verify must pass strategy + teardown_intents so the no-op gate resolves "
            "the same target the consolidation phase will use (VIB-5727)"
        )
