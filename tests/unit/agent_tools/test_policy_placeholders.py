"""VIB-4167 — Defense-in-depth tests for placeholder-primitive refusal.

These tests are the test-first contract for VIB-4167 (T7 of the VIB-4160
primitives refactor). The card is at
``docs/internal/uat-cards/VIB-4167.md``; do NOT edit assertions here without
amending the card and re-running Phase 1.

The trust statement (card §1):

    The agent_tools PolicyEngine refuses every placeholder IntentType
    (LIQUIDATE, OPEN_CDP, MINT_STABLE, REPAY_STABLE, CLOSE_CDP) at the
    LLM-mediated tool boundary, regardless of
    AgentPolicy.allowed_intent_types configuration, until P1 wires real
    connectors.

Three LLM-mediated paths carry an LLM-controlled ``intent_type``:

    1. ``compile_intent``                              — D1.1, D1.4, D1.5
    2. ``simulate_intent``                             — D1.2
    3. ``execute_compiled_bundle`` re-validation       — D1.3, D1.6

Plus the static / structural guards:

    F3  no convenience tool emits a placeholder       — F3 test
    F6a policy.py guard not inside swallowing try     — F6a test
    F6b executor.py wires up the helper call          — F6b test
    F7  Gate A and Gate B share the SAME frozenset    — F7 test
"""

from __future__ import annotations

import ast
import inspect
from pathlib import Path

import pytest

import almanak.framework.agent_tools.policy as policy_module
from almanak.framework.agent_tools.catalog import RiskTier, ToolCategory, ToolDefinition, get_default_catalog
from almanak.framework.agent_tools.errors import RiskBlockedError
from almanak.framework.agent_tools.executor import ToolExecutor
from almanak.framework.agent_tools.policy import (
    _PLACEHOLDER_INTENT_NAMES,
    _normalize_intent_type_for_gate,
    AgentPolicy,
    PolicyEngine,
)
from almanak.framework.agent_tools.testing import MockGatewayClient, MockGatewayConfig
from almanak.framework.intents.compiler import _PLACEHOLDER_INTENT_TYPES

# Sorted list of placeholder string values, e.g.
# ["CLOSE_CDP", "LIQUIDATE", "MINT_STABLE", "OPEN_CDP", "REPAY_STABLE"].
# Sorted so pytest's parametrize id generation is deterministic for CI logs.
PLACEHOLDER_VALUES: list[str] = sorted(t.value for t in _PLACEHOLDER_INTENT_TYPES)


# ---------------------------------------------------------------------------
# Helper: build a minimal ToolDefinition for direct PolicyEngine.check() tests
# ---------------------------------------------------------------------------


def _planning_tool(name: str) -> ToolDefinition:
    """Return the real ToolDefinition for a planning tool name (compile_intent /
    simulate_intent). The placeholder check fires inside
    ``_check_intent_type_allowed`` regardless of risk_tier, so we use the
    actual catalog entries to keep the test honest about request_schema /
    risk_tier shape."""
    catalog = get_default_catalog()
    tool = catalog.get(name)
    assert tool is not None, f"catalog is missing tool {name!r}"
    return tool


# ===========================================================================
# D1.1 — compile_intent refuses each placeholder
# ===========================================================================


class TestD1_1_CompileIntentRefusesEachPlaceholder:
    @pytest.mark.parametrize("placeholder", PLACEHOLDER_VALUES)
    def test_compile_intent_refuses_each_placeholder(self, placeholder: str) -> None:
        engine = PolicyEngine(AgentPolicy(allowed_chains={"arbitrum"}, allowed_intent_types=None))
        decision = engine.check(
            _planning_tool("compile_intent"),
            {
                "intent_type": placeholder,
                "params": {"chain": "arbitrum"},
                "chain": "arbitrum",
            },
        )
        assert decision.allowed is False, decision.violations
        joined = " | ".join(decision.violations).lower()
        assert "placeholder" in joined, decision.violations
        assert placeholder in " | ".join(decision.violations), decision.violations


# ===========================================================================
# D1.2 — simulate_intent refuses each placeholder
# ===========================================================================


class TestD1_2_SimulateIntentRefusesEachPlaceholder:
    @pytest.mark.parametrize("placeholder", PLACEHOLDER_VALUES)
    def test_simulate_intent_refuses_each_placeholder(self, placeholder: str) -> None:
        engine = PolicyEngine(AgentPolicy(allowed_chains={"arbitrum"}, allowed_intent_types=None))
        decision = engine.check(
            _planning_tool("simulate_intent"),
            {
                "intent_type": placeholder,
                "params": {"chain": "arbitrum"},
                "chain": "arbitrum",
            },
        )
        assert decision.allowed is False, decision.violations
        joined = " | ".join(decision.violations)
        # Mirror D1.1: the violation must name the specific placeholder so
        # the LLM hint loop can correct itself rather than just "you used a
        # placeholder, but I'm not telling you which one".
        assert "placeholder" in joined.lower(), decision.violations
        assert placeholder in joined, decision.violations

    @pytest.mark.parametrize("placeholder", PLACEHOLDER_VALUES)
    @pytest.mark.parametrize("dry_run", [False, True], ids=["live", "dry_run"])
    @pytest.mark.asyncio
    async def test_simulate_intent_with_cached_bundle_refuses_placeholder(
        self,
        placeholder: str,
        dry_run: bool,
        tmp_path: Path,
    ) -> None:
        """CodeRabbit-Major regression guard. ``simulate_intent(bundle_id=...)``
        loads the cached bundle bytes and calls ``Execute(dry_run=True)``
        directly. Without re-checking the cached args' ``intent_type``, a
        placeholder bundle slips past Gate A on the simulate path even though
        ``execute_compiled_bundle`` correctly refuses it."""
        from almanak.framework.agent_tools.bundle_cache import BundleCache

        bundle_cache = BundleCache(cache_dir=tmp_path)
        wallet = "0x" + "f" * 40
        executor = ToolExecutor(
            MockGatewayClient(MockGatewayConfig()),
            policy=AgentPolicy(allowed_chains={"arbitrum"}),
            wallet_address=wallet,
            deployment_id="vib-4167-test",
            bundle_cache=bundle_cache,
        )

        bundle_id = f"vib-4167-sim-bundle-{placeholder}-{dry_run}"
        bundle_cache.put(
            bundle_id,
            "arbitrum",
            b"opaque-bundle",
            {
                "intent_type": placeholder,
                "params": {"chain": "arbitrum"},
                "chain": "arbitrum",
            },
            wallet_address=wallet,
            deployment_id="vib-4167-test",
        )

        result = await executor.execute(
            "simulate_intent",
            {"bundle_id": bundle_id, "chain": "arbitrum"},
        )

        assert result.status == "error", (placeholder, result)
        assert result.error is not None, result
        assert result.error.get("error_code") == "risk_blocked", (placeholder, result.error)
        haystack = " | ".join(
            [
                str(result.error.get("message", "")),
                str(result.error.get("suggestion", "")),
            ]
        ).lower()
        assert "placeholder" in haystack, (placeholder, result.error)


# ===========================================================================
# D1.3 — Helper-level: _check_intent_type_allowed refuses placeholder cached args
# ===========================================================================


class TestD1_3_CheckIntentTypeAllowedRefusesEachPlaceholder:
    @pytest.mark.parametrize("placeholder", PLACEHOLDER_VALUES)
    def test_check_intent_type_allowed_refuses_each_placeholder(self, placeholder: str) -> None:
        engine = PolicyEngine(AgentPolicy())
        violations: list[str] = []
        suggestions: list[str] = []
        engine._check_intent_type_allowed(
            {
                "intent_type": placeholder,
                "params": {"chain": "arbitrum"},
                "chain": "arbitrum",
            },
            violations,
            suggestions,
        )
        assert any("placeholder" in v.lower() for v in violations), violations
        assert any(placeholder in v for v in violations), violations
        # The hint must steer the agent to a wired primitive.
        assert any("wired primitive" in s.lower() or "p1" in s.lower() for s in suggestions), suggestions


# ===========================================================================
# D1.4 — Refusal is unconditional regardless of allowed_intent_types
# ===========================================================================


class TestD1_4_PlaceholderRefusalIsNotDefeatableByAllowedIntentTypes:
    @pytest.mark.parametrize("placeholder", PLACEHOLDER_VALUES)
    @pytest.mark.parametrize(
        "allowed_intent_types",
        [
            None,
            set(),
            set(PLACEHOLDER_VALUES),  # explicit "I want to allow placeholders" config
            {"LIQUIDATE", "OPEN_CDP", "MINT_STABLE", "REPAY_STABLE", "CLOSE_CDP", "SWAP"},
        ],
        ids=["None", "empty", "all_placeholders", "all_placeholders_plus_swap"],
    )
    def test_placeholder_refusal_is_not_defeatable_by_allowed_intent_types(
        self,
        placeholder: str,
        allowed_intent_types: set[str] | None,
    ) -> None:
        engine = PolicyEngine(AgentPolicy(allowed_intent_types=allowed_intent_types))
        violations: list[str] = []
        engine._check_intent_type_allowed({"intent_type": placeholder}, violations, [])
        assert any("placeholder" in v.lower() for v in violations), (
            f"Placeholder refusal MUST fire even when allowed_intent_types={allowed_intent_types!r}: {violations}"
        )


# ===========================================================================
# D1.5 — Refusal is case-insensitive
# ===========================================================================


class TestD1_5_PlaceholderRefusalIsCaseInsensitive:
    @pytest.mark.parametrize("placeholder", PLACEHOLDER_VALUES)
    @pytest.mark.parametrize("transform", [str.upper, str.lower, str.title, str.swapcase])
    def test_placeholder_refusal_is_case_insensitive(self, placeholder: str, transform) -> None:
        intent_str = transform(placeholder)
        engine = PolicyEngine(AgentPolicy())
        violations: list[str] = []
        engine._check_intent_type_allowed({"intent_type": intent_str}, violations, [])
        assert any("placeholder" in v.lower() for v in violations), (intent_str, violations)


# ===========================================================================
# D1.5b — Refusal matches gateway's separator + whitespace normalisation
# ===========================================================================


class TestD1_5b_PlaceholderRefusalMatchesGatewayNormalisation:
    """Mirror gateway's ``_normalize_intent_type`` so an LLM cannot smuggle
    a placeholder past Gate A by passing an alias the gateway would have
    normalised to the same enum (e.g. ``"open-cdp"`` / ``"opencdp"``).
    Without this, Gate B catches the alias correctly but the "no gRPC,
    no bundle cached" property of Gate A is silently lost."""

    @pytest.mark.parametrize("placeholder", PLACEHOLDER_VALUES)
    @pytest.mark.parametrize(
        "transform",
        [
            lambda s: s.replace("_", "-"),
            lambda s: s.replace("_", ""),
            lambda s: f"  {s}  ",
            lambda s: s.lower().replace("_", "-"),
            lambda s: s.lower().replace("_", ""),
            lambda s: f"\t{s.lower()}\n",
            # CodeRabbit-Major: embedded whitespace must also normalise.
            lambda s: s.replace("_", " "),               # OPEN CDP
            lambda s: s.lower().replace("_", " "),       # open cdp
            lambda s: s.replace("_", "\t"),              # OPEN\tCDP
            lambda s: f" {s.lower().replace('_', ' ')} ",  # mixed pad + space
        ],
        ids=[
            "underscore_to_dash",
            "no_separator",
            "whitespace_padded",
            "lowercase_dash",
            "lowercase_no_separator",
            "tab_newline_padded_lowercase",
            "embedded_space",
            "embedded_space_lower",
            "embedded_tab",
            "embedded_space_padded",
        ],
    )
    def test_placeholder_refusal_handles_separator_and_whitespace_aliases(
        self, placeholder: str, transform
    ) -> None:
        intent_str = transform(placeholder)
        engine = PolicyEngine(AgentPolicy())
        violations: list[str] = []
        engine._check_intent_type_allowed({"intent_type": intent_str}, violations, [])
        assert any("placeholder" in v.lower() for v in violations), (
            f"Gate A failed to refuse alias {intent_str!r} — gateway would have "
            f"normalised this to a placeholder enum, so Gate A must too."
        )

    @pytest.mark.parametrize("placeholder_value", PLACEHOLDER_VALUES)
    def test_placeholder_refusal_accepts_intent_type_enum_member(self, placeholder_value: str) -> None:
        """gemini-Major regression guard. Pydantic ``model_dump()`` returns
        a string for ``intent_type: str``, but defense-in-depth: if a
        caller passes an ``IntentType`` enum member directly,
        ``isinstance(intent_type, str)`` would short-circuit and skip the
        gate. The gate must accept either shape."""
        from almanak.framework.intents.vocabulary import IntentType

        engine = PolicyEngine(AgentPolicy())
        violations: list[str] = []
        suggestions: list[str] = []
        engine._check_intent_type_allowed(
            {"intent_type": IntentType(placeholder_value)}, violations, suggestions
        )
        assert any("placeholder" in v.lower() for v in violations), (placeholder_value, violations)

    def test_normalisation_helper_matches_gateway_semantics(self) -> None:
        """Direct contract test on the normalisation helper. Mirrors the
        rule documented in the helper docstring AND in
        ``almanak/gateway/services/execution_service.py::_normalize_intent_type``."""
        # All shapes of "OPEN_CDP" must collapse to the same key.
        assert _normalize_intent_type_for_gate("OPEN_CDP") == "opencdp"
        assert _normalize_intent_type_for_gate("open_cdp") == "opencdp"
        assert _normalize_intent_type_for_gate("open-cdp") == "opencdp"
        assert _normalize_intent_type_for_gate("opencdp") == "opencdp"
        assert _normalize_intent_type_for_gate("  OPEN-CDP  ") == "opencdp"
        # CodeRabbit-Major: embedded whitespace must also normalise away.
        # ``str.strip`` only handles edges; without an explicit drop, an
        # alias like "open cdp" or "open\tcdp" sails past Gate A.
        assert _normalize_intent_type_for_gate("open cdp") == "opencdp"
        assert _normalize_intent_type_for_gate("OPEN\tCDP") == "opencdp"
        assert _normalize_intent_type_for_gate("OPEN \n CDP") == "opencdp"
        assert _normalize_intent_type_for_gate(" open _ cdp ") == "opencdp"
        # Real intent types do NOT collapse onto a placeholder key.
        assert _normalize_intent_type_for_gate("LP_OPEN") == "lpopen"
        assert _normalize_intent_type_for_gate("SWAP") == "swap"


# ===========================================================================
# D1.6 — End-to-end via ToolExecutor: execute_compiled_bundle refuses cached placeholder
# ===========================================================================


class TestD1_6_ExecuteCompiledBundleRefusesCachedPlaceholderViaExecutor:
    """The load-bearing F4 test. A regression that removes the
    ``_check_intent_type_allowed`` call from ``_execute_compiled_bundle``'s
    re-validation block trips this test directly."""

    @pytest.mark.parametrize("placeholder", PLACEHOLDER_VALUES)
    @pytest.mark.parametrize("dry_run", [False, True], ids=["live", "dry_run"])
    @pytest.mark.asyncio
    async def test_execute_compiled_bundle_refuses_cached_placeholder_via_executor(
        self,
        placeholder: str,
        dry_run: bool,
        tmp_path: Path,
    ) -> None:
        """Both live (`dry_run=False`) AND dry_run (`dry_run=True`) paths
        must refuse a cached placeholder. The dry_run case is the load-bearing
        Codex-finding regression: a dry_run still calls Execute() in
        simulation mode, so leaving the placeholder check inside
        ``if not dry_run:`` would let a placeholder bundle reach the
        gateway and defeat Gate A's "no gRPC dispatch" promise."""
        from almanak.framework.agent_tools.bundle_cache import BundleCache

        bundle_cache = BundleCache(cache_dir=tmp_path)
        wallet = "0x" + "f" * 40
        executor = ToolExecutor(
            MockGatewayClient(MockGatewayConfig()),
            policy=AgentPolicy(allowed_chains={"arbitrum"}),
            wallet_address=wallet,
            deployment_id="vib-4167-test",
            bundle_cache=bundle_cache,
        )

        bundle_id = f"vib-4167-placeholder-bundle-{placeholder}-{dry_run}"
        bundle_cache.put(
            bundle_id,
            "arbitrum",
            b"opaque-bundle",
            {
                "intent_type": placeholder,
                "params": {"chain": "arbitrum"},
                "chain": "arbitrum",
            },
            wallet_address=wallet,
            deployment_id="vib-4167-test",
        )

        result = await executor.execute(
            "execute_compiled_bundle",
            {"bundle_id": bundle_id, "chain": "arbitrum", "dry_run": dry_run},
        )

        # The card requires status "error" + risk_blocked code + cite "placeholder".
        # ToolExecutor.execute() catches RiskBlockedError and returns a structured
        # error envelope (see executor.py::execute() except-handlers). The error
        # envelope key is "error_code" (per _error_dict / ToolError.to_dict()).
        assert result.status == "error", (placeholder, dry_run, result)
        assert result.error is not None, (placeholder, dry_run, result)
        # AgentErrorCode.RISK_BLOCKED.value is "risk_blocked".
        assert result.error.get("error_code") == "risk_blocked", (placeholder, dry_run, result.error)
        # The placeholder violation message MUST be carried into the error envelope
        # (either in the message, the singular ``suggestion`` field, OR a plural
        # ``suggestions``/``violations`` payload — different envelope shapes have
        # appeared across the executor's error paths, and the gate must surface
        # the placeholder cite via at least one of them).
        haystack = " | ".join(
            [
                str(result.error.get("message", "")),
                str(result.error.get("suggestion", "")),
                " | ".join(str(s) for s in (result.error.get("suggestions") or [])),
                " | ".join(str(s) for s in (result.error.get("violations") or [])),
            ]
        ).lower()
        assert "placeholder" in haystack, (placeholder, dry_run, result.error)


# ===========================================================================
# F3 — _action_to_intent never returns a placeholder
# ===========================================================================


class TestF3_ActionToIntentNeverReturnsPlaceholder:
    """Static parity guard: the convenience action mapping (swap_tokens →
    "swap", open_lp_position → "lp_open", …) cannot grow a placeholder
    output. Catches the "someone wires liquidate_position to LIQUIDATE before
    P1 ships the connector" regression."""

    # Minimal valid args for each known action tool. New action tools added
    # to ``_action_to_intent`` must be added here too — the test enumerates
    # every action-category catalog entry and fails closed on missing args.
    _ACTION_TOOL_MIN_ARGS: dict[str, dict] = {
        "swap_tokens": {
            "token_in": "USDC",
            "token_out": "WETH",
            "amount": "10",
            "slippage_bps": 50,
        },
        "open_lp_position": {
            "token_a": "USDC",
            "token_b": "WETH",
            "amount_a": "10",
            "amount_b": "0.005",
            "price_lower": "0.0003",
            "price_upper": "0.0004",
            "fee_tier": "500",
            "protocol": "uniswap_v3",
        },
        "close_lp_position": {
            "position_id": "1234",
            "amount": "all",
            "protocol": "uniswap_v3",
        },
        "supply_lending": {"token": "USDC", "amount": "10", "protocol": "aave_v3"},
        "borrow_lending": {
            "token": "USDC",
            "amount": "10",
            "collateral_token": "WETH",
            "collateral_amount": "0.005",
            "protocol": "aave_v3",
        },
        "repay_lending": {"token": "USDC", "amount": "10", "protocol": "aave_v3"},
        "withdraw_lending": {"token": "USDC", "amount": "10", "protocol": "aave_v3"},
        "bridge_tokens": {
            "token": "USDC",
            "amount": "10",
            "from_chain": "arbitrum",
            "to_chain": "base",
            "slippage_bps": 50,
        },
        "wrap_native": {"token": "ETH", "amount": "0.01"},
        "unwrap_native": {"token": "WETH", "amount": "0.01"},
    }

    # Action tools that don't go through ``_action_to_intent`` (handled by
    # dedicated dispatch — vault lifecycle + execute_compiled_bundle). They
    # are explicitly exempt from the F3 enumeration but the test asserts
    # the complete action-category set equals the union of mapped + exempt
    # so a NEW unexpected tool fails closed.
    _EXEMPT_ACTION_TOOLS: frozenset[str] = frozenset(
        {
            "execute_compiled_bundle",
            "deploy_vault",
            "settle_vault",
            "approve_vault_underlying",
            "deposit_vault",
            "teardown_vault",
        }
    )

    def test_action_to_intent_never_returns_placeholder(self) -> None:
        catalog = get_default_catalog()
        action_tools = [t.name for t in catalog.list_tools(category=ToolCategory.ACTION)]

        # Closed-set guard — every action-category tool is either in the
        # mapped set OR the exempt set. A new tool added to the catalog that
        # is in neither trips this test.
        unmapped_unexempt = sorted(
            set(action_tools) - set(self._ACTION_TOOL_MIN_ARGS.keys()) - self._EXEMPT_ACTION_TOOLS
        )
        assert not unmapped_unexempt, (
            f"New action tools introduced without F3 coverage: {unmapped_unexempt}. "
            f"Add minimal args to TestF3_ActionToIntentNeverReturnsPlaceholder._ACTION_TOOL_MIN_ARGS, "
            f"or add to _EXEMPT_ACTION_TOOLS with a comment explaining why placeholder emission is "
            f"impossible."
        )

        # Build a real ToolExecutor whose _action_to_intent we will exercise.
        executor = ToolExecutor(
            MockGatewayClient(MockGatewayConfig()),
            policy=AgentPolicy(),
            wallet_address="0x" + "0" * 40,
        )

        # Lowercased placeholder set for case-insensitive containment check —
        # _action_to_intent returns lowercase strings ("swap", "lp_open", …)
        # but a regression returning "LIQUIDATE" (upper) should still trip.
        placeholder_lower = _PLACEHOLDER_INTENT_NAMES

        for tool_name, args in self._ACTION_TOOL_MIN_ARGS.items():
            try:
                intent_type, _ = executor._action_to_intent(tool_name, dict(args))
            except Exception as exc:  # noqa: BLE001
                pytest.fail(
                    f"_action_to_intent({tool_name!r}, args) raised {type(exc).__name__}: {exc}. "
                    f"If the action signature changed, update _ACTION_TOOL_MIN_ARGS."
                )
            assert intent_type.lower() not in placeholder_lower, (
                f"REGRESSION: action tool {tool_name!r} maps to placeholder intent {intent_type!r}. "
                f"Placeholder primitives MUST NOT have a convenience-tool entry until P1."
            )


# ===========================================================================
# F6a — policy.py: _check_intent_type_allowed references placeholder set, not in swallowing try
# ===========================================================================


class TestF6a_PolicyCheckIntentTypeAllowedIsNotInsideASwallowingTry:
    """Static AST guard. A regression that wraps the placeholder check in a
    bare ``try: ... except: pass`` (the classic silent-failure shape) trips
    this test BEFORE the runtime tests can be reached at all."""

    def test_policy_check_intent_type_allowed_is_not_inside_a_swallowing_try(self) -> None:
        source = inspect.getsource(policy_module)
        tree = ast.parse(source)

        # Find the function definition.
        target: ast.FunctionDef | None = None
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "_check_intent_type_allowed":
                target = node
                break
        assert target is not None, "policy._check_intent_type_allowed function not found in AST walk"

        # The function body MUST reference the placeholder names set
        # (either _PLACEHOLDER_INTENT_NAMES or _PLACEHOLDER_INTENT_TYPES).
        placeholder_refs = [
            node
            for node in ast.walk(target)
            if isinstance(node, ast.Name) and node.id in {"_PLACEHOLDER_INTENT_NAMES", "_PLACEHOLDER_INTENT_TYPES"}
        ]
        assert placeholder_refs, (
            "_check_intent_type_allowed body does NOT reference the placeholder set. "
            "The Gate A boundary is silently turned off."
        )

        # Each reference must NOT be nested inside a try block whose handlers
        # swallow exceptions (bare ``except:`` or ``except Exception:`` with
        # no ``raise``). This is the literal F6 silent-error shape.
        for ref in placeholder_refs:
            for ancestor in _ancestors_of(target, ref):
                if not isinstance(ancestor, ast.Try):
                    continue
                for handler in ancestor.handlers:
                    if _handler_swallows(handler):
                        pytest.fail(
                            f"Placeholder reference at policy.py line {ref.lineno} is nested inside a "
                            f"swallowing try/except (handler line {handler.lineno}). This is the "
                            f"silent-failure regression shape — fix the try block, do not silence "
                            f"this test."
                        )


# ===========================================================================
# F6b — executor.py: _execute_compiled_bundle invokes _check_intent_type_allowed
# ===========================================================================


class TestF6b_ExecutorExecuteCompiledBundleInvokesIntentTypeCheck:
    """Static AST guard. F4's wire-up regression (helper added but executor
    forgets to call it) is caught at runtime by D1.6 AND structurally here."""

    def test_executor_execute_compiled_bundle_invokes_intent_type_check(self) -> None:
        from almanak.framework.agent_tools import executor as executor_module

        source = inspect.getsource(executor_module)
        tree = ast.parse(source)

        # Find _execute_compiled_bundle (async method).
        target: ast.AsyncFunctionDef | ast.FunctionDef | None = None
        for node in ast.walk(tree):
            if isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef)) and node.name == "_execute_compiled_bundle":
                target = node
                break
        assert target is not None, "_execute_compiled_bundle method not found in AST walk"

        # Locate the wire-up call. After the helper extraction, the gate is
        # invoked via ``self._raise_if_cached_args_are_placeholder(...)``;
        # the inline ``self._policy_engine._check_intent_type_allowed(...)``
        # is also accepted as a direct wire-up shape. EITHER must exist —
        # the helper itself is checked by F6d below.
        intent_type_calls = _find_policy_engine_calls(target, "_check_intent_type_allowed")
        helper_calls = _find_self_method_calls(target, "_raise_if_cached_args_are_placeholder")
        gate_calls = intent_type_calls + helper_calls

        chain_calls = _find_policy_engine_calls(target, "_check_chain_allowed")

        assert gate_calls, (
            "_execute_compiled_bundle does NOT invoke either "
            "self._policy_engine._check_intent_type_allowed(...) OR "
            "self._raise_if_cached_args_are_placeholder(...). The cached-args placeholder "
            "gate is silently turned off (F4 regression)."
        )
        assert chain_calls, (
            "Sibling-call anchor _check_chain_allowed is missing — the F6b structural test "
            "needs to be re-anchored against a different sibling that DOES exist in the "
            "re-validation block."
        )

        # Each gate call must NOT be inside a swallowing try/except.
        for call in gate_calls:
            for ancestor in _ancestors_of(target, call):
                if not isinstance(ancestor, ast.Try):
                    continue
                for handler in ancestor.handlers:
                    if _handler_swallows(handler):
                        pytest.fail(
                            f"Gate-A call at executor.py line {call.lineno} is "
                            f"nested inside a swallowing try/except (handler line {handler.lineno})."
                        )

        # Both calls live inside the same _execute_compiled_bundle method,
        # so they trivially share the function as enclosing AST node — this
        # asserts the call is at least co-located with the existing
        # re-validation surface, even if (correctly) hoisted out of the
        # dry_run-scoped block (see F6c below).
        gate_ancestors = {id(a) for a in _ancestors_of(target, gate_calls[0])}
        chain_ancestors = {id(a) for a in _ancestors_of(target, chain_calls[0])}
        shared = gate_ancestors & chain_ancestors
        assert shared, (
            "Gate-A call and chain check do not share any enclosing AST node — the "
            "intent_type gate has been moved entirely out of _execute_compiled_bundle, "
            "which would mean cached placeholders are unguarded."
        )


# ===========================================================================
# F6d — The helper _raise_if_cached_args_are_placeholder must call the gate
# ===========================================================================


class TestF6d_HelperInvokesPolicyEngineGate:
    """When the wire-up is hidden behind a helper (DRY for the
    ``execute_compiled_bundle`` and ``simulate_intent`` cached-bundle
    paths), the helper itself must invoke
    ``self._policy_engine._check_intent_type_allowed(...)``. Without this,
    the F6b helper-shape branch would silently pass even though the
    helper is a no-op."""

    def test_helper_invokes_policy_engine_gate(self) -> None:
        from almanak.framework.agent_tools import executor as executor_module

        source = inspect.getsource(executor_module)
        tree = ast.parse(source)

        target: ast.AsyncFunctionDef | ast.FunctionDef | None = None
        for node in ast.walk(tree):
            if (
                isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef))
                and node.name == "_raise_if_cached_args_are_placeholder"
            ):
                target = node
                break
        if target is None:
            # The helper is optional — if the wire-up is inlined directly
            # in both call sites, F6b's direct branch handles the check.
            pytest.skip(
                "_raise_if_cached_args_are_placeholder helper not present; "
                "wire-up must be inlined and is verified by F6b directly."
            )

        intent_type_calls = _find_policy_engine_calls(target, "_check_intent_type_allowed")
        assert intent_type_calls, (
            "_raise_if_cached_args_are_placeholder is defined but does NOT invoke "
            "self._policy_engine._check_intent_type_allowed(...) — the helper is a no-op."
        )

        # And it must raise on placeholder violations (else F6b's call to
        # the helper would silently swallow). Assert at least one ast.Raise.
        has_raise = any(isinstance(n, ast.Raise) for n in ast.walk(target))
        assert has_raise, (
            "_raise_if_cached_args_are_placeholder does not raise — placeholder "
            "violations would be silently absorbed."
        )


# ===========================================================================
# F6c — Codex P2 regression guard: placeholder check must run for dry_run too
# ===========================================================================


class TestF6c_PlaceholderCheckIsNotGuardedByNotDryRun:
    """Static AST guard. The placeholder check MUST run regardless of
    ``dry_run``, because Gate A's contract is "no gRPC dispatch for a
    placeholder" — and a dry_run still calls ``Execution.Execute(...)`` in
    simulation mode. A regression that puts the call back inside
    ``if not dry_run:`` would silently re-open the bypass that Codex caught
    on this PR.

    Mirror at runtime: D1.6 is parameterized over ``dry_run ∈ {True, False}``
    so a missing wire-up in the dry_run branch fails the runtime test too.
    F6c is the structural backstop in case D1.6 is silenced via skip
    decorators during a fix-loop.
    """

    def test_placeholder_check_is_not_guarded_by_not_dry_run(self) -> None:
        from almanak.framework.agent_tools import executor as executor_module

        source = inspect.getsource(executor_module)
        tree = ast.parse(source)

        target: ast.AsyncFunctionDef | ast.FunctionDef | None = None
        for node in ast.walk(tree):
            if isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef)) and node.name == "_execute_compiled_bundle":
                target = node
                break
        assert target is not None, "_execute_compiled_bundle method not found"

        # Either the direct gate call OR the helper call counts as the gate.
        intent_type_calls = _find_policy_engine_calls(target, "_check_intent_type_allowed")
        helper_calls = _find_self_method_calls(target, "_raise_if_cached_args_are_placeholder")
        gate_calls = intent_type_calls + helper_calls
        assert gate_calls, (
            "no Gate-A call (direct or via helper) found inside _execute_compiled_bundle"
        )

        # For each call, walk ancestors and assert NO ast.If with the
        # specific "not dry_run" test guards it. (An equivalent
        # ``if dry_run is False:`` is also rejected.)
        for call in gate_calls:
            for ancestor in _ancestors_of(target, call):
                if not isinstance(ancestor, ast.If):
                    continue
                if _is_not_dry_run_guard(ancestor.test):
                    pytest.fail(
                        f"Gate-A call at line {call.lineno} is nested inside "
                        f"`if not dry_run:` (or equivalent) at line {ancestor.lineno}. "
                        "This re-opens the dry_run bypass Codex flagged. The placeholder "
                        "check MUST run for dry_run=True too — only spend / chain / token / "
                        "position checks are correctly dry_run-scoped."
                    )


# ===========================================================================
# F7 — Gate A and Gate B share the SAME _PLACEHOLDER_INTENT_TYPES frozenset
# ===========================================================================


class TestF7_GateAAndGateBSharePlaceholderSet:
    """Runtime ``is`` identity check (NOT equality). Equality would silently
    pass for two identical copies; identity catches the shadow-copy
    regression."""

    def test_gate_a_and_gate_b_share_placeholder_set(self) -> None:
        from almanak.framework.intents.compiler import _PLACEHOLDER_INTENT_TYPES as gate_b
        from almanak.framework.agent_tools import policy as gate_a_module

        assert gate_a_module._PLACEHOLDER_INTENT_TYPES is gate_b, (
            "Gate A (agent_tools.policy) does NOT import the same _PLACEHOLDER_INTENT_TYPES "
            "frozenset object as Gate B (intents.compiler). Drift between the two consumers "
            "is now possible — fix the Gate A import to reference the canonical module."
        )

        # The derived names set is a single comprehension over the IntentType
        # frozenset — verify the relationship holds at runtime so a
        # maintainer can't replace it with a hard-coded list. Derivation
        # uses ``_normalize_intent_type_for_gate`` so Gate A's matching
        # surface mirrors gateway-side ``ExecutionService._normalize_intent_type``
        # (lowercase, no whitespace, no ``-``/``_``).
        expected_names = frozenset(_normalize_intent_type_for_gate(t.value) for t in gate_b)
        assert _PLACEHOLDER_INTENT_NAMES == expected_names, (
            "_PLACEHOLDER_INTENT_NAMES is out of sync with _PLACEHOLDER_INTENT_TYPES — "
            "the derivation must remain "
            "{_normalize_intent_type_for_gate(t.value) for t in _PLACEHOLDER_INTENT_TYPES}."
        )


# ===========================================================================
# AST helpers
# ===========================================================================


def _ancestors_of(root: ast.AST, target: ast.AST) -> list[ast.AST]:
    """Return the chain of AST ancestors from the immediate parent up to
    ``root`` (inclusive). Empty if ``target is root`` or ``target`` is not
    in the subtree."""
    parents: dict[int, ast.AST] = {}
    for node in ast.walk(root):
        for child in ast.iter_child_nodes(node):
            parents[id(child)] = node

    chain: list[ast.AST] = []
    current = parents.get(id(target))
    while current is not None:
        chain.append(current)
        if current is root:
            break
        current = parents.get(id(current))
    return chain


def _handler_swallows(handler: ast.ExceptHandler) -> bool:
    """A handler "swallows" exceptions when at least one normal-execution
    path through its body returns without re-raising.

    The earlier implementation returned False on ANY nested ``raise``,
    which falsely cleared shapes like ``except E: log(); if debug: raise``
    (the conditional re-raise still leaves the normal path silent). To
    catch that, we require the handler to UNCONDITIONALLY raise — the
    last statement in the body must be either ``raise`` or an unconditional
    structure (e.g. ``return`` / nested ``raise``) that exits the handler.

    A handler is considered NOT to swallow only when *every* possible
    flow leaving the body re-raises. Approximated by: the body's last
    statement is an ``ast.Raise``, OR the body's last statement is an
    ``ast.If`` whose every branch ends in ``ast.Raise`` (recursive). Any
    other shape — including conditional raise, raise-inside-loop,
    raise-inside-try — is treated as swallowing because the normal path
    can still leave silently.

    This is intentionally strict: the F6a/F6b guards are about preventing
    silent-error regressions, so the bar is "every path re-raises", not
    "some path re-raises".
    """
    if not handler.body:
        return True

    return not _node_always_raises(handler.body[-1])


def _node_always_raises(node: ast.stmt) -> bool:
    """Return True if every flow path through ``node`` exits via ``raise``."""
    if isinstance(node, ast.Raise):
        return True
    if isinstance(node, ast.If):
        if not node.orelse:
            return False
        return _block_always_raises(node.body) and _block_always_raises(node.orelse)
    if isinstance(node, ast.Try):
        # try block must always raise; every except handler must always raise.
        return _block_always_raises(node.body) and all(
            h.body and _block_always_raises(h.body) for h in node.handlers
        )
    if isinstance(node, ast.With):
        return _block_always_raises(node.body)
    return False


def _block_always_raises(body: list[ast.stmt]) -> bool:
    """Return True if executing ``body`` always exits via ``raise``."""
    return bool(body) and _node_always_raises(body[-1])


def _is_not_dry_run_guard(test: ast.expr) -> bool:
    """Return True if ``test`` is the AST shape ``not dry_run`` or
    ``dry_run is False`` (or boolean equivalents). Used by F6c."""
    # ``not dry_run``
    if isinstance(test, ast.UnaryOp) and isinstance(test.op, ast.Not):
        if isinstance(test.operand, ast.Name) and test.operand.id == "dry_run":
            return True
    # ``dry_run is False``  /  ``dry_run == False``
    if isinstance(test, ast.Compare) and len(test.ops) == 1 and len(test.comparators) == 1:
        left, op, right = test.left, test.ops[0], test.comparators[0]
        if isinstance(left, ast.Name) and left.id == "dry_run":
            if isinstance(op, (ast.Is, ast.Eq)) and isinstance(right, ast.Constant) and right.value is False:
                return True
    # ``False is dry_run`` / ``False == dry_run``
    if isinstance(test, ast.Compare) and len(test.ops) == 1 and len(test.comparators) == 1:
        left, op, right = test.left, test.ops[0], test.comparators[0]
        if isinstance(right, ast.Name) and right.id == "dry_run":
            if isinstance(op, (ast.Is, ast.Eq)) and isinstance(left, ast.Constant) and left.value is False:
                return True
    return False


def _find_policy_engine_calls(root: ast.AST, attr_name: str) -> list[ast.Call]:
    """Find every ``self._policy_engine.<attr_name>(...)`` call inside root."""
    matches: list[ast.Call] = []
    for node in ast.walk(root):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if not isinstance(func, ast.Attribute) or func.attr != attr_name:
            continue
        inner = func.value
        if not isinstance(inner, ast.Attribute) or inner.attr != "_policy_engine":
            continue
        if not isinstance(inner.value, ast.Name) or inner.value.id != "self":
            continue
        matches.append(node)
    return matches


def _find_self_method_calls(root: ast.AST, method_name: str) -> list[ast.Call]:
    """Find every ``self.<method_name>(...)`` call inside root."""
    matches: list[ast.Call] = []
    for node in ast.walk(root):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if not isinstance(func, ast.Attribute) or func.attr != method_name:
            continue
        if not isinstance(func.value, ast.Name) or func.value.id != "self":
            continue
        matches.append(node)
    return matches


# ===========================================================================
# Sanity test — the placeholder set has not silently shrunk to empty
# ===========================================================================


def test_placeholder_set_is_non_empty_and_contains_known_p0_values() -> None:
    """If P1 ever lands and removes a placeholder, this test must be updated
    to reflect the new bar AND the corresponding wired-connector test must
    be added in the same PR. Silent shrinkage of the placeholder set
    (Gate A becomes a no-op) is the failure mode this guards."""
    assert len(_PLACEHOLDER_INTENT_TYPES) >= 1, (
        "_PLACEHOLDER_INTENT_TYPES is empty — Gate A and Gate B have nothing to refuse. "
        "If P1 wired the last placeholder, intentionally remove this test in the SAME PR."
    )
    # The 5 P0 placeholders from VIB-4165 are the floor at the time of writing.
    # When a real connector ships, drop the corresponding name here.
    p0_floor = {"LIQUIDATE", "OPEN_CDP", "MINT_STABLE", "REPAY_STABLE", "CLOSE_CDP"}
    current = {t.value for t in _PLACEHOLDER_INTENT_TYPES}
    missing = p0_floor - current
    if missing:
        # A removal is allowed (P1 progress) but must be intentional.
        # Update the floor to the new reality in the same PR that wires the
        # connector and add a comment naming the VIB ticket here.
        pytest.fail(
            f"Placeholder primitives {sorted(missing)} were removed from "
            f"_PLACEHOLDER_INTENT_TYPES. If this is intentional (P1 wired the "
            f"connector), update p0_floor here and the placeholder docstring "
            f"in compiler.py in the SAME PR. If not, the silent removal IS the "
            f"regression."
        )


