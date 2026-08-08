"""ALM-3184: the managed-fork signal is a DECLARATION, and nothing else.

The defect this replaces: ``is_local_rpc`` returned True for **any** host on
port 8545-8550 and for any hostname containing ``anvil``. Money-path consumers
keyed real safety behaviour on it — the Uniswap-V3-family oracle price-impact
guard, and Enso's 5% slippage widening — so a production RPC proxy on ``:8545``
ran mainnet swaps with the manipulation guard off.

An intermediate revision replaced the heuristic with an ``anvil_nodeInfo``
probe. Review rejected that too, and correctly: ``fork_signal`` is re-exported
from ``almanak.framework.execution``, so the probe handed strategy code a
loopback socket primitive, and loopback is not sanctioned egress (AGENTS.md
§Gateway boundary; blueprint 20 — the sidecar guard rejects every non-gateway
loopback connect). The probe is gone. Nothing in this module contacts anything.

Every test here asserts one of two properties:

* **positive** — only a declared ``Network.ANVIL``, threaded as a literal
  ``True``, produces True; and
* **fail-safe** — absent, unknown, and malformed all resolve to production.
"""

from __future__ import annotations

import ast
import pathlib
import sys
from unittest.mock import MagicMock, patch

import pytest

from almanak.core.rpc_network import Network
from almanak.framework.execution import fork_signal
from almanak.framework.execution.fork_signal import (
    is_managed_fork_network,
    resolve_managed_fork,
)

# URL shapes the old heuristic accepted as "local". Every one of these is a
# plausible production endpoint, and none of them can influence the signal now.
PRODUCTION_URLS_THE_HEURISTIC_ACCEPTED = (
    "http://rpc-proxy.internal.example:8545",
    "http://10.0.0.5:8546",
    "https://anvil-cluster.rpc.example.com/v2/key",
    "https://rpc.example.com:8550",
)


class TestNoRuntimeDetectionSurvives:
    """The P1 remedy: no probe, no socket, no loopback primitive."""

    @pytest.mark.parametrize(
        "name",
        ["probe_managed_fork", "_probe_uncached", "is_loopback_endpoint", "reset_managed_fork_probe_cache"],
    )
    def test_probe_machinery_is_gone(self, name):
        """Deleted, not merely unused — it was reachable by strategy code."""
        assert not hasattr(fork_signal, name), (
            f"{name} is back. fork_signal is re-exported from almanak.framework.execution, "
            "so anything here that opens a socket is a strategy-reachable egress primitive."
        )

    def test_module_is_not_re_exporting_a_probe(self):
        from almanak.framework import execution

        assert not hasattr(execution, "probe_managed_fork")
        assert set(fork_signal.__all__) == {"is_managed_fork_network", "resolve_managed_fork"}

    def test_module_imports_no_transport(self):
        """No egress library is importable from this module, at any depth.

        AST-based, not a substring scan: the module's own prose necessarily
        names ``socket`` and ``web3`` while explaining why they are absent, and
        a raw ``in source`` check cannot tell an import from a docstring. It
        also catches a *function-local* ``import web3``, which is exactly the
        shape the removed probe used and the shape a reintroduction would take.
        """
        tree = ast.parse(pathlib.Path(fork_signal.__file__).read_text(encoding="utf-8"))
        banned = {"web3", "requests", "httpx", "aiohttp", "socket", "urllib", "http"}

        imported: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(alias.name.split(".")[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.add(node.module.split(".")[0])

        assert not (imported & banned), (
            f"fork_signal imports {sorted(imported & banned)}. It is re-exported from "
            "almanak.framework.execution, so any transport here is a strategy-reachable "
            "egress primitive (ALM-3184 P1)."
        )

    def test_resolution_never_touches_the_network(self):
        """Negative control: resolving any declaration constructs no transport.

        ``sys.modules`` is checked rather than a patch target because the point
        is that the module has no transport dependency at all — patching
        ``web3.Web3`` would only prove this one path avoided that one symbol.
        """
        with patch.dict(sys.modules, {"web3": None}):
            # If anything in the resolution path imported web3, this raises.
            assert resolve_managed_fork(None) is False
            assert resolve_managed_fork(True) is True
            assert resolve_managed_fork(False) is False


class TestIsManagedForkNetwork:
    """The only sanctioned mapping from a declared network to the signal."""

    def test_anvil_enum_is_a_fork(self):
        assert is_managed_fork_network(Network.ANVIL) is True

    @pytest.mark.parametrize("wire", ["anvil", "ANVIL", " Anvil "])
    def test_anvil_wire_values_are_a_fork(self, wire):
        assert is_managed_fork_network(wire) is True

    @pytest.mark.parametrize(
        "network",
        [Network.MAINNET, Network.SEPOLIA, Network.TESTNET, "mainnet", "sepolia", "testnet"],
    )
    def test_every_other_network_is_production(self, network):
        assert is_managed_fork_network(network) is False

    def test_absent_network_is_production(self):
        assert is_managed_fork_network(None) is False

    @pytest.mark.parametrize("garbage", ["", "not-a-network", "anvil-ish", 42, object()])
    def test_unparseable_network_is_production(self, garbage):
        """A malformed declaration is not a licence to disable a guard."""
        assert is_managed_fork_network(garbage) is False


class TestResolveManagedFork:
    """Declaration-only resolution and the fail-safe direction."""

    def test_declared_true_is_a_fork(self):
        assert resolve_managed_fork(True) is True

    def test_declared_false_is_production(self):
        assert resolve_managed_fork(False) is False

    def test_undeclared_is_production(self):
        """The behaviour change from the probe revision, pinned.

        Undeclared no longer means "go and find out"; it means production. A
        test that relied on sniffing must declare instead.
        """
        assert resolve_managed_fork(None) is False

    @pytest.mark.parametrize("truthy", ["true", "false", 1, object(), MagicMock()])
    def test_malformed_declaration_is_production(self, truthy):
        """A truthy non-bool must not out-rank an absent declaration.

        ``MagicMock()`` is the case that matters in practice: a test double's
        auto-created attribute is truthy, so a truthiness check would silently
        disable the guard in every mock-context unit test — and would have made
        this whole change look green while shipping nothing.
        """
        assert resolve_managed_fork(truthy) is False

    @pytest.mark.parametrize("url", PRODUCTION_URLS_THE_HEURISTIC_ACCEPTED)
    def test_the_url_is_not_even_an_input_any_more(self, url):
        """The regression: these URLs all satisfied ``is_local_rpc``.

        ``resolve_managed_fork`` no longer accepts a URL at all, so the class of
        defect is closed structurally rather than by a check that could be
        weakened later.
        """
        from almanak.framework.execution.simulator.config import is_local_rpc

        assert is_local_rpc(url) is True, "fixture no longer reproduces the old heuristic"
        with pytest.raises(TypeError):
            resolve_managed_fork(None, url)  # type: ignore[call-arg]


class TestSignalThreading:
    """The declaration must actually reach the guards, or the fix is inert."""

    @pytest.mark.parametrize("declared", [True, False, None])
    def test_compiler_config_reaches_the_connector_context(self, declared):
        """``IntentCompilerConfig.managed_fork`` → ``BaseCompilerContext.managed_fork``."""
        from almanak.framework.intents.compiler import IntentCompiler, IntentCompilerConfig

        compiler = IntentCompiler(
            chain="arbitrum",
            rpc_url="https://arb.example.invalid",
            config=IntentCompilerConfig(allow_placeholder_prices=True, managed_fork=declared),
        )
        kwargs = compiler._base_compiler_context_kwargs(resolve_rpc_url=False)
        assert kwargs["managed_fork"] is declared

    def test_compiler_config_defaults_to_undeclared(self):
        from almanak.framework.intents.compiler import IntentCompilerConfig

        assert IntentCompilerConfig(allow_placeholder_prices=True).managed_fork is None

    @pytest.mark.parametrize("declared", [True, False, None])
    def test_orchestrator_forwards_its_declaration_to_the_refresh(self, declared):
        """``ExecutionOrchestrator(managed_fork=...)`` → ``refresh_deferred_bundle``."""
        from almanak.framework.execution.orchestrator import ExecutionOrchestrator

        orchestrator = ExecutionOrchestrator(
            signer=MagicMock(),
            submitter=MagicMock(),
            simulator=MagicMock(),
            chain="arbitrum",
            rpc_url="http://127.0.0.1:8545",
            managed_fork=declared,
        )
        assert orchestrator.managed_fork is declared

    def test_orchestrator_defaults_to_undeclared(self):
        from almanak.framework.execution.orchestrator import ExecutionOrchestrator

        orchestrator = ExecutionOrchestrator(
            signer=MagicMock(), submitter=MagicMock(), simulator=MagicMock(), chain="arbitrum"
        )
        assert orchestrator.managed_fork is None

    def test_permission_discovery_declares_production(self):
        """Manifest discovery is offline and deterministic — it must never probe.

        Asserts the constructed compiler, not the module's source text. A
        ``"managed_fork=False" in inspect.getsource(...)`` check matches a
        comment or an unrelated branch just as happily as the real call, and
        breaks on ``managed_fork = False`` spacing — it would have gone green
        for a discovery lane that declared nothing at all.
        """
        from almanak.framework.permissions.discovery import _CompilerCache

        compiler = _CompilerCache(chain="arbitrum", rpc_url=None).get("uniswap_v3")
        kwargs = compiler._base_compiler_context_kwargs(resolve_rpc_url=False)

        assert kwargs["managed_fork"] is False
        # ...and the consequence that matters: it resolves to production, so a
        # discovery run cannot relax a guard.
        assert resolve_managed_fork(kwargs["managed_fork"]) is False


def test_money_path_guards_do_not_import_the_url_heuristic():
    """Regression guard: the converted guards must not drift back to ``is_local_rpc``.

    ``is_local_rpc`` remains correct for simulation-vendor selection, so it is
    not deleted — which means nothing but this test stops a future edit from
    reintroducing the bypass in one of these four files.
    """
    from pathlib import Path

    repo_root = Path(fork_signal.__file__).resolve().parents[3]
    money_path_modules = (
        "almanak/connectors/uniswap_v3/compiler.py",
        "almanak/connectors/uniswap_v4/adapter.py",
        "almanak/connectors/traderjoe_v2/compiler.py",
        "almanak/connectors/enso/deferred_refresh_provider.py",
    )
    offenders = []
    for rel in money_path_modules:
        path = repo_root / rel
        assert path.exists(), f"{rel} moved — re-scope this ALM-3184 guard"
        source = path.read_text(encoding="utf-8")
        # Substring, not import-shape: a late `from ... import is_local_rpc`
        # inside a function is exactly how the bypass got in the first place.
        if "is_local_rpc" in source:
            offenders.append(rel)

    assert not offenders, (
        f"{offenders} reference is_local_rpc, a URL heuristic that returns True for any host "
        "on port 8545-8550. Money-path guards must use resolve_managed_fork (ALM-3184)."
    )
