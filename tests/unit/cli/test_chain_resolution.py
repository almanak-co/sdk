"""Unit tests for the dependency-light ``get_default_chain`` helper.

Issue #1703: extracted from ``almanak.framework.cli.run`` into the
minimal ``almanak.framework.cli.chain_resolution`` module so sweep
workers can import it without pulling the heavy CLI deps tree.
"""

from __future__ import annotations

import types

from almanak.framework.cli.chain_resolution import get_default_chain


class TestGetDefaultChain:
    def test_returns_default_chain_from_metadata(self) -> None:
        metadata = types.SimpleNamespace(default_chain="base", supported_chains=["base", "optimism"])

        class S:
            STRATEGY_METADATA = metadata

        assert get_default_chain(S) == "base"

    def test_falls_back_to_supported_chains_first(self) -> None:
        metadata = types.SimpleNamespace(default_chain=None, supported_chains=["arbitrum", "optimism"])

        class S:
            STRATEGY_METADATA = metadata

        assert get_default_chain(S) == "arbitrum"

    def test_legacy_supported_chains_attribute(self) -> None:
        class S:
            SUPPORTED_CHAINS = ["polygon", "base"]

        assert get_default_chain(S) == "polygon"

    def test_final_fallback_is_arbitrum(self) -> None:
        class S:
            pass

        assert get_default_chain(S) == "arbitrum"

    def test_metadata_with_falsy_values_falls_through(self) -> None:
        """Empty default_chain + empty supported_chains should not crash."""
        metadata = types.SimpleNamespace(default_chain="", supported_chains=[])

        class S:
            STRATEGY_METADATA = metadata

        assert get_default_chain(S) == "arbitrum"


class TestChainResolutionIsImportCheap:
    def test_module_adds_no_gateway_imports_beyond_framework_baseline(self) -> None:
        """The module must not re-introduce the ``run.py`` dependency chain.

        Issue #1703 context: ``almanak.framework.cli.run`` pulls in the gateway
        tree (balance providers, price oracles, etc.) and the full indicator
        set. In parallel-sweep workers those imports were paid *per subprocess*
        via a lazy ``from ..run import get_default_chain`` inside the worker
        function. This module was extracted precisely so workers can import
        just the chain-resolution helper without dragging in the gateway tree.

        Mechanism: spawn a clean subprocess, measure what importing
        ``almanak.framework.cli.chain_resolution`` adds to ``sys.modules``
        *on top of* the already-paid ``almanak.framework`` baseline, and
        reject any gateway modules that appear in that delta.

        Why a subprocess: mutating ``sys.modules`` in-process (``del`` +
        re-import, or ``importlib.reload``) rebinds the function object and
        breaks ``TestRunReExportStillWorks`` below — ``run.get_default_chain``
        is a direct binding to the original function.
        """
        import subprocess
        import sys
        import textwrap

        probe = textwrap.dedent(
            """
            import sys

            # Baseline: everything `almanak.framework` itself already pays for.
            # `chain_resolution`'s job is to not add anything on top of this.
            import almanak.framework  # noqa: F401
            baseline = set(sys.modules)

            import almanak.framework.cli.chain_resolution  # noqa: F401
            delta = set(sys.modules) - baseline

            gateway_added = sorted(m for m in delta if m.startswith("almanak.gateway."))
            if gateway_added:
                print("GATEWAY_ADDED:" + ",".join(gateway_added))
                sys.exit(1)
            """
        )
        result = subprocess.run(
            [sys.executable, "-c", probe],
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert result.returncode == 0, (
            "chain_resolution.py now pulls gateway modules on top of the "
            "almanak.framework baseline; the whole point of #1703 was to keep "
            "this module cheap enough that parallel-sweep workers don't pay "
            "the run.py import tree per subprocess.\n"
            f"stdout: {result.stdout}\nstderr: {result.stderr}"
        )


class TestRunReExportStillWorks:
    def test_get_default_chain_importable_from_run(self) -> None:
        """Back-compat: `almanak.framework.cli.run.get_default_chain` must
        continue to resolve to the extracted function (#1703)."""
        from almanak.framework.cli import chain_resolution, run

        assert run.get_default_chain is chain_resolution.get_default_chain
