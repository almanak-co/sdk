"""The chain-truth gate must fail on each defect class it claims to catch.

VIB-6231. ``scripts/ci/check_chain_truth_agreement.py`` runs three checks. A
gate that cannot be shown to fail on its own motivating defect is not a gate, so
each check here gets a negative control that reintroduces the exact shape it
exists to catch, rather than only asserting the tree is currently clean.

The motivating defects, all real and all fixed in the same change:

* ``lido`` accepted ``protocols={"arbitrum": ["lido"]}`` at config validation and
  compilation then answered ``LidoCompiler only supported on ethereum``.
* ``pancakeswap_v3`` on ``linea`` was published by the catalogue and compiled
  fine, but config validation rejected it, so the strategy could not load.
* ``gnosis`` and ``zksync`` appeared in the published chain axis via
  ``strategy_matrix_entries`` even though ``resolve_chain_name`` rejects them.
"""

from __future__ import annotations

import pytest

from scripts.ci.check_chain_truth_agreement import (
    CONFIG_ONLY,
    MATRIX_ONLY,
    Disagreement,
    find_disagreements,
    find_unregistered_chain_declarations,
    load_waivers,
)


class TestTheTreeIsClean:
    """The live repo satisfies the invariant (modulo the recorded waivers)."""

    def test_no_unwaived_disagreements(self) -> None:
        disagreements, _unmapped, compared = find_disagreements()
        # Guard against a vacuous pass: "0 unwaived" is only meaningful if the
        # comparison actually had pairs to compare.
        assert compared > 0, "compared zero (protocol, chain) pairs -- the gate inspected nothing"
        live = [d for d in disagreements if d not in load_waivers()]
        assert live == [], [d.describe() for d in live]

    def test_no_unregistered_chains_at_all(self) -> None:
        """No waiver: every unresolvable chain was removed, not excused.

        A hardcoded exemption here would defeat the invariant the gate exists to
        state -- flagged independently by two auditors on PR #3500.
        """
        live = find_unregistered_chain_declarations()
        assert live == [], (
            f"manifest declares chain(s) `resolve_chain_name` rejects: {live}. "
            "Anything published here is unusable by construction."
        )

    def test_lido_agrees_across_surfaces(self) -> None:
        """The specific pair that motivated the gate."""
        disagreements, _unmapped, _ = find_disagreements()
        assert not [d for d in disagreements if d.protocol == "lido"]

    def test_pancakeswap_v3_linea_is_not_advertised(self) -> None:
        """Linea is intentionally absent from the proven unified declaration."""
        from almanak.framework.execution.config import SUPPORTED_PROTOCOLS

        assert "linea" not in SUPPORTED_PROTOCOLS["pancakeswap_v3"]


class TestConfigValidationResolvesProtocolAliases:
    """`_validate_protocols` must answer the same question the compiler does."""

    def test_uniswap_v3_on_mantle_is_accepted(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """``uniswap_v3`` on mantle IS Agni Finance, and it compiles.

        Before the fix this was rejected as "not available on chain mantle"
        while the compiler happily normalised it to ``agni_finance``.
        """
        from almanak.framework.execution.config import MultiChainRuntimeConfig

        monkeypatch.setenv("ALMANAK_MANTLE_RPC_URL", "http://127.0.0.1:8545")
        config = MultiChainRuntimeConfig(
            chains=["mantle"],
            protocols={"mantle": ["uniswap_v3"]},
            private_key="0x" + "11" * 32,
        )
        # The stored spelling is deliberately NOT canonicalised -- downstream
        # readers of ``config.protocols`` see what the user wrote.
        assert config.protocols == {"mantle": ["uniswap_v3"]}

    def test_a_genuinely_unavailable_pair_is_still_rejected(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Negative control: alias resolution must not open the gate.

        ``lido`` on arbitrum is the motivating defect -- it must now be refused
        at config load, not accepted and refused later by the compiler.
        """
        from almanak.config.runtime import ConfigurationError
        from almanak.framework.execution.config import MultiChainRuntimeConfig

        monkeypatch.setenv("ALMANAK_ARBITRUM_RPC_URL", "http://127.0.0.1:8545")
        with pytest.raises(ConfigurationError, match="not available on chain"):
            MultiChainRuntimeConfig(
                chains=["arbitrum"],
                protocols={"arbitrum": ["lido"]},
                private_key="0x" + "11" * 32,
            )


class TestGateFailsOnAReintroducedDisagreement:
    """Negative controls for check 1, both directions."""

    def _disagreements_with_config(self, monkeypatch: pytest.MonkeyPatch, table: dict[str, set[str]]):
        import scripts.ci.check_chain_truth_agreement as gate

        monkeypatch.setattr(gate, "_published_chains_by_protocol", lambda: {"demo": {"ethereum"}})
        monkeypatch.setattr(gate, "_config_accepts", lambda p, c: c in table.get(p, set()))
        import almanak.framework.execution.config as cfg

        monkeypatch.setattr(cfg, "SUPPORTED_PROTOCOLS", table)
        return gate.find_disagreements()

    def test_config_only_pair_is_reported(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Config accepts a chain the catalogue never publishes (the lido shape)."""
        disagreements, _unmapped, compared = self._disagreements_with_config(
            monkeypatch, {"demo": {"ethereum", "arbitrum"}}
        )
        assert compared > 0
        assert Disagreement("demo", "arbitrum", CONFIG_ONLY) in disagreements

    def test_matrix_only_pair_is_reported(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Catalogue publishes a chain config rejects (the pancakeswap shape)."""
        disagreements, _unmapped, compared = self._disagreements_with_config(monkeypatch, {"demo": set()})
        assert compared > 0
        assert Disagreement("demo", "ethereum", MATRIX_ONLY) in disagreements

    def test_agreeing_surfaces_report_nothing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Positive control: the reporter is not simply always reporting."""
        disagreements, _unmapped, _ = self._disagreements_with_config(monkeypatch, {"demo": {"ethereum"}})
        assert disagreements == []


class TestGateFailsOnAnUnregisteredChain:
    """Negative control for check 3."""

    def test_phantom_chain_in_unified_support_is_reported(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import scripts.ci.check_chain_truth_agreement as gate

        class _Connector:
            name = "demo"
            all_supported_chains = ("ethereum", "definitely_not_a_chain")

        from almanak.connectors import _connector as connector_mod

        monkeypatch.setattr(connector_mod.CONNECTOR_REGISTRY, "all", lambda: (_Connector(),))
        rows = gate.find_unregistered_chain_declarations()
        assert ("demo", "supported_chains", "definitely_not_a_chain") in rows
        # The registered chain on the same connector must NOT be reported.
        assert not [r for r in rows if r[2] == "ethereum"]

    def test_registered_unified_support_reports_nothing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import scripts.ci.check_chain_truth_agreement as gate

        class _Connector:
            name = "demo"
            all_supported_chains = ("ethereum",)

        from almanak.connectors import _connector as connector_mod

        monkeypatch.setattr(connector_mod.CONNECTOR_REGISTRY, "all", lambda: (_Connector(),))
        assert gate.find_unregistered_chain_declarations() == []


class TestWaiverFileIsStrict:
    """A malformed waiver must raise, not silently match nothing.

    A waiver that matches nothing leaves a real disagreement unreported while
    looking deliberate -- the failure mode is a benign-looking pass.
    """

    def _write(self, tmp_path, body: str):
        path = tmp_path / "waivers.yml"
        path.write_text(body)
        return path

    def test_missing_ticket_raises(self, tmp_path) -> None:
        path = self._write(
            tmp_path, "waivers:\n  enso:\n    - chain: blast\n      direction: config-only\n      reason: x\n"
        )
        with pytest.raises(ValueError, match="missing"):
            load_waivers(path)

    def test_missing_reason_raises(self, tmp_path) -> None:
        path = self._write(
            tmp_path, "waivers:\n  enso:\n    - chain: blast\n      direction: config-only\n      ticket: VIB-1\n"
        )
        with pytest.raises(ValueError, match="missing"):
            load_waivers(path)

    def test_empty_ticket_raises(self, tmp_path) -> None:
        path = self._write(
            tmp_path,
            "waivers:\n  enso:\n    - chain: blast\n      direction: config-only\n      ticket: '  '\n      reason: x\n",
        )
        with pytest.raises(ValueError, match="empty ticket"):
            load_waivers(path)

    def test_bad_direction_raises(self, tmp_path) -> None:
        path = self._write(
            tmp_path,
            "waivers:\n  enso:\n    - chain: blast\n      direction: sideways\n      ticket: VIB-1\n      reason: x\n",
        )
        with pytest.raises(ValueError, match="direction"):
            load_waivers(path)

    def test_wellformed_waiver_parses(self, tmp_path) -> None:
        path = self._write(
            tmp_path,
            "waivers:\n  enso:\n    - chain: blast\n      direction: config-only\n      ticket: VIB-1\n      reason: x\n",
        )
        assert load_waivers(path) == {Disagreement("enso", "blast", CONFIG_ONLY)}

    def test_the_committed_waiver_file_parses(self) -> None:
        """The real file must satisfy its own schema."""
        assert isinstance(load_waivers(), set)
