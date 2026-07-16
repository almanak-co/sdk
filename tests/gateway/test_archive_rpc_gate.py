"""The pre-fork archive gate must FAIL, not warn (VIB-5869 / ALM-2695).

Before VIB-5869 this was a `logger.warning`: the fork started, ran, dispatched
real intents, and then died on `missing trie node` deep inside an iteration —
unrecoverable, and with no message connecting it to the missing archive RPC.
A fork that cannot survive its own first cold read must not start.
"""

from __future__ import annotations

import pytest

from almanak.core.chains import ChainRegistry
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.managed import ManagedGateway


def _rpc_env_vars() -> list[str]:
    """Every env var the gate consults when deciding "is an archive RPC set?".

    Derived from the registry rather than hand-listed: a hand-listed version of
    this fixture silently passed for `bsc` while `base` and `ethereum` kept
    reading a developer's real .env, i.e. the test was measuring the local
    machine, not the gate.
    """
    names = ["ALCHEMY_API_KEY", "ALMANAK_GATEWAY_ALCHEMY_API_KEY", "RPC_URL", "ALMANAK_RPC_URL"]
    names.append("ALMANAK_ALLOW_PRUNED_FORK_RPC")
    for chain in ChainRegistry.names():
        upper = chain.upper()
        names += [f"{upper}_RPC_URL", f"ALMANAK_{upper}_RPC_URL"]
    names += ["BNB_RPC_URL", "ALMANAK_BNB_RPC_URL"]  # bsc alias
    return names


@pytest.fixture
def no_rpc_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """A user with no archive RPC configured at all — the ALM-2695 setup."""
    for var in _rpc_env_vars():
        monkeypatch.delenv(var, raising=False)


def _gateway(chains: list[str], **kwargs) -> ManagedGateway:
    return ManagedGateway(GatewaySettings(), anvil_chains=chains, **kwargs)


class TestGateFailsFast:
    @pytest.mark.parametrize(
        "chain",
        # ALM-2695 chains (newly flagged) + the legacy set, which must keep
        # gating after VIB-5869 turned the warning into a failure.
        ["bsc", "arbitrum", "base", "optimism", "ethereum", "polygon", "avalanche", "zerog", "xlayer"],
    )
    def test_unconfigured_archive_chain_raises(self, chain: str, no_rpc_env: None) -> None:
        with pytest.raises(RuntimeError, match="Refusing to start Anvil fork"):
            _gateway([chain])._check_archive_rpc_availability()

    def test_error_names_the_chain_and_the_remedy(self, no_rpc_env: None) -> None:
        """The old warning left users with 'missing trie node' and no lead.
        The error must say which chain, how little state it serves, and what
        to actually do about it."""
        with pytest.raises(RuntimeError) as exc:
            _gateway(["bsc"])._check_archive_rpc_availability()
        msg = str(exc.value)
        assert "bsc" in msg
        assert "ALCHEMY_API_KEY" in msg
        assert "BSC_RPC_URL" in msg
        assert "missing trie node" in msg  # links the symptom to the cause
        assert "Retrying cannot help" in msg  # kills the "Retry test now" reflex

    def test_error_quotes_the_measured_window(self, no_rpc_env: None) -> None:
        with pytest.raises(RuntimeError) as exc:
            _gateway(["bsc"])._check_archive_rpc_availability()
        assert "bsc-rpc.publicnode.com" in str(exc.value)
        assert "of state" in str(exc.value)

    def test_all_failing_chains_reported_at_once(self, no_rpc_env: None) -> None:
        """Fix-one-rerun-hit-the-next is a bad loop; report every offender."""
        with pytest.raises(RuntimeError) as exc:
            _gateway(["bsc", "arbitrum"])._check_archive_rpc_availability()
        assert "bsc" in str(exc.value)
        assert "arbitrum" in str(exc.value)


class TestGatePasses:
    def test_alchemy_key_satisfies_the_gate(self, no_rpc_env: None, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ALCHEMY_API_KEY", "test-key")
        _gateway(["bsc", "arbitrum"])._check_archive_rpc_availability()  # must not raise

    @pytest.mark.parametrize("var", ["POLYGON_RPC_URL", "ALMANAK_POLYGON_RPC_URL"])
    def test_chain_specific_rpc_satisfies_the_gate(
        self, var: str, no_rpc_env: None, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(var, "https://archive.example/polygon")
        _gateway(["polygon"])._check_archive_rpc_availability()  # must not raise

    @pytest.mark.parametrize("var", ["RPC_URL", "ALMANAK_RPC_URL"])
    def test_generic_rpc_satisfies_the_gate(self, var: str, no_rpc_env: None, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(var, "https://archive.example")
        _gateway(["bsc"])._check_archive_rpc_availability()  # must not raise

    def test_gateway_prefixed_alchemy_satisfies_the_gate(
        self, no_rpc_env: None, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Codex finding: the gate must see the SAME creds the fork path uses.
        A deployer that injects only ``ALMANAK_GATEWAY_ALCHEMY_API_KEY`` (the
        prefixed form ``gateway_prefixed_or_bare`` resolves) configured Alchemy;
        the gate must not false-positive-block it."""
        monkeypatch.setenv("ALMANAK_GATEWAY_ALCHEMY_API_KEY", "test-key")
        _gateway(["bsc", "arbitrum", "base"])._check_archive_rpc_availability()  # must not raise

    def test_unflagged_chain_is_not_gated(self, no_rpc_env: None) -> None:
        """Monad serves ~11.5 days of state — gating it would be false friction."""
        _gateway(["monad"])._check_archive_rpc_availability()  # must not raise

    def test_external_anvil_is_not_gated(self, no_rpc_env: None) -> None:
        """The operator owns an external fork's upstream RPC; not our call."""
        gw = _gateway(["bsc"], external_anvil_ports={"bsc": 8545})
        gw._check_archive_rpc_availability()  # must not raise


class TestEscapeHatch:
    # The core safety property: only genuine opt-in values disable the gate.
    # gemini finding — a naive `if env_value(var):` truthy-string check treats
    # "0"/"false" as enabled, silently disabling the gate for the one operator
    # who set the var explicitly to keep it ON. That is the inverse of intent.
    ENABLES = ["1", "true", "TRUE", "True", "yes", "on", " on ", "\t1\n"]
    KEEPS_GATE_ON = ["0", "false", "False", "no", "off", "", "  ", "2", "disable", "nope"]

    @pytest.mark.parametrize("value", ENABLES)
    def test_truthy_values_downgrade_to_warning(
        self, value: str, no_rpc_env: None, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        monkeypatch.setenv("ALMANAK_ALLOW_PRUNED_FORK_RPC", value)
        with caplog.at_level("WARNING"):
            _gateway(["bsc"])._check_archive_rpc_availability()  # must not raise
        assert "bsc" in caplog.text
        assert "bypassing the archive-RPC gate" in caplog.text  # loud + names the bypass
        assert "missing trie node" in caplog.text  # names the failure mode
        assert "48s" in caplog.text or "of state" in caplog.text  # names the measured window

    @pytest.mark.parametrize("value", KEEPS_GATE_ON)
    def test_falsey_values_keep_gate_on(self, value: str, no_rpc_env: None, monkeypatch: pytest.MonkeyPatch) -> None:
        """`0` / `false` / `""` / anything-not-opt-in must NOT bypass a safety gate."""
        monkeypatch.setenv("ALMANAK_ALLOW_PRUNED_FORK_RPC", value)
        with pytest.raises(RuntimeError, match="Refusing to start Anvil fork"):
            _gateway(["bsc"])._check_archive_rpc_availability()
