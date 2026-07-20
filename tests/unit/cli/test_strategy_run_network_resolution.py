"""Tests for `strat run` network-resolution logic.

The `run()` command resolves the gateway network before branching into managed
vs pre-existing gateway paths. This file pins the *flag / --anvil-port* half of
that ladder.

VIB-5920: the block these tests used to hand-replicate now lives in
``almanak.framework.cli._network_resolution.resolve_network``, so they call the
real resolver instead of a copy that could drift. Config-key precedence, hosted
mode, and invalid-value handling are covered in
``test_network_resolution_vib5920.py``.
"""

from __future__ import annotations

import pytest

from almanak.framework.cli._network_resolution import resolve_network


@pytest.fixture(autouse=True)
def _local_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ALMANAK_IS_HOSTED", raising=False)


def _resolve_network(
    *,
    anvil_ports: tuple[str, ...],
    network: str | None,
    no_gateway: bool,
) -> str:
    """Resolve the gateway network exactly as ``_setup_gateway`` does.

    No strategy config is in scope here, so resolution falls through to the
    ``"mainnet"`` default whenever neither the flag nor ``--anvil-port`` wins.
    """
    return resolve_network(
        flag_network=network,
        anvil_ports_present=bool(anvil_ports),
        no_gateway=no_gateway,
        strategy_config=None,
    ).network


class TestRunNetworkResolution:
    def test_managed_mode_with_anvil_ports_infers_anvil(self):
        assert _resolve_network(anvil_ports=("arbitrum=8545",), network=None, no_gateway=False) == "anvil"

    def test_managed_mode_with_explicit_network_not_overridden(self):
        assert _resolve_network(anvil_ports=("arbitrum=8545",), network="mainnet", no_gateway=False) == "mainnet"

    def test_managed_mode_no_anvil_ports_defaults_to_mainnet(self):
        assert _resolve_network(anvil_ports=(), network=None, no_gateway=False) == "mainnet"

    def test_no_gateway_with_anvil_ports_does_not_auto_infer(self):
        """`--no-gateway` takes priority; anvil inference must not fire.

        (`--anvil-port` combined with `--no-gateway` is rejected downstream,
        but the resolution block runs first and must not silently coerce
        the network to `"anvil"`.)
        """
        assert _resolve_network(anvil_ports=("arbitrum=8545",), network=None, no_gateway=True) == "mainnet"

    def test_no_gateway_with_explicit_anvil_network_preserved(self):
        """`--no-gateway --network anvil` should propagate `anvil`."""
        assert _resolve_network(anvil_ports=(), network="anvil", no_gateway=True) == "anvil"
