"""Tests for `strat run` network-resolution logic (see almanak/framework/cli/run.py).

The `run()` command resolves `network` / `gateway_network` before branching into
managed vs pre-existing gateway paths. This unit mirrors that resolution as a
pure function so the invariants can be asserted without spinning up Click +
gateway + Anvil plumbing.
"""

from __future__ import annotations


def _resolve_network(
    *,
    anvil_ports: tuple[str, ...],
    network: str | None,
    no_gateway: bool,
) -> tuple[str | None, str]:
    """Replicate the resolution block at almanak/framework/cli/run.py (top of run()).

    Returns the tuple (resolved_network, gateway_network) so callers can assert
    both values.
    """
    if anvil_ports and not network and not no_gateway:
        network = "anvil"
    gateway_network = network or "mainnet"
    return network, gateway_network


class TestRunNetworkResolution:
    def test_managed_mode_with_anvil_ports_infers_anvil(self):
        network, gateway_network = _resolve_network(
            anvil_ports=("arbitrum=8545",),
            network=None,
            no_gateway=False,
        )
        assert network == "anvil"
        assert gateway_network == "anvil"

    def test_managed_mode_with_explicit_network_not_overridden(self):
        network, gateway_network = _resolve_network(
            anvil_ports=("arbitrum=8545",),
            network="mainnet",
            no_gateway=False,
        )
        assert network == "mainnet"
        assert gateway_network == "mainnet"

    def test_managed_mode_no_anvil_ports_defaults_to_mainnet(self):
        network, gateway_network = _resolve_network(
            anvil_ports=(),
            network=None,
            no_gateway=False,
        )
        assert network is None
        assert gateway_network == "mainnet"

    def test_no_gateway_with_anvil_ports_does_not_auto_infer(self):
        """`--no-gateway` takes priority; anvil inference must not fire.

        (`--anvil-port` combined with `--no-gateway` is rejected downstream,
        but the resolution block runs first and must not silently coerce
        `network` to `"anvil"`.)
        """
        network, gateway_network = _resolve_network(
            anvil_ports=("arbitrum=8545",),
            network=None,
            no_gateway=True,
        )
        assert network is None
        assert gateway_network == "mainnet"

    def test_no_gateway_with_explicit_anvil_network_preserved(self):
        """`--no-gateway --network anvil` should propagate `anvil` through both outputs."""
        network, gateway_network = _resolve_network(
            anvil_ports=(),
            network="anvil",
            no_gateway=True,
        )
        assert network == "anvil"
        assert gateway_network == "anvil"
