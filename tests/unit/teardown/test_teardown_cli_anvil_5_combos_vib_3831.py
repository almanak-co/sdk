"""Regression guards for VIB-3831 (BUG-39 residual on 5 teardown combos).

After VIB-3705 (swap-only no-position misclassification) and VIB-3719
(amount='all' zero-balance branch) merged, the April-31 verification harness
still observed teardown CLI ``exit-1`` on five chain/protocol combos:

| Strategy                          | Chain     | Protocol family    |
| --------------------------------- | --------- | ------------------ |
| pancakeswap_v3_swap_bsc           | bsc       | PancakeSwap V3     |
| pancakeswap_v3_swap_arbitrum      | arbitrum  | PancakeSwap V3     |
| velodrome_aave_optimism           | optimism  | Velodrome + Aave   |
| curve_lp_lifecycle_arbitrum       | arbitrum  | Curve LP           |
| traderjoe_lp_sweep_avalanche      | avalanche | TraderJoe V2 LB LP |

VIB-3819 then closed the **teardown-CLI-wide** regression that the
``almanak strat teardown --network anvil`` path was constructing
``ManagedGateway()`` *without* ``anvil_chains`` / ``wallet_address`` /
``anvil_funding``, so the fork never started. Per the QA project memory:
"Worth a sweep of the QA-PostFixes April31 backlog after the fix lands —
every 'exit-1 with no clear cause' teardown report from that window may
auto-resolve."

This guard pins that the same VIB-3819 fix path covers all five chains: any
``--network anvil`` teardown for ``{bsc, arbitrum, optimism, avalanche}``
must produce a ``ManagedGateway(anvil_chains=[<chain>], ...)`` construction.
If the QA harness re-run still surfaces real residuals after VIB-3819, those
will be per-protocol teardown wiring bugs (compiler returning empty
ActionBundle, etc.) — file as separate tickets at that point.
"""

from __future__ import annotations

import importlib
import json
from unittest.mock import MagicMock

import pytest
from click.testing import CliRunner

teardown_cli_module = importlib.import_module("almanak.framework.cli.teardown")


VIB_3831_COMBOS = [
    pytest.param("bsc", id="pancakeswap_v3_swap_bsc"),
    pytest.param("arbitrum", id="pancakeswap_v3_swap_arbitrum"),
    pytest.param("optimism", id="velodrome_aave_optimism"),
    pytest.param("arbitrum", id="curve_lp_lifecycle_arbitrum"),
    pytest.param("avalanche", id="traderjoe_lp_sweep_avalanche"),
]


@pytest.fixture
def cli_runner() -> CliRunner:
    return CliRunner()


class _FakeGatewayClient:
    def __init__(self, _config) -> None:
        self.connected = False
        self.channel = None

    def connect(self) -> None:
        self.connected = True

    def health_check(self) -> bool:
        return True

    def disconnect(self) -> None:
        self.connected = False


class _SwapOnlyStrategy:
    STRATEGY_NAME = "vib_3831_probe"

    def __init__(self, config, chain: str, wallet_address: str) -> None:
        self.config = config
        self.chain = chain
        self.wallet_address = wallet_address
        self.strategy_id = "vib_3831_probe"

    def get_open_positions(self):
        from types import SimpleNamespace

        return SimpleNamespace(positions=[])

    def create_market_snapshot(self):
        from types import SimpleNamespace

        return SimpleNamespace(get_price_oracle_dict=lambda: {})

    def generate_teardown_intents(self, _mode, market=None):
        return []


def _write_strategy_files(tmp_path, chain: str) -> tuple[str, str]:
    strategy_file = tmp_path / "strategy.py"
    strategy_file.write_text("# placeholder — load_strategy_from_file is monkeypatched\n")

    config_file = tmp_path / "config.json"
    config_file.write_text(
        json.dumps(
            {
                "chain": chain,
                "wallet_address": "0x0000000000000000000000000000000000000001",
                "strategy_id": "vib_3831_probe",
                "anvil_funding": {"WETH": 1, "USDC": 1000, "ETH": 1},
            }
        )
    )
    return str(strategy_file), str(config_file)


@pytest.mark.parametrize("chain", VIB_3831_COMBOS)
def test_teardown_cli_anvil_passes_anvil_chains_for_5_combos(
    chain: str,
    cli_runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    """The VIB-3819 teardown-CLI fork-boot fix must apply to every chain that
    surfaced as a PARTIAL combo in VIB-3831. If a chain ever stops getting
    ``anvil_chains=[<chain>]`` from the CLI, this test fails per-combo so
    we know exactly which one regressed.
    """
    _, config_file = _write_strategy_files(tmp_path, chain)

    monkeypatch.setattr(
        teardown_cli_module,
        "load_strategy_from_file",
        lambda _path: (_SwapOnlyStrategy, None),
    )
    monkeypatch.setattr(
        "almanak.framework.gateway_client.GatewayClient",
        _FakeGatewayClient,
    )
    monkeypatch.setattr(
        "almanak.framework.data.tokens.resolver.TokenResolver.set_gateway_channel",
        lambda _self, _channel: None,
    )

    fake_managed_gateway_instance = MagicMock()
    fake_managed_gateway_instance.start = MagicMock(return_value=None)
    fake_managed_gateway_instance.stop = MagicMock(return_value=None)
    managed_gateway_ctor = MagicMock(return_value=fake_managed_gateway_instance)

    monkeypatch.setattr("almanak.gateway.managed.ManagedGateway", managed_gateway_ctor)
    monkeypatch.setattr(
        "almanak.gateway.managed.find_available_gateway_port",
        lambda _host, port: port,
    )

    result = cli_runner.invoke(
        teardown_cli_module.teardown,
        [
            "execute",
            "-d",
            str(tmp_path),
            "-c",
            config_file,
            "--network",
            "anvil",
            "--mode",
            "graceful",
            "--force",
        ],
    )

    assert result.exit_code == 0, result.output

    assert managed_gateway_ctor.called, "ManagedGateway was never instantiated"
    _, kwargs = managed_gateway_ctor.call_args
    assert kwargs.get("anvil_chains") == [chain], (
        f"VIB-3831 regression for chain={chain!r}: ManagedGateway must receive "
        f"anvil_chains=[{chain!r}] for --network anvil teardown to actually start "
        f"the Anvil fork. Got anvil_chains={kwargs.get('anvil_chains')!r}. Without "
        f"this, every teardown CLI invocation against {chain} returns exit-1 with "
        f"no clear cause (the BUG-39 residual symptom)."
    )
    assert kwargs.get("wallet_address") == "0x0000000000000000000000000000000000000001"
    assert kwargs.get("anvil_funding") == {"WETH": 1, "USDC": 1000, "ETH": 1}
