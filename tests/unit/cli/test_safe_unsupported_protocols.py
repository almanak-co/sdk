"""Safe-mode runs refuse connectors that declare ``safe_supported=False``."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

import pytest

from almanak.framework.cli import _run_components
from almanak.framework.cli._run_components import _build_runtime_config
from almanak.framework.execution.safe_compatibility import safe_unsupported_protocol_error


def test_eoa_only_protocol_is_named_in_the_error() -> None:
    error = safe_unsupported_protocol_error({"bsc": ["uniswap_v3", "aster_perps"]})
    assert error is not None and "aster_perps" in error


@pytest.mark.parametrize("protocols", [{"bsc": ["uniswap_v3"]}, {}, None])
def test_safe_capable_protocols_pass(protocols: object) -> None:
    assert safe_unsupported_protocol_error(protocols) is None


def _run(is_safe_mode: bool) -> None:
    runtime_config = SimpleNamespace(is_safe_mode=is_safe_mode, execution_address="0x" + "11" * 20)
    with (
        patch.object(_run_components, "_resolve_runtime_private_key_kwarg", return_value=None),
        patch.object(_run_components, "_resolve_effective_signing_key", return_value="0xkey"),
        patch.object(_run_components, "_load_local_runtime_config", return_value=runtime_config),
        patch.object(_run_components, "_register_chain_wallets", side_effect=AssertionError("reached registration")),
    ):
        _build_runtime_config(
            no_gateway=True,
            multi_chain=False,
            resolved_network="mainnet",
            config_chain="bsc",
            strategy_chains=["bsc"],
            strategy_protocols={"bsc": ["aster_perps"]},
            gateway_client=None,
            strategy_config={},
        )


def test_safe_mode_run_with_eoa_only_protocol_exits_before_wallet_registration() -> None:
    with pytest.raises(SystemExit):
        _run(is_safe_mode=True)


def test_eoa_run_with_eoa_only_protocol_proceeds() -> None:
    with pytest.raises(AssertionError, match="reached registration"):
        _run(is_safe_mode=False)
