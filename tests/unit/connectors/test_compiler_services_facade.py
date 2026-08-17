"""Characterization tests for the shared connector compiler-services facade."""

from types import SimpleNamespace
from unittest.mock import Mock, sentinel

import pytest

from almanak.connectors._strategy_base.base.compiler import CompilerServicesFacadeMixin
from almanak.connectors.aave_v3.compiler import _LendingCompilerAdapter as AaveCompilerAdapter
from almanak.connectors.aerodrome.compiler import _AerodromeCompileImpl
from almanak.connectors.benqi.compiler import _LendingCompilerAdapter as BenqiCompilerAdapter
from almanak.connectors.compound_v3.compiler import _LendingCompilerAdapter as CompoundCompilerAdapter
from almanak.connectors.curvance.compiler import _LendingCompilerAdapter as CurvanceCompilerAdapter
from almanak.connectors.euler_v2.compiler import _LendingCompilerAdapter as EulerCompilerAdapter
from almanak.connectors.morpho_blue.compiler import _LendingCompilerAdapter as MorphoCompilerAdapter
from almanak.connectors.silo_v2.compiler import _LendingCompilerAdapter as SiloCompilerAdapter
from almanak.connectors.spark.compiler import _LendingCompilerAdapter as SparkCompilerAdapter
from almanak.connectors.traderjoe_v2.compiler import _TraderJoeV2CompileImpl


@pytest.mark.parametrize(
    "adapter_type",
    [
        AaveCompilerAdapter,
        _AerodromeCompileImpl,
        BenqiCompilerAdapter,
        CompoundCompilerAdapter,
        CurvanceCompilerAdapter,
        EulerCompilerAdapter,
        MorphoCompilerAdapter,
        SiloCompilerAdapter,
        SparkCompilerAdapter,
        _TraderJoeV2CompileImpl,
    ],
)
def test_migrated_compilers_share_the_services_facade(adapter_type: type) -> None:
    assert issubclass(adapter_type, CompilerServicesFacadeMixin)


def test_facade_preserves_legacy_attributes_and_delegates_services() -> None:
    services = Mock()
    services.resolve_token.return_value = sentinel.token
    services.eth_call.return_value = "0x01"
    ctx = SimpleNamespace(
        chain="base",
        wallet_address="0x" + "1" * 40,
        rpc_url="https://rpc.example",
        rpc_timeout=12.5,
        price_oracle=sentinel.oracle,
        default_deadline_seconds=300,
        gateway_client=sentinel.gateway,
        token_resolver=sentinel.resolver,
        gateway_internal_preflight=True,
        services=services,
    )

    facade = CompilerServicesFacadeMixin(ctx)

    assert facade.chain == "base"
    assert facade.wallet_address == ctx.wallet_address
    assert facade._get_chain_rpc_url() == ctx.rpc_url
    assert facade._resolve_token("USDC") is sentinel.token
    assert facade.eth_call("0xpool", "0xdata", chain="base") == "0x01"
    assert facade._eth_call("0xpool", "0xdata") == "0x01"
    services.resolve_token.assert_called_once_with("USDC")
    services.eth_call.assert_any_call("0xpool", "0xdata", chain="base")
    services.eth_call.assert_any_call("0xpool", "0xdata", chain=None)
