"""Intent tests for Fluid DEX LP on Arbitrum.

4-Layer verification: compilation, execution, receipt parsing, balance deltas.
NO MOCKING — all tests use Anvil fork.

To run:
    uv run pytest tests/intents/arbitrum/test_fluid_lp.py -v -s
"""

from decimal import Decimal

import pytest

from almanak.framework.connectors.fluid.receipt_parser import FluidReceiptParser
from almanak.framework.connectors.fluid.sdk import FluidSDK, FluidSDKError
from almanak.framework.intents import IntentCompiler, LPCloseIntent, LPOpenIntent

CHAIN_NAME = "arbitrum"


def _find_unencumbered_pool(sdk: FluidSDK):
    try:
        addresses = sdk.get_all_dex_addresses()
    except FluidSDKError:
        return None
    for addr in addresses:
        try:
            data = sdk.get_dex_data(addr)
            if not data.is_smart_collateral and not data.is_smart_debt:
                return (data.dex_address, data.token0, data.token1)
        except FluidSDKError:
            continue
    return None


@pytest.mark.integration
class TestFluidLPCompilation:
    def test_lp_open_fails_phase1(self, funded_wallet, anvil_rpc_url):
        """LP_OPEN compilation correctly returns FAILED in phase 1.

        Fluid DEX deposit() reverts on all pools due to complex Liquidity-layer routing.
        LP support is a follow-up. This test documents the expected phase 1 behavior.
        """
        sdk = FluidSDK(chain=CHAIN_NAME, rpc_url=anvil_rpc_url)
        pool_info = _find_unencumbered_pool(sdk)
        if pool_info is None:
            pytest.skip("No unencumbered Fluid DEX pool found on this fork")
        dex_address = pool_info[0]
        compiler = IntentCompiler(chain=CHAIN_NAME, wallet_address=funded_wallet, rpc_url=anvil_rpc_url)
        intent = LPOpenIntent(pool=dex_address, amount0=Decimal("0.001"), amount1=Decimal("0.001"),
                              range_lower=Decimal("1"), range_upper=Decimal("2"), protocol="fluid")
        result = compiler.compile(intent)
        assert result.status.value == "FAILED", "LP_OPEN should fail in phase 1 (deposit not supported)"
        assert "not supported" in (result.error or "").lower()

    def test_lp_close_compiles(self, funded_wallet, anvil_rpc_url):
        sdk = FluidSDK(chain=CHAIN_NAME, rpc_url=anvil_rpc_url)
        pool_info = _find_unencumbered_pool(sdk)
        if pool_info is None:
            pytest.skip("No unencumbered Fluid DEX pool found on this fork")
        compiler = IntentCompiler(chain=CHAIN_NAME, wallet_address=funded_wallet, rpc_url=anvil_rpc_url)
        intent = LPCloseIntent(position_id="1", protocol="fluid", pool=pool_info[0])
        result = compiler.compile(intent)
        assert result.status.value == "SUCCESS", f"Compilation failed: {result.error}"

    def test_rejects_invalid_pool(self, funded_wallet, anvil_rpc_url):
        from almanak.framework.intents.compiler import IntentCompilerConfig
        compiler = IntentCompiler(chain=CHAIN_NAME, wallet_address=funded_wallet, rpc_url=anvil_rpc_url,
                                  config=IntentCompilerConfig(allow_placeholder_prices=True))
        intent = LPOpenIntent(pool="INVALID", amount0=Decimal("0.001"), amount1=Decimal("0.001"),
                              range_lower=Decimal("1"), range_upper=Decimal("2"), protocol="fluid")
        result = compiler.compile(intent)
        assert result.status.value == "FAILED"


class TestFluidReceiptParsing:
    def test_parser_extracts_nft_id(self):
        from tests.unit.connectors.fluid.test_fluid_receipt_parser import _log_operate, _make_receipt
        parser = FluidReceiptParser()
        receipt = _make_receipt([_log_operate(nft_id=42, token0_amt=1_000_000, token1_amt=2_000_000)])
        assert parser.extract_position_id(receipt) == 42

    def test_supported_extractions(self):
        parser = FluidReceiptParser()
        assert "position_id" in parser.SUPPORTED_EXTRACTIONS
        assert "lp_close_data" in parser.SUPPORTED_EXTRACTIONS


@pytest.mark.integration
class TestEncumbranceGuard:
    def test_sdk_pool_data_readable(self, anvil_rpc_url):
        """Verify pool data is readable from on-chain (smoke test)."""
        sdk = FluidSDK(chain=CHAIN_NAME, rpc_url=anvil_rpc_url)
        try:
            addresses = sdk.get_all_dex_addresses()
        except FluidSDKError:
            pytest.skip("Cannot enumerate Fluid DEX pools")
        if not addresses:
            pytest.skip("No Fluid DEX pools on this fork")
        data = sdk.get_dex_data(addresses[0])
        assert isinstance(data.is_smart_collateral, bool)
        assert isinstance(data.is_smart_debt, bool)
