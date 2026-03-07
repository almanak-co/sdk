"""Tests for Monad chain configuration across the SDK."""

import pytest


class TestMonadChainEnum:
    """Monad is registered in both Chain enums."""

    def test_core_chain_enum(self):
        from almanak.core.enums import Chain

        assert Chain.MONAD.value == "MONAD"

    def test_execution_chain_enum(self):
        from almanak.framework.execution.interfaces import Chain

        assert Chain.MONAD.value == "monad"


class TestMonadChainConfig:
    """Monad chain configuration in execution config."""

    def test_chain_id(self):
        from almanak.framework.execution.config import CHAIN_IDS

        assert CHAIN_IDS["monad"] == 143

    def test_supported_on_uniswap_v3(self):
        from almanak.framework.execution.config import SUPPORTED_PROTOCOLS

        assert "monad" in SUPPORTED_PROTOCOLS["uniswap_v3"]


class TestMonadGasConstants:
    """Monad gas constants are configured."""

    def test_gas_buffer(self):
        from almanak.framework.execution.gas.constants import CHAIN_GAS_BUFFERS

        assert "monad" in CHAIN_GAS_BUFFERS
        assert CHAIN_GAS_BUFFERS["monad"] == 1.1

    def test_gas_price_cap(self):
        from almanak.framework.execution.gas.constants import CHAIN_GAS_PRICE_CAPS_GWEI

        assert "monad" in CHAIN_GAS_PRICE_CAPS_GWEI

    def test_tx_timeout(self):
        from almanak.framework.execution.gas.constants import CHAIN_TX_TIMEOUTS

        assert "monad" in CHAIN_TX_TIMEOUTS

    def test_grpc_timeout(self):
        from almanak.framework.execution.gas.constants import CHAIN_GRPC_EXECUTE_TIMEOUTS

        assert "monad" in CHAIN_GRPC_EXECUTE_TIMEOUTS

    def test_simulation_buffer(self):
        from almanak.framework.execution.gas.constants import CHAIN_SIMULATION_BUFFERS

        assert "monad" in CHAIN_SIMULATION_BUFFERS


class TestMonadContracts:
    """Monad contract addresses in the registry."""

    def test_uniswap_v3_addresses(self):
        from almanak.core.contracts import UNISWAP_V3

        assert "monad" in UNISWAP_V3
        monad = UNISWAP_V3["monad"]
        assert "swap_router" in monad
        assert "factory" in monad
        assert "position_manager" in monad
        assert "quoter_v2" in monad

    def test_uniswap_v3_tokens(self):
        from almanak.core.contracts import UNISWAP_V3_TOKENS

        assert "monad" in UNISWAP_V3_TOKENS
        tokens = UNISWAP_V3_TOKENS["monad"]
        assert "MON" in tokens
        assert "WMON" in tokens
        assert "WETH" in tokens
        assert "USDC" in tokens


class TestMonadTokenDefaults:
    """Monad tokens in the default token registry."""

    def test_wrapped_native(self):
        from almanak.framework.data.tokens.defaults import WRAPPED_NATIVE

        assert "monad" in WRAPPED_NATIVE
        assert WRAPPED_NATIVE["monad"] == "0x3bd359C1119dA7Da1D913D1C4D2B7c461115433A"

    def test_mon_token(self):
        from almanak.framework.data.tokens.defaults import MON, NATIVE_SENTINEL

        assert "monad" in MON.addresses
        assert MON.addresses["monad"] == NATIVE_SENTINEL
        assert MON.decimals == 18

    def test_wmon_token(self):
        from almanak.framework.data.tokens.defaults import WMON

        assert "monad" in WMON.addresses
        assert WMON.decimals == 18

    def test_usdc_on_monad(self):
        from almanak.framework.data.tokens.defaults import USDC

        assert "monad" in USDC.addresses
        assert USDC.addresses["monad"] == "0x754704Bc059F8C67012fEd69BC8A327a5aafb603"

    def test_weth_on_monad(self):
        from almanak.framework.data.tokens.defaults import WETH

        assert "monad" in WETH.addresses

    def test_wbtc_on_monad(self):
        from almanak.framework.data.tokens.defaults import WBTC

        assert "monad" in WBTC.addresses

    def test_token_resolver_resolves_monad_usdc(self):
        from almanak.framework.data.tokens import get_token_resolver

        resolver = get_token_resolver()
        token = resolver.resolve("USDC", "monad")
        assert token.decimals == 6
        assert token.address.lower() == "0x754704bc059f8c67012fed69bc8a327a5aafb603"


class TestMonadRPCConfig:
    """Monad RPC configuration."""

    def test_public_rpc_url(self):
        from almanak.gateway.utils.rpc_provider import PUBLIC_RPC_URLS

        assert "monad" in PUBLIC_RPC_URLS
        assert PUBLIC_RPC_URLS["monad"] == "https://rpc.monad.xyz"

    def test_anvil_port(self):
        from almanak.gateway.utils.rpc_provider import ANVIL_CHAIN_PORTS

        assert "monad" in ANVIL_CHAIN_PORTS
        assert ANVIL_CHAIN_PORTS["monad"] == 8555

    def test_alchemy_key(self):
        from almanak.gateway.utils.rpc_provider import ALCHEMY_CHAIN_KEYS

        assert "monad" in ALCHEMY_CHAIN_KEYS


class TestMonadPositionManager:
    """Monad position manager in Uniswap V3 receipt parser."""

    def test_position_manager_address(self):
        from almanak.framework.connectors.uniswap_v3.receipt_parser import POSITION_MANAGER_ADDRESSES

        assert "monad" in POSITION_MANAGER_ADDRESSES
