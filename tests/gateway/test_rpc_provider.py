"""Tests for RPC provider env var precedence and custom URL resolution."""

import asyncio
import os
from typing import Any
from unittest.mock import patch

import pytest
from web3 import AsyncWeb3
from web3.exceptions import ExtraDataLengthError
from web3.providers import AsyncBaseProvider
from web3.types import RPCEndpoint, RPCResponse

from almanak.gateway.utils.rpc_provider import (
    POA_CHAINS,
    NodeProvider,
    _get_custom_url,
    _has_chain_specific_url,
    _has_custom_url,
    _has_generic_url,
    get_rpc_url,
    has_api_key_configured,
    inject_poa_middleware,
    is_poa_chain,
)


@pytest.fixture(autouse=True)
def clean_env():
    """Clear all env vars to guarantee isolation from ambient RPC config."""
    with patch.dict(os.environ, {}, clear=True):
        yield


class TestCustomUrlPrecedence:
    """Test _get_custom_url env var precedence order."""

    def test_almanak_prefixed_chain_url_highest_priority(self):
        """ALMANAK_ARBITRUM_RPC_URL beats everything else."""
        with patch.dict(os.environ, {
            "ALMANAK_ARBITRUM_RPC_URL": "https://almanak-chain",
            "ARBITRUM_RPC_URL": "https://bare-chain",
            "ALMANAK_RPC_URL": "https://almanak-generic",
            "RPC_URL": "https://bare-generic",
        }):
            assert _get_custom_url("arbitrum") == "https://almanak-chain"

    def test_bare_chain_rpc_url_beats_generic(self):
        """ARBITRUM_RPC_URL beats ALMANAK_RPC_URL and RPC_URL."""
        with patch.dict(os.environ, {
            "ARBITRUM_RPC_URL": "https://bare-chain",
            "ALMANAK_RPC_URL": "https://almanak-generic",
            "RPC_URL": "https://bare-generic",
        }):
            assert _get_custom_url("arbitrum") == "https://bare-chain"

    def test_almanak_rpc_url_beats_bare_rpc_url(self):
        """ALMANAK_RPC_URL beats RPC_URL."""
        with patch.dict(os.environ, {
            "ALMANAK_RPC_URL": "https://almanak-generic",
            "RPC_URL": "https://bare-generic",
        }):
            assert _get_custom_url("arbitrum") == "https://almanak-generic"

    def test_bare_rpc_url_is_lowest_custom_priority(self):
        """RPC_URL is used when no other custom env var is set."""
        with patch.dict(os.environ, {"RPC_URL": "https://bare-generic"}):
            assert _get_custom_url("arbitrum") == "https://bare-generic"

    def test_no_custom_url_raises(self):
        """ValueError raised when no custom URL env var is set."""
        with pytest.raises(ValueError, match="No custom RPC URL found"):
            _get_custom_url("arbitrum")


class TestBscBnbAlias:
    """Test that bsc/bnb aliases work interchangeably."""

    def test_bsc_finds_bnb_env_var(self):
        """BSC chain checks BNB_RPC_URL too."""
        with patch.dict(os.environ, {"BNB_RPC_URL": "https://bnb-rpc"}):
            assert _get_custom_url("bsc") == "https://bnb-rpc"

    def test_bnb_finds_bsc_env_var(self):
        """BNB chain checks BSC_RPC_URL too."""
        with patch.dict(os.environ, {"BSC_RPC_URL": "https://bsc-rpc"}):
            assert _get_custom_url("bnb") == "https://bsc-rpc"

    def test_bsc_prefixed_env_var(self):
        """ALMANAK_BSC_RPC_URL works for bsc chain."""
        with patch.dict(os.environ, {"ALMANAK_BSC_RPC_URL": "https://almanak-bsc"}):
            assert _get_custom_url("bsc") == "https://almanak-bsc"

    def test_bnb_prefixed_env_var(self):
        """ALMANAK_BNB_RPC_URL works for bnb chain."""
        with patch.dict(os.environ, {"ALMANAK_BNB_RPC_URL": "https://almanak-bnb"}):
            assert _get_custom_url("bnb") == "https://almanak-bnb"

    def test_bsc_variant_has_priority_for_bsc_chain(self):
        """When both BSC and BNB are set, BSC comes first for 'bsc' chain."""
        with patch.dict(os.environ, {
            "ALMANAK_BSC_RPC_URL": "https://bsc-first",
            "ALMANAK_BNB_RPC_URL": "https://bnb-second",
        }):
            assert _get_custom_url("bsc") == "https://bsc-first"


class TestHasCustomUrl:
    """Test _has_custom_url helper."""

    def test_returns_true_for_chain_url(self):
        with patch.dict(os.environ, {"ARBITRUM_RPC_URL": "https://arb"}):
            assert _has_custom_url("arbitrum") is True

    def test_returns_true_for_generic_url(self):
        with patch.dict(os.environ, {"RPC_URL": "https://generic"}):
            assert _has_custom_url("arbitrum") is True

    def test_returns_false_when_empty(self):
        assert _has_custom_url("arbitrum") is False


class _PoABlockProvider(AsyncBaseProvider):
    """Minimal async provider returning the BSC block shape from ALM-3325."""

    async def make_request(self, method: RPCEndpoint, params: Any) -> RPCResponse:
        assert method == "eth_getBlockByNumber"
        return {
            "jsonrpc": "2.0",
            "id": 1,
            "result": {
                "number": "0x0",
                "timestamp": "0x5e9f5c4e",
                "extraData": "0x" + ("11" * 517),
            },
        }


class TestPoAMiddleware:
    """Descriptor-driven middleware configuration for sync and async clients."""

    @pytest.mark.parametrize("chain", ["bsc", "bnb", "polygon", "avalanche", "optimism", "op"])
    def test_injects_middleware_for_poa_chains(self, chain: str) -> None:
        web3 = AsyncWeb3(_PoABlockProvider())

        inject_poa_middleware(web3, chain)
        block = asyncio.run(web3.eth.get_block(0))

        assert block["number"] == 0
        assert len(block["proofOfAuthorityData"]) == 517

    def test_plain_client_reproduces_extra_data_failure(self) -> None:
        web3 = AsyncWeb3(_PoABlockProvider())

        with pytest.raises(ExtraDataLengthError, match="517 bytes"):
            asyncio.run(web3.eth.get_block(0))

    def test_does_not_inject_middleware_for_non_poa_chain(self) -> None:
        web3 = AsyncWeb3(_PoABlockProvider())

        inject_poa_middleware(web3, "ethereum")

        with pytest.raises(ExtraDataLengthError, match="517 bytes"):
            asyncio.run(web3.eth.get_block(0))

    @pytest.mark.parametrize("async_client", [True, False], ids=["async", "sync"])
    def test_selects_matching_web3_v6_legacy_middleware(
        self,
        monkeypatch: pytest.MonkeyPatch,
        async_client: bool,
    ) -> None:
        import web3 as web3_mod
        import web3.middleware as web3_middleware

        class _RecordingOnion:
            def __init__(self) -> None:
                self.injected: list[tuple[object, int]] = []

            def inject(self, middleware: object, *, layer: int) -> None:
                self.injected.append((middleware, layer))

        class _FakeAsyncWeb3:
            def __init__(self) -> None:
                self.middleware_onion = _RecordingOnion()

        class _FakeSyncWeb3:
            def __init__(self) -> None:
                self.middleware_onion = _RecordingOnion()

        async_middleware = object()
        sync_middleware = object()
        monkeypatch.setattr(web3_mod, "AsyncWeb3", _FakeAsyncWeb3)
        monkeypatch.delattr(web3_middleware, "ExtraDataToPOAMiddleware")
        monkeypatch.setattr(web3_middleware, "async_geth_poa_middleware", async_middleware, raising=False)
        monkeypatch.setattr(web3_middleware, "geth_poa_middleware", sync_middleware, raising=False)

        web3 = _FakeAsyncWeb3() if async_client else _FakeSyncWeb3()
        inject_poa_middleware(web3, "bsc")

        expected = async_middleware if async_client else sync_middleware
        assert web3.middleware_onion.injected == [(expected, 0)]


class TestPoaChainMembership:
    """``POA_CHAINS`` is derived from the descriptors; pin the chains that need it."""

    @pytest.mark.parametrize("chain", ["bsc", "polygon", "avalanche", "optimism"])
    def test_descriptor_declared_poa_chains(self, chain: str) -> None:
        assert chain in POA_CHAINS
        assert is_poa_chain(chain)

    def test_optimism_pre_bedrock_genesis_requires_poa_middleware(self) -> None:
        """ALM-3450: OP Mainnet's legacy genesis carries a 117-byte extraData."""
        assert "optimism" in POA_CHAINS
        assert is_poa_chain("op")

    @pytest.mark.parametrize("chain", ["ethereum", "arbitrum", "base"])
    def test_standard_genesis_chains_are_not_poa(self, chain: str) -> None:
        """Base is OP-stack but Bedrock-native: its genesis extraData decodes without middleware."""
        assert chain not in POA_CHAINS
        assert not is_poa_chain(chain)


class TestGetRpcUrl:
    """Test get_rpc_url end-to-end behavior."""

    def test_chain_specific_url_beats_alchemy(self):
        """Chain-specific URL env var is preferred over ALCHEMY_API_KEY."""
        with patch.dict(os.environ, {
            "ARBITRUM_RPC_URL": "https://arb-specific",
            "ALCHEMY_API_KEY": "test-alchemy-key",
        }):
            url = get_rpc_url("arbitrum")
            assert url == "https://arb-specific"

    def test_alchemy_beats_generic_rpc_url(self):
        """Alchemy takes priority over generic RPC_URL for supported chains (VIB-225)."""
        with patch.dict(os.environ, {
            "RPC_URL": "https://arb1.arbitrum.io/rpc",
            "ALCHEMY_API_KEY": "test-alchemy-key",
        }):
            url = get_rpc_url("base")
            # Should use Alchemy for base, NOT the generic arb1 RPC_URL
            assert "alchemy.com" in url
            assert "arb1.arbitrum.io" not in url

    def test_generic_rpc_url_used_when_no_alchemy_for_chain(self):
        """Generic RPC_URL used as fallback when ALCHEMY_API_KEY is not set."""
        with patch.dict(os.environ, {
            "RPC_URL": "https://custom-rpc",
        }):
            # For a chain that is in ALCHEMY_CHAIN_KEYS but no Alchemy key set
            url = get_rpc_url("arbitrum")
            assert url == "https://custom-rpc"

    def test_alchemy_fallback_when_no_custom_url(self):
        """ALCHEMY_API_KEY is used when no custom URL is set (backward compat)."""
        with patch.dict(os.environ, {"ALCHEMY_API_KEY": "test-alchemy-key"}):
            url = get_rpc_url("arbitrum")
            assert "alchemy.com" in url
            assert "test-alchemy-key" in url

    def test_alchemy_route_for_mantle(self):
        """mantle resolves to mantle-mainnet.g.alchemy.com when an Alchemy key is set."""
        with patch.dict(os.environ, {"ALCHEMY_API_KEY": "test-alchemy-key"}, clear=False):
            for var in ("MANTLE_RPC_URL", "ALMANAK_MANTLE_RPC_URL", "RPC_URL", "ALMANAK_RPC_URL"):
                os.environ.pop(var, None)
            url = get_rpc_url("mantle")
        assert "mantle-mainnet.g.alchemy.com" in url
        assert "test-alchemy-key" in url

    def test_alchemy_route_for_xlayer(self):
        """xlayer resolves to xlayer-mainnet.g.alchemy.com when an Alchemy key is set."""
        with patch.dict(os.environ, {"ALCHEMY_API_KEY": "test-alchemy-key"}, clear=False):
            for var in ("XLAYER_RPC_URL", "ALMANAK_XLAYER_RPC_URL", "RPC_URL", "ALMANAK_RPC_URL"):
                os.environ.pop(var, None)
            url = get_rpc_url("xlayer")
        assert "xlayer-mainnet.g.alchemy.com" in url
        assert "test-alchemy-key" in url

    def test_chain_descriptors_register_xlayer_alchemy_prefix(self):
        """xlayer / mantle Alchemy routing is wired on the chain descriptor, so any
        deployment that imports ``almanak.core.chains`` (i.e. all of them) routes
        through Alchemy. Previously this lived in ``config/rpc_defaults.json`` with
        a duplicated in-process ``_BUILTIN_CHAINS`` fallback; both are gone."""
        from almanak.core.chains import ChainRegistry
        from almanak.gateway.utils.rpc_provider import ALCHEMY_CHAIN_KEYS

        # ChainDescriptor.rpc is the single source of truth.
        assert ChainRegistry.resolve("xlayer").rpc.alchemy_prefix == "xlayer"
        assert ChainRegistry.resolve("mantle").rpc.alchemy_prefix == "mantle"
        # ALCHEMY_CHAIN_KEYS is derived from ChainRegistry at module import.
        assert ALCHEMY_CHAIN_KEYS.get("xlayer") == "xlayer"
        assert ALCHEMY_CHAIN_KEYS.get("mantle") == "mantle"

    def test_anvil_mode_ignores_custom_url(self):
        """Anvil network always returns localhost regardless of custom URL."""
        with patch.dict(os.environ, {"RPC_URL": "https://custom-rpc"}):
            url = get_rpc_url("arbitrum", network="anvil")
            assert "127.0.0.1" in url

    def test_explicit_custom_url_param_unchanged(self):
        """get_rpc_url(provider=CUSTOM, custom_url=...) still works."""
        url = get_rpc_url(
            "arbitrum",
            provider=NodeProvider.CUSTOM,
            custom_url="https://explicit-url",
        )
        assert url == "https://explicit-url"

    def test_custom_provider_without_param_uses_env(self):
        """NodeProvider.CUSTOM without custom_url falls back to env var lookup."""
        with patch.dict(os.environ, {"RPC_URL": "https://from-env"}):
            url = get_rpc_url("arbitrum", provider=NodeProvider.CUSTOM)
            assert url == "https://from-env"

    def test_per_chain_url_used_for_correct_chain(self):
        """Per-chain URL is used for the matching chain."""
        with patch.dict(os.environ, {
            "ARBITRUM_RPC_URL": "https://arb-specific",
            "BASE_RPC_URL": "https://base-specific",
        }):
            assert get_rpc_url("arbitrum") == "https://arb-specific"
            assert get_rpc_url("base") == "https://base-specific"

    def test_public_rpc_fallback_when_nothing_configured(self):
        """Falls back to free public RPC when no env vars or API keys are set."""
        url = get_rpc_url("arbitrum")
        assert url == "https://arbitrum-one-rpc.publicnode.com"

    def test_error_for_unsupported_chain_no_config(self):
        """Unsupported chain with no public RPC raises ValueError."""
        with pytest.raises(ValueError, match="No RPC provider available"):
            get_rpc_url("unsupported_chain_xyz")


class TestAutoSelectProvider:
    """Test _auto_select_provider ordering."""

    def test_chain_specific_url_selected_over_alchemy(self):
        """Chain-specific URL takes priority over Alchemy."""
        with patch.dict(os.environ, {
            "ARBITRUM_RPC_URL": "https://arb-specific",
            "ALCHEMY_API_KEY": "test-key",
        }):
            url = get_rpc_url("arbitrum")
            assert url == "https://arb-specific"

    def test_alchemy_selected_over_generic_rpc_url(self):
        """Alchemy takes priority over generic RPC_URL for supported chains (VIB-225)."""
        with patch.dict(os.environ, {
            "RPC_URL": "https://generic-rpc",
            "ALCHEMY_API_KEY": "test-key",
        }):
            url = get_rpc_url("base")
            assert "alchemy.com" in url

    def test_alchemy_selected_over_tenderly(self):
        """Alchemy takes priority over Tenderly."""
        with patch.dict(os.environ, {
            "ALCHEMY_API_KEY": "test-key",
            "TENDERLY_API_KEY_ARBITRUM": "tenderly-key",
        }):
            url = get_rpc_url("arbitrum")
            assert "alchemy.com" in url

    def test_alchemy_selected_over_public(self):
        """Alchemy takes priority over free public RPCs."""
        with patch.dict(os.environ, {"ALCHEMY_API_KEY": "test-key"}):
            url = get_rpc_url("arbitrum")
            assert "alchemy.com" in url
            assert "publicnode" not in url

    def test_public_rpc_used_when_no_keys(self):
        """Public RPC used as last resort when no env vars or keys are set."""
        url = get_rpc_url("ethereum")
        assert url == "https://ethereum-rpc.publicnode.com"


class TestHasApiKeyConfigured:
    """Test has_api_key_configured with custom URL env vars."""

    def test_with_rpc_url(self):
        """RPC_URL is recognized as a configured source."""
        with patch.dict(os.environ, {"RPC_URL": "https://custom"}):
            assert has_api_key_configured() is True

    def test_with_almanak_rpc_url(self):
        """ALMANAK_RPC_URL is recognized."""
        with patch.dict(os.environ, {"ALMANAK_RPC_URL": "https://custom"}):
            assert has_api_key_configured() is True

    def test_with_per_chain_url(self):
        """Per-chain URL is recognized."""
        with patch.dict(os.environ, {"ARBITRUM_RPC_URL": "https://arb"}):
            assert has_api_key_configured() is True

    def test_with_alchemy(self):
        """ALCHEMY_API_KEY still works (backward compat)."""
        with patch.dict(os.environ, {"ALCHEMY_API_KEY": "key"}):
            assert has_api_key_configured() is True

    def test_with_bsc_alias(self):
        """BSC_RPC_URL is recognized (alias for BNB)."""
        with patch.dict(os.environ, {"BSC_RPC_URL": "https://bsc"}):
            assert has_api_key_configured() is True

    def test_with_almanak_bsc_alias(self):
        """ALMANAK_BSC_RPC_URL is recognized (alias for BNB)."""
        with patch.dict(os.environ, {"ALMANAK_BSC_RPC_URL": "https://bsc"}):
            assert has_api_key_configured() is True


    def test_with_nothing(self):
        """Returns False when nothing is configured."""
        assert has_api_key_configured() is False


class TestChainSpecificVsGenericUrl:
    """Test _has_chain_specific_url vs _has_generic_url split (VIB-225)."""

    def test_chain_specific_detected(self):
        """Chain-specific env var detected by _has_chain_specific_url."""
        with patch.dict(os.environ, {"BASE_RPC_URL": "https://base-rpc"}):
            assert _has_chain_specific_url("base") is True
            assert _has_chain_specific_url("arbitrum") is False

    def test_generic_not_detected_as_chain_specific(self):
        """Generic RPC_URL is NOT detected by _has_chain_specific_url."""
        with patch.dict(os.environ, {"RPC_URL": "https://generic"}):
            assert _has_chain_specific_url("base") is False
            assert _has_chain_specific_url("arbitrum") is False

    def test_generic_detected_by_has_generic_url(self):
        """Generic RPC_URL detected by _has_generic_url."""
        with patch.dict(os.environ, {"RPC_URL": "https://generic"}):
            assert _has_generic_url() is True

    def test_almanak_generic_detected(self):
        """ALMANAK_RPC_URL detected by _has_generic_url."""
        with patch.dict(os.environ, {"ALMANAK_RPC_URL": "https://almanak-generic"}):
            assert _has_generic_url() is True

    @pytest.mark.parametrize("env_vars", [
        {"RPC_URL": "https://generic"},
        {"BASE_RPC_URL": "https://base"},
    ])
    def test_has_custom_url_still_covers_both(self, env_vars):
        """_has_custom_url returns True for both chain-specific and generic."""
        with patch.dict(os.environ, env_vars):
            assert _has_custom_url("base") is True

    def test_no_urls_all_false(self):
        """All helpers return False when nothing is set."""
        assert _has_chain_specific_url("base") is False
        assert _has_generic_url() is False
        assert _has_custom_url("base") is False
