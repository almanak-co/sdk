"""Tests for Web3BalanceProvider integration with TokenResolver.

Verifies that the provider uses TokenResolver as the sole source of truth
for token resolution, with an on-chain ERC20 fallback for raw addresses that
are not in the static registry.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.gateway.data.balance.web3_provider import (
    TokenMetadata,
    Web3BalanceProvider,
)


class TestWeb3BalanceProviderTokenResolver:
    """Tests for TokenResolver integration in Web3BalanceProvider."""

    @pytest.fixture
    def mock_resolver(self):
        """Create a mock TokenResolver."""
        from almanak.framework.data.tokens.models import ResolvedToken

        resolver = MagicMock()

        # Default: resolve WETH on arbitrum
        weth_resolved = ResolvedToken(
            symbol="WETH",
            address="0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
            decimals=18,
            chain="arbitrum",
            chain_id=42161,
            is_native=False,
            is_wrapped_native=True,
            source="static",
        )
        resolver.resolve.return_value = weth_resolved
        return resolver

    @pytest.fixture
    def provider(self, mock_resolver):
        """Create provider with mock resolver."""
        return Web3BalanceProvider(
            rpc_url="http://localhost:8545",
            wallet_address="0x0000000000000000000000000000000000000001",
            chain="arbitrum",
            token_resolver=mock_resolver,
        )

    def test_init_with_custom_resolver(self, mock_resolver):
        """Provider accepts custom token_resolver parameter."""
        provider = Web3BalanceProvider(
            rpc_url="http://localhost:8545",
            wallet_address="0x0000000000000000000000000000000000000001",
            chain="arbitrum",
            token_resolver=mock_resolver,
        )
        assert provider._token_resolver is mock_resolver

    def test_init_with_default_resolver(self):
        """Provider uses get_token_resolver() when no resolver provided."""
        mock_resolver = MagicMock()
        with patch(
            "almanak.framework.data.tokens.resolver.get_token_resolver",
            return_value=mock_resolver,
        ):
            provider = Web3BalanceProvider(
                rpc_url="http://localhost:8545",
                wallet_address="0x0000000000000000000000000000000000000001",
                chain="arbitrum",
            )
            assert provider._token_resolver is mock_resolver

    @pytest.mark.asyncio
    async def test_resolve_token_uses_resolver(self, provider, mock_resolver):
        """_resolve_token delegates to TokenResolver.resolve()."""
        result = await provider._resolve_token("WETH")

        mock_resolver.resolve.assert_called_once_with("WETH", "arbitrum")
        assert result is not None
        assert result.symbol == "WETH"
        assert result.decimals == 18
        assert result.address == "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"

    @pytest.mark.asyncio
    async def test_resolve_token_by_address(self, provider, mock_resolver):
        """_resolve_token resolves addresses via TokenResolver."""
        from almanak.framework.data.tokens.models import ResolvedToken

        usdc_resolved = ResolvedToken(
            symbol="USDC",
            address="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            decimals=6,
            chain="arbitrum",
            chain_id=42161,
            source="static",
        )
        mock_resolver.resolve.return_value = usdc_resolved

        result = await provider._resolve_token("0xaf88d065e77c8cC2239327C5EDb3A432268e5831")

        assert result is not None
        assert result.symbol == "USDC"
        assert result.decimals == 6

    @pytest.mark.asyncio
    async def test_resolve_token_returns_token_metadata(self, provider, mock_resolver):
        """_resolve_token converts ResolvedToken to TokenMetadata."""
        result = await provider._resolve_token("WETH")

        assert isinstance(result, TokenMetadata)
        assert result.symbol == "WETH"
        assert result.decimals == 18
        assert result.is_native is False

    @pytest.mark.asyncio
    async def test_resolve_native_token(self, provider, mock_resolver):
        """_resolve_token correctly handles native tokens."""
        from almanak.framework.data.tokens.models import ResolvedToken

        eth_resolved = ResolvedToken(
            symbol="ETH",
            address="0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE",
            decimals=18,
            chain="arbitrum",
            chain_id=42161,
            is_native=True,
            source="static",
        )
        mock_resolver.resolve.return_value = eth_resolved

        result = await provider._resolve_token("ETH")

        assert result is not None
        assert result.is_native is True
        assert result.symbol == "ETH"

    @pytest.mark.asyncio
    async def test_resolve_usdc_correct_decimals(self, provider, mock_resolver):
        """_resolve_token returns correct decimals for USDC (6, not 18)."""
        from almanak.framework.data.tokens.models import ResolvedToken

        usdc_resolved = ResolvedToken(
            symbol="USDC",
            address="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            decimals=6,
            chain="arbitrum",
            chain_id=42161,
            source="static",
        )
        mock_resolver.resolve.return_value = usdc_resolved

        result = await provider._resolve_token("USDC")

        assert result is not None
        assert result.decimals == 6  # NEVER default to 18

    @pytest.mark.asyncio
    async def test_resolve_unknown_token_returns_none(self, provider, mock_resolver):
        """_resolve_token returns None for unknown non-address tokens.

        Symbol input with no static entry cannot be recovered without dynamic
        discovery (CoinGecko/Jupiter), which is intentionally out of scope.
        """
        from almanak.framework.data.tokens.exceptions import TokenNotFoundError

        mock_resolver.resolve.side_effect = TokenNotFoundError("UNKNOWN_TOKEN", "arbitrum")

        result = await provider._resolve_token("UNKNOWN_TOKEN")

        assert result is None

    @pytest.mark.asyncio
    async def test_resolve_unknown_address_returns_none(self, provider, mock_resolver):
        """_resolve_token returns None for unknown addresses when on-chain lookup also fails."""
        from almanak.framework.data.tokens.exceptions import TokenNotFoundError

        unknown_addr = "0x1111111111111111111111111111111111111111"
        mock_resolver.resolve.side_effect = TokenNotFoundError(unknown_addr, "arbitrum")

        # Raw address now triggers the on-chain fallback. Stub it to return None
        # (as OnChainLookup does for non-ERC20 contracts) so the assertion still holds.
        fake_lookup = MagicMock()
        fake_lookup.lookup = AsyncMock(return_value=None)
        with patch.object(Web3BalanceProvider, "_get_onchain_lookup", return_value=fake_lookup):
            result = await provider._resolve_token(unknown_addr)

        assert result is None

    @pytest.mark.asyncio
    async def test_resolver_unexpected_failure_propagates(self, provider, mock_resolver):
        """Unexpected resolver exceptions propagate -- they are NOT collapsed to None.

        Coercing programmer errors / infrastructure failures into "token not
        found" would mask the real problem and put strategies into HOLD instead
        of surfacing the failure to the gateway as a service error. Only typed
        TokenNotFoundError / TokenResolutionError map to None.
        """
        mock_resolver.resolve.side_effect = Exception("resolver unavailable")

        with pytest.raises(Exception, match="resolver unavailable"):
            await provider._resolve_token("WETH")

        # WETH is not an EVM address, so the on-chain fallback is never reached
        # -- the resolver exception propagates without being silently coerced.

    def test_no_local_token_registry(self, provider):
        """Provider no longer has a local _token_registry attribute."""
        assert not hasattr(provider, "_token_registry")

    def test_token_registry_removed_from_module(self):
        """TOKEN_REGISTRY is no longer exported from web3_provider module."""
        import almanak.gateway.data.balance.web3_provider as mod

        assert not hasattr(mod, "TOKEN_REGISTRY")

    @pytest.mark.asyncio
    async def test_resolve_bridged_token(self, provider, mock_resolver):
        """_resolve_token handles bridged tokens like USDC.e."""
        from almanak.framework.data.tokens.models import ResolvedToken

        usdc_e_resolved = ResolvedToken(
            symbol="USDC.e",
            address="0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8",
            decimals=6,
            chain="arbitrum",
            chain_id=42161,
            source="static",
        )
        mock_resolver.resolve.return_value = usdc_e_resolved

        result = await provider._resolve_token("USDC.e")

        assert result is not None
        assert result.symbol == "USDC.e"
        assert result.decimals == 6

    @pytest.mark.asyncio
    async def test_resolve_case_insensitive(self, provider, mock_resolver):
        """_resolve_token works with different cases."""
        await provider._resolve_token("weth")

        # TokenResolver.resolve should be called with the original token
        mock_resolver.resolve.assert_called_once_with("weth", "arbitrum")

    def test_add_token_registers_with_resolver(self, provider, mock_resolver):
        """add_token() registers tokens with the TokenResolver."""
        provider.add_token(
            symbol="CUSTOM",
            address="0x0000000000000000000000000000000000000042",
            decimals=18,
        )

        mock_resolver.register.assert_called_once()
        registered = mock_resolver.register.call_args[0][0]
        assert registered.symbol == "CUSTOM"
        assert registered.decimals == 18
        from almanak.core.enums import Chain

        assert registered.chain == Chain.ARBITRUM


class TestWeb3BalanceProviderMultiChain:
    """Test resolver integration across multiple chains."""

    @pytest.mark.asyncio
    async def test_ethereum_chain(self):
        """Provider works with ethereum chain."""
        mock_resolver = MagicMock()
        from almanak.framework.data.tokens.models import ResolvedToken

        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="WETH",
            address="0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
            decimals=18,
            chain="ethereum",
            chain_id=1,
            source="static",
        )

        provider = Web3BalanceProvider(
            rpc_url="http://localhost:8545",
            wallet_address="0x0000000000000000000000000000000000000001",
            chain="ethereum",
            token_resolver=mock_resolver,
        )

        result = await provider._resolve_token("WETH")
        mock_resolver.resolve.assert_called_once_with("WETH", "ethereum")
        assert result is not None
        assert result.address == "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"

    @pytest.mark.asyncio
    async def test_avalanche_chain(self):
        """Provider works with avalanche chain."""
        mock_resolver = MagicMock()
        from almanak.framework.data.tokens.models import ResolvedToken

        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="WAVAX",
            address="0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7",
            decimals=18,
            chain="avalanche",
            chain_id=43114,
            source="static",
        )

        provider = Web3BalanceProvider(
            rpc_url="http://localhost:8545",
            wallet_address="0x0000000000000000000000000000000000000001",
            chain="avalanche",
            token_resolver=mock_resolver,
        )

        result = await provider._resolve_token("WAVAX")
        mock_resolver.resolve.assert_called_once_with("WAVAX", "avalanche")
        assert result is not None
        assert result.address == "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7"


class TestWeb3BalanceProviderOnChainFallback:
    """Tests for the on-chain ERC20 fallback when the static resolver misses."""

    @pytest.fixture
    def static_miss_resolver(self):
        """Resolver that always raises TokenNotFoundError for any input."""
        from almanak.framework.data.tokens.exceptions import TokenNotFoundError

        resolver = MagicMock()
        resolver.resolve.side_effect = TokenNotFoundError("unused", "arbitrum")
        return resolver

    @pytest.fixture
    def provider(self, static_miss_resolver):
        return Web3BalanceProvider(
            rpc_url="http://localhost:8545",
            wallet_address="0x0000000000000000000000000000000000000001",
            chain="base",
            token_resolver=static_miss_resolver,
        )

    @pytest.fixture
    def fake_onchain_metadata(self):
        """Fake TokenMetadata returned by OnChainLookup.lookup."""
        from almanak.gateway.services.onchain_lookup import (
            TokenMetadata as OnChainTokenMetadata,
        )

        return OnChainTokenMetadata(
            address="0xcb5ff7331193c45f61f05b035ddabe08f13f6ba3",
            symbol="OPENAGENTS",
            decimals=18,
            name="OpenAgents",
            is_native=False,
        )

    @pytest.mark.asyncio
    async def test_static_miss_valid_erc20_returns_metadata(
        self, provider, static_miss_resolver, fake_onchain_metadata
    ):
        """Unknown address -> OnChainLookup returns metadata -> provider returns it.

        SECURITY: the discovered token MUST NOT be written back into the shared
        TokenResolver. The resolver indexes by symbol as well as by address
        (see cache.py:278-315), and `metadata.symbol` comes from the contract's
        on-chain symbol() call -- an attacker-controlled value. Persisting a
        (chain, symbol) -> address entry from an untrusted contract would
        corrupt future symbol-based resolutions process-wide.
        """
        fake_lookup = MagicMock()
        fake_lookup.lookup = AsyncMock(return_value=fake_onchain_metadata)

        with patch.object(Web3BalanceProvider, "_get_onchain_lookup", return_value=fake_lookup):
            result = await provider._resolve_token("0xcb5ff7331193c45f61f05b035ddabe08f13f6ba3")

        assert result is not None
        assert result.symbol == "OPENAGENTS"
        assert result.decimals == 18
        assert result.address == "0xcb5ff7331193c45f61f05b035ddabe08f13f6ba3"
        assert result.is_native is False

        fake_lookup.lookup.assert_awaited_once_with(
            "base", "0xcb5ff7331193c45f61f05b035ddabe08f13f6ba3"
        )
        # register() must NOT be called: the contract-reported symbol is untrusted.
        static_miss_resolver.register.assert_not_called()

    @pytest.mark.asyncio
    async def test_static_miss_onchain_returns_none(self, provider, static_miss_resolver):
        """Unknown address -> OnChainLookup returns None -> _resolve_token returns None."""
        fake_lookup = MagicMock()
        fake_lookup.lookup = AsyncMock(return_value=None)

        with patch.object(Web3BalanceProvider, "_get_onchain_lookup", return_value=fake_lookup):
            result = await provider._resolve_token("0xdead000000000000000000000000000000000000")

        assert result is None
        static_miss_resolver.register.assert_not_called()

    @pytest.mark.asyncio
    async def test_static_miss_onchain_infrastructure_failure_propagates(
        self, provider, static_miss_resolver
    ):
        """Unknown address -> OnChainLookup raises infra error -> exception propagates.

        OnChainLookup internally catches ContractLogicError (non-ERC20 contracts)
        and returns None. Any other exception that escapes lookup() is an
        infrastructure-level failure (network, RPC timeout, web3 client bug)
        that MUST propagate so the gateway returns a service error -- coercing
        it to "token not found" would make strategies HOLD instead of retry.
        """
        fake_lookup = MagicMock()
        fake_lookup.lookup = AsyncMock(side_effect=Exception("rpc down"))

        with patch.object(Web3BalanceProvider, "_get_onchain_lookup", return_value=fake_lookup):
            with pytest.raises(Exception, match="rpc down"):
                await provider._resolve_token("0xdead000000000000000000000000000000000001")

        static_miss_resolver.register.assert_not_called()

    @pytest.mark.asyncio
    async def test_static_miss_non_address_does_not_call_onchain(
        self, provider, static_miss_resolver
    ):
        """Symbol-like input -> _get_onchain_lookup is NOT called."""
        with patch.object(
            Web3BalanceProvider, "_get_onchain_lookup", autospec=True
        ) as mock_get_lookup:
            result = await provider._resolve_token("OPENAGENTS")

        assert result is None
        mock_get_lookup.assert_not_called()

    @pytest.mark.asyncio
    async def test_static_hit_skips_onchain(self):
        """Static hit -> OnChainLookup is not invoked at all."""
        from almanak.framework.data.tokens.models import ResolvedToken

        resolver = MagicMock()
        resolver.resolve.return_value = ResolvedToken(
            symbol="WETH",
            address="0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
            decimals=18,
            chain="arbitrum",
            chain_id=42161,
            is_native=False,
            is_wrapped_native=True,
            source="static",
        )
        provider = Web3BalanceProvider(
            rpc_url="http://localhost:8545",
            wallet_address="0x0000000000000000000000000000000000000001",
            chain="arbitrum",
            token_resolver=resolver,
        )

        with patch.object(
            Web3BalanceProvider, "_get_onchain_lookup", autospec=True
        ) as mock_get_lookup:
            result = await provider._resolve_token("WETH")

        assert result is not None
        assert result.symbol == "WETH"
        mock_get_lookup.assert_not_called()


class TestWeb3BalanceProviderGetBalanceOnChainFallback:
    """Integration test: get_balance on an unknown ERC20 address via on-chain fallback."""

    @pytest.mark.asyncio
    async def test_get_balance_unknown_address_uses_onchain_decimals(self):
        """get_balance for a raw address resolves decimals on-chain and computes human balance."""
        from almanak.framework.data.tokens.exceptions import TokenNotFoundError
        from almanak.gateway.services.onchain_lookup import (
            TokenMetadata as OnChainTokenMetadata,
        )

        unknown_address = "0xcb5ff7331193c45f61f05b035ddabe08f13f6ba3"

        static_miss_resolver = MagicMock()
        static_miss_resolver.resolve.side_effect = TokenNotFoundError(unknown_address, "base")

        provider = Web3BalanceProvider(
            rpc_url="http://localhost:8545",
            wallet_address="0x0000000000000000000000000000000000000001",
            chain="base",
            token_resolver=static_miss_resolver,
        )

        fake_lookup = MagicMock()
        fake_lookup.lookup = AsyncMock(
            return_value=OnChainTokenMetadata(
                address=unknown_address,
                symbol="OPENAGENTS",
                decimals=18,
                name="OpenAgents",
                is_native=False,
            )
        )

        # 2.5 OPENAGENTS in raw (18 decimals)
        raw_balance = 2_500_000_000_000_000_000

        with patch.object(Web3BalanceProvider, "_get_onchain_lookup", return_value=fake_lookup):
            with patch.object(
                Web3BalanceProvider,
                "_get_erc20_balance_with_retry",
                new=AsyncMock(return_value=raw_balance),
            ):
                result = await provider.get_balance(unknown_address)

        from decimal import Decimal

        assert result.balance == Decimal("2.5")
        assert result.decimals == 18
        assert result.address == unknown_address
        assert result.token == "OPENAGENTS"
        assert result.raw_balance == raw_balance
