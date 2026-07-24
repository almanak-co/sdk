"""Tests for MultiDexPriceService integration with TokenResolver.

Verifies that the service uses TokenResolver as the sole source of truth
for token address and decimals resolution (no local fallback registries).
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.gateway.data.price.multi_dex import (
    MultiDexPriceService,
    TokenNotSupportedError,
)


class TestMultiDexPriceServiceTokenResolver:
    """Tests for TokenResolver integration in MultiDexPriceService."""

    @pytest.fixture
    def mock_resolver(self):
        """Create a mock TokenResolver."""
        from almanak.framework.data.tokens.models import ResolvedToken

        resolver = MagicMock()

        # Default: resolve WETH on ethereum
        weth_resolved = ResolvedToken(
            symbol="WETH",
            address="0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
            decimals=18,
            chain="ethereum",
            chain_id=1,
            is_native=False,
            is_wrapped_native=True,
            source="static",
        )
        resolver.resolve.return_value = weth_resolved
        return resolver

    @pytest.fixture
    def service(self, mock_resolver):
        """Create service with mock resolver."""
        return MultiDexPriceService(
            chain="ethereum",
            token_resolver=mock_resolver,
        )

    def test_init_with_custom_resolver(self, mock_resolver):
        """Service accepts custom token_resolver parameter."""
        service = MultiDexPriceService(
            chain="ethereum",
            token_resolver=mock_resolver,
        )
        assert service._token_resolver is mock_resolver

    def test_init_with_default_resolver(self):
        """Service uses get_token_resolver() when no resolver provided."""
        mock_resolver = MagicMock()
        with patch(
            "almanak.framework.data.tokens.resolver.get_token_resolver",
            return_value=mock_resolver,
        ):
            service = MultiDexPriceService(chain="ethereum")
            assert service._token_resolver is mock_resolver

    def test_resolve_token_address_uses_resolver(self, service, mock_resolver):
        """_resolve_token_address delegates to TokenResolver.resolve()."""
        result = service._resolve_token_address("WETH")

        mock_resolver.resolve.assert_called_once_with("WETH", "ethereum")
        assert result == "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"

    def test_resolve_token_address_by_symbol(self, service, mock_resolver):
        """_resolve_token_address resolves symbols via TokenResolver."""
        from almanak.framework.data.tokens.models import ResolvedToken

        usdc_resolved = ResolvedToken(
            symbol="USDC",
            address="0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            decimals=6,
            chain="ethereum",
            chain_id=1,
            source="static",
        )
        mock_resolver.resolve.return_value = usdc_resolved

        result = service._resolve_token_address("USDC")

        assert result == "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"

    def test_get_token_decimals_uses_resolver(self, service, mock_resolver):
        """_get_token_decimals delegates to TokenResolver.resolve()."""
        result = service._get_token_decimals("WETH")

        mock_resolver.resolve.assert_called_once_with("WETH", "ethereum")
        assert result == 18

    def test_get_token_decimals_usdc(self, service, mock_resolver):
        """_get_token_decimals returns correct decimals for USDC (6, not 18)."""
        from almanak.framework.data.tokens.models import ResolvedToken

        usdc_resolved = ResolvedToken(
            symbol="USDC",
            address="0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            decimals=6,
            chain="ethereum",
            chain_id=1,
            source="static",
        )
        mock_resolver.resolve.return_value = usdc_resolved

        result = service._get_token_decimals("USDC")

        assert result == 6  # NEVER default to 18

    def test_resolve_token_address_resolver_failure_passthrough_address(self, service, mock_resolver):
        """_resolve_token_address passes through raw addresses when resolver fails."""
        from almanak.framework.data.tokens.exceptions import TokenNotFoundError

        unknown_addr = "0x1111111111111111111111111111111111111111"
        mock_resolver.resolve.side_effect = TokenNotFoundError(unknown_addr, "ethereum")

        result = service._resolve_token_address(unknown_addr)

        assert result == unknown_addr

    def test_resolve_token_address_unknown_symbol_raises(self, service, mock_resolver):
        """_resolve_token_address raises TokenNotSupportedError for unknown symbols."""
        from almanak.framework.data.tokens.exceptions import TokenNotFoundError

        mock_resolver.resolve.side_effect = TokenNotFoundError("UNKNOWN_TOKEN", "ethereum")

        with pytest.raises(TokenNotSupportedError):
            service._resolve_token_address("UNKNOWN_TOKEN")

    def test_get_token_decimals_resolver_failure_raises(self, service, mock_resolver):
        """_get_token_decimals raises TokenNotSupportedError when resolver fails."""
        mock_resolver.resolve.side_effect = Exception("resolver unavailable")

        with pytest.raises(TokenNotSupportedError):
            service._get_token_decimals("UNKNOWN_TOKEN")

    def test_token_addresses_removed_from_module(self):
        """TOKEN_ADDRESSES is no longer defined in multi_dex module."""
        import almanak.gateway.data.price.multi_dex as mod

        assert not hasattr(mod, "TOKEN_ADDRESSES")

    def test_token_decimals_removed_from_module(self):
        """TOKEN_DECIMALS is no longer defined in multi_dex module."""
        import almanak.gateway.data.price.multi_dex as mod

        assert not hasattr(mod, "TOKEN_DECIMALS")

    def test_resolve_bridged_token(self, service, mock_resolver):
        """_resolve_token_address handles bridged tokens like USDC.e."""
        from almanak.framework.data.tokens.models import ResolvedToken

        mock_resolver_arb = MagicMock()
        usdc_e_resolved = ResolvedToken(
            symbol="USDC.e",
            address="0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8",
            decimals=6,
            chain="arbitrum",
            chain_id=42161,
            source="static",
        )
        mock_resolver_arb.resolve.return_value = usdc_e_resolved

        service_arb = MultiDexPriceService(
            chain="arbitrum",
            token_resolver=mock_resolver_arb,
        )

        result = service_arb._resolve_token_address("USDC.e")

        assert result == "0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8"

    def test_amount_to_wei_uses_resolver_decimals(self, service, mock_resolver):
        """_amount_to_wei uses decimals from TokenResolver."""
        from almanak.framework.data.tokens.models import ResolvedToken

        usdc_resolved = ResolvedToken(
            symbol="USDC",
            address="0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            decimals=6,
            chain="ethereum",
            chain_id=1,
            source="static",
        )
        mock_resolver.resolve.return_value = usdc_resolved

        result = service._amount_to_wei(Decimal("100"), "USDC")

        assert result == 100_000_000  # 100 * 10^6

    def test_wei_to_amount_uses_resolver_decimals(self, service, mock_resolver):
        """_wei_to_amount uses decimals from TokenResolver."""
        from almanak.framework.data.tokens.models import ResolvedToken

        usdc_resolved = ResolvedToken(
            symbol="USDC",
            address="0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            decimals=6,
            chain="ethereum",
            chain_id=1,
            source="static",
        )
        mock_resolver.resolve.return_value = usdc_resolved

        result = service._wei_to_amount(100_000_000, "USDC")

        assert result == Decimal("100")


class TestMultiDexPriceServiceMultiChain:
    """Test resolver integration across multiple chains."""

    def test_arbitrum_chain(self):
        """Service works with arbitrum chain and resolver."""
        mock_resolver = MagicMock()
        from almanak.framework.data.tokens.models import ResolvedToken

        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="WETH",
            address="0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
            decimals=18,
            chain="arbitrum",
            chain_id=42161,
            source="static",
        )

        service = MultiDexPriceService(
            chain="arbitrum",
            token_resolver=mock_resolver,
        )

        result = service._resolve_token_address("WETH")
        mock_resolver.resolve.assert_called_once_with("WETH", "arbitrum")
        assert result == "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"

    def test_base_chain(self):
        """Service works with base chain and resolver."""
        mock_resolver = MagicMock()
        from almanak.framework.data.tokens.models import ResolvedToken

        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="USDC",
            address="0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
            decimals=6,
            chain="base",
            chain_id=8453,
            source="static",
        )

        service = MultiDexPriceService(
            chain="base",
            token_resolver=mock_resolver,
        )

        result = service._resolve_token_address("USDC")
        assert result == "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"

        decimals = service._get_token_decimals("USDC")
        assert decimals == 6


# =============================================================================
# MultiDexPriceResult.price_spread_bps
# =============================================================================


def _quote(dex: str, amount_out: str, amount_in: str = "100") -> "DexQuote":
    from almanak.gateway.data.price.multi_dex import DexQuote

    amt_in = Decimal(amount_in)
    amt_out = Decimal(amount_out)
    return DexQuote(
        dex=dex,
        token_in="USDC",
        token_out="WETH",
        amount_in=amt_in,
        amount_out=amt_out,
        price=amt_out / amt_in if amt_in > 0 else Decimal("0"),
    )


def _result(quotes: dict) -> "MultiDexPriceResult":
    from almanak.gateway.data.price.multi_dex import MultiDexPriceResult

    return MultiDexPriceResult(
        token_in="USDC",
        token_out="WETH",
        amount_in=Decimal("100"),
        quotes=quotes,
    )


class TestPriceSpreadBps:
    """Branch coverage for MultiDexPriceResult.price_spread_bps."""

    def test_no_quotes_returns_zero(self):
        assert _result({}).price_spread_bps == 0

    def test_single_quote_returns_zero(self):
        assert _result({"curve": _quote("curve", "1.0")}).price_spread_bps == 0

    def test_two_quotes_but_only_one_valid_returns_zero(self):
        quotes = {
            "curve": _quote("curve", "1.0"),
            "enso": _quote("enso", "0"),  # amount_out=0 -> is_valid False
        }
        assert _result(quotes).price_spread_bps == 0

    def test_spread_computed_between_best_and_worst_valid_quotes(self):
        quotes = {
            "curve": _quote("curve", "1.00"),
            "uniswap_v3": _quote("uniswap_v3", "1.02"),
            "enso": _quote("enso", "0"),  # invalid, excluded from the spread
        }
        # (1.02 - 1.00) / 1.00 * 10000 = 200 bps
        assert _result(quotes).price_spread_bps == 200

    def test_identical_quotes_have_zero_spread(self):
        quotes = {
            "curve": _quote("curve", "1.0"),
            "enso": _quote("enso", "1.0"),
        }
        assert _result(quotes).price_spread_bps == 0

    def test_spread_truncates_toward_zero(self):
        quotes = {
            "curve": _quote("curve", "3"),
            "enso": _quote("enso", "1"),
        }
        # (3 - 1) / 1 * 10000 = 20000 bps exactly
        assert _result(quotes).price_spread_bps == 20000


# =============================================================================
# MultiDexPriceService._get_curve_quote
# =============================================================================


class TestGetCurveQuote:
    """Branch coverage for the simulated Curve quote path."""

    @pytest.fixture
    def service(self):
        return MultiDexPriceService(chain="ethereum", token_resolver=MagicMock())

    @pytest.mark.asyncio
    async def test_mock_quote_short_circuits(self, service):
        from almanak.gateway.data.price.multi_dex import DexQuote

        sentinel = DexQuote(
            dex="curve",
            token_in="USDC",
            token_out="USDT",
            amount_in=Decimal("1"),
            amount_out=Decimal("1"),
            price=Decimal("1"),
        )
        captured = []

        def mock_fn(token_in, token_out, amount_in):
            captured.append((token_in, token_out, amount_in))
            return sentinel

        service.set_mock_quote("curve", mock_fn)

        quote = await service._get_curve_quote("USDC", "USDT", Decimal("123"))

        assert quote is sentinel
        assert captured == [("USDC", "USDT", Decimal("123"))]

    @pytest.mark.asyncio
    async def test_stable_pair_uses_peg_price_and_stable_parameters(self, service):
        amount = Decimal("100000")
        quote = await service._get_curve_quote("USDC", "USDT", amount)

        assert quote.dex == "curve"
        assert quote.chain == "ethereum"
        assert quote.route == "3pool"
        assert quote.fee_bps == 4
        assert quote.slippage_estimate_bps == 1
        # $100k stable trade: max(1, int(100000 / 1e6)) = 1 bp impact.
        assert quote.price_impact_bps == 1
        assert quote.amount_out == amount * (Decimal(10000 - 1) / Decimal(10000))
        assert quote.price == quote.amount_out / amount

    @pytest.mark.asyncio
    async def test_large_stable_trade_scales_impact_per_million(self, service):
        amount = Decimal("5000000")
        quote = await service._get_curve_quote("USDT", "DAI", amount)

        assert quote.price_impact_bps == 5  # 1 bp per $1M
        assert quote.route == "3pool"

    @pytest.mark.asyncio
    async def test_lst_pair_keeps_full_price_but_uses_crypto_pool_parameters(self, service):
        amount = Decimal("10")
        with patch.object(service, "_get_default_price", return_value=Decimal("1")) as price_fn:
            quote = await service._get_curve_quote("WETH", "stETH", amount)

        price_fn.assert_called_once_with("WETH", "stETH")
        # LST pair: no 0.995 discount, but crypto-pool fee and slippage model.
        assert quote.route == "CryptoSwap"
        assert quote.fee_bps == 30
        # $25k notional (10 * 2500): 1 bp per $1M floor.
        assert quote.price_impact_bps == 1
        # _estimate_slippage(25000, "curve") = max(1, int(2 * 0.5)) = 1
        assert quote.slippage_estimate_bps == 1
        assert quote.amount_out == amount * (Decimal(10000 - 1) / Decimal(10000))

    @pytest.mark.asyncio
    async def test_non_stable_non_lst_pair_gets_discounted_quote(self, service):
        amount = Decimal("40")  # 40 * 2500 = $100k notional
        with patch.object(service, "_get_default_price", return_value=Decimal("0.05")):
            quote = await service._get_curve_quote("WETH", "WBTC", amount)

        assert quote.route == "CryptoSwap"
        assert quote.fee_bps == 30
        # _estimate_price_impact(100000, "curve") = int(5 * 1.0) = 5 bps
        assert quote.price_impact_bps == 5
        # _estimate_slippage(100000, "curve") = int(2 * 1.0) = 2 bps
        assert quote.slippage_estimate_bps == 2
        expected_out = (
            amount * Decimal("0.05") * Decimal("0.995") * (Decimal(10000 - 5) / Decimal(10000))
        )
        assert quote.amount_out == expected_out
        assert quote.price == expected_out / amount

    @pytest.mark.asyncio
    async def test_zero_amount_yields_zero_price_without_division_error(self, service):
        quote = await service._get_curve_quote("USDC", "USDT", Decimal("0"))

        assert quote.amount_out == Decimal("0")
        assert quote.price == Decimal("0")
        assert quote.is_valid is False

    @pytest.mark.asyncio
    async def test_non_stable_pair_without_price_feed_fails_loud(self, service):
        from almanak.gateway.data.price.multi_dex import QuoteUnavailableError

        # No mock and no oracle: the simulation refuses to invent a price
        # (VIB-3137) and the error propagates out of _get_curve_quote.
        with pytest.raises(QuoteUnavailableError, match="No simulated price for WETH->WBTC"):
            await service._get_curve_quote("WETH", "WBTC", Decimal("1"))
