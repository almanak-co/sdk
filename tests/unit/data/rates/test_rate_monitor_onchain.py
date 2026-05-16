"""Tests for RateMonitor on-chain Aave V3 rate fetching.

Verifies that _fetch_aave_v3_rate_onchain correctly:
- Calls AaveProtocolDataProvider.getReserveData(asset) via JSON-RPC
- Parses liquidityRate (word 5) for supply APY
- Parses variableBorrowRate (word 6) for borrow APY
- Computes utilization from totalVariableDebt / totalAToken
- Raises TokenNotSupportedError for unknown tokens
- Raises RateUnavailableError on RPC errors
- Falls back to placeholder rates when rpc_url is not set

Regression test for VIB-129: market.lending_rate() returns live on-chain rates.
"""

from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.data.rates.monitor import (
    RAY,
    RateMonitor,
    RateSide,
    RateUnavailableError,
    TokenNotSupportedError,
)

# Ethereum Aave V3 ProtocolDataProvider (from almanak.core.contracts)
ETHEREUM_DATA_PROVIDER = "0x7B4EB56E7CD4b454BA8ff71E4518426369a138a3"


def _build_reserve_data_hex(
    liquidity_rate_ray: int = 0,
    variable_borrow_rate_ray: int = 0,
    total_atoken: int = 1_000_000 * 10**6,
    total_variable_debt: int = 750_000 * 10**6,
) -> str:
    """Build a mock ABI-encoded AaveProtocolDataProvider.getReserveData() response.

    The function returns 12 values (11 uint256 + 1 uint40), each padded to 32 bytes:
      [0] unbacked
      [1] accruedToTreasuryScaled
      [2] totalAToken          <- used for utilization
      [3] totalStableDebt
      [4] totalVariableDebt   <- used for utilization
      [5] liquidityRate        <- supply APY in ray
      [6] variableBorrowRate   <- borrow APY in ray
      [7] stableBorrowRate
      [8] averageStableBorrowRate
      [9] liquidityIndex        <- initialized to RAY for any real reserve
      [10] variableBorrowIndex  <- initialized to RAY for any real reserve
      [11] lastUpdateTimestamp

    `liquidityIndex` and `variableBorrowIndex` default to RAY (1e27) because
    Aave V3 initializes them on reserve setup; a fully-zero response is what
    `getReserveData` returns for an *unknown* reserve, and the monitor rejects
    that case as `TokenNotSupportedError`.
    """
    words = [0] * 12
    words[2] = total_atoken
    words[4] = total_variable_debt
    words[5] = liquidity_rate_ray
    words[6] = variable_borrow_rate_ray
    words[9] = 10**27  # liquidityIndex = RAY (Aave V3 init value)
    words[10] = 10**27  # variableBorrowIndex = RAY (Aave V3 init value)
    raw = b"".join(w.to_bytes(32, "big") for w in words)
    return "0x" + raw.hex()


def _make_rpc_response(result_hex: str) -> MagicMock:
    """Create a mock httpx response returning the given hex result."""
    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    mock_response.json.return_value = {"jsonrpc": "2.0", "id": 1, "result": result_hex}
    return mock_response


def _make_rpc_error(message: str) -> MagicMock:
    """Create a mock httpx response with an RPC error."""
    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    mock_response.json.return_value = {
        "jsonrpc": "2.0",
        "id": 1,
        "error": {"code": -32603, "message": message},
    }
    return mock_response


def _make_mock_client(response: MagicMock) -> AsyncMock:
    """Create an async context manager mock for httpx.AsyncClient."""
    mock_client = AsyncMock()
    mock_client.post = AsyncMock(return_value=response)
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    return mock_client


@pytest.fixture
def monitor_with_rpc() -> RateMonitor:
    """RateMonitor configured with a (mocked) rpc_url."""
    return RateMonitor(chain="ethereum", rpc_url="http://localhost:8545")


@pytest.fixture
def monitor_no_rpc() -> RateMonitor:
    """RateMonitor without rpc_url -- uses placeholder rates."""
    return RateMonitor(chain="ethereum")


class TestOnchainSupplyRate:
    """Tests for fetching supply (liquidityRate) from on-chain."""

    @pytest.mark.asyncio
    async def test_supply_rate_parsed_from_word5(self, monitor_with_rpc: RateMonitor) -> None:
        """Supply APY parsed from liquidityRate (word index 5) of getReserveData response."""
        supply_ray = int(Decimal("4.25") / Decimal("100") * RAY)
        result_hex = _build_reserve_data_hex(liquidity_rate_ray=supply_ray)

        mock_client = _make_mock_client(_make_rpc_response(result_hex))
        with patch("httpx.AsyncClient", return_value=mock_client):
            rate = await monitor_with_rpc.get_lending_rate("aave_v3", "USDC", RateSide.SUPPLY)

        assert rate.protocol == "aave_v3"
        assert rate.token == "USDC"
        assert rate.side == "supply"
        assert rate.chain == "ethereum"
        assert abs(rate.apy_percent - Decimal("4.25")) < Decimal("0.001")
        # apy_ray should match what we put in
        assert rate.apy_ray == Decimal(supply_ray)

    @pytest.mark.asyncio
    async def test_zero_supply_rate_is_valid(self, monitor_with_rpc: RateMonitor) -> None:
        """Zero supply rate is a valid on-chain value (e.g., illiquid asset)."""
        result_hex = _build_reserve_data_hex(liquidity_rate_ray=0)

        mock_client = _make_mock_client(_make_rpc_response(result_hex))
        with patch("httpx.AsyncClient", return_value=mock_client):
            rate = await monitor_with_rpc.get_lending_rate("aave_v3", "USDC", RateSide.SUPPLY)

        assert rate.apy_percent == Decimal("0")


class TestOnchainBorrowRate:
    """Tests for fetching borrow (variableBorrowRate) from on-chain."""

    @pytest.mark.asyncio
    async def test_borrow_rate_parsed_from_word6(self, monitor_with_rpc: RateMonitor) -> None:
        """Borrow APY parsed from variableBorrowRate (word index 6) of getReserveData response."""
        borrow_ray = int(Decimal("5.75") / Decimal("100") * RAY)
        result_hex = _build_reserve_data_hex(variable_borrow_rate_ray=borrow_ray)

        mock_client = _make_mock_client(_make_rpc_response(result_hex))
        with patch("httpx.AsyncClient", return_value=mock_client):
            rate = await monitor_with_rpc.get_lending_rate("aave_v3", "USDC", RateSide.BORROW)

        assert rate.side == "borrow"
        assert abs(rate.apy_percent - Decimal("5.75")) < Decimal("0.001")

    @pytest.mark.asyncio
    async def test_supply_rate_not_used_for_borrow(self, monitor_with_rpc: RateMonitor) -> None:
        """Supply rate in word 5 is NOT used when fetching borrow rate."""
        supply_ray = int(Decimal("4.0") / Decimal("100") * RAY)
        borrow_ray = int(Decimal("6.0") / Decimal("100") * RAY)
        result_hex = _build_reserve_data_hex(
            liquidity_rate_ray=supply_ray, variable_borrow_rate_ray=borrow_ray
        )

        mock_client = _make_mock_client(_make_rpc_response(result_hex))
        with patch("httpx.AsyncClient", return_value=mock_client):
            rate = await monitor_with_rpc.get_lending_rate("aave_v3", "USDC", RateSide.BORROW)

        # Must use borrow rate, not supply rate
        assert abs(rate.apy_percent - Decimal("6.0")) < Decimal("0.001")


class TestUtilizationComputation:
    """Tests for utilization rate derived from totalVariableDebt / totalAToken."""

    @pytest.mark.asyncio
    async def test_utilization_75_percent(self, monitor_with_rpc: RateMonitor) -> None:
        """75% utilization when totalVariableDebt = 0.75 * totalAToken."""
        supply_ray = int(Decimal("3.0") / Decimal("100") * RAY)
        result_hex = _build_reserve_data_hex(
            liquidity_rate_ray=supply_ray,
            total_atoken=1_000_000,
            total_variable_debt=750_000,
        )

        mock_client = _make_mock_client(_make_rpc_response(result_hex))
        with patch("httpx.AsyncClient", return_value=mock_client):
            rate = await monitor_with_rpc.get_lending_rate("aave_v3", "USDC", RateSide.SUPPLY)

        assert rate.utilization_percent is not None
        assert abs(rate.utilization_percent - Decimal("75")) < Decimal("0.01")

    @pytest.mark.asyncio
    async def test_utilization_none_when_total_atoken_zero(self, monitor_with_rpc: RateMonitor) -> None:
        """Utilization is None when totalAToken is 0 (empty market)."""
        supply_ray = int(Decimal("0") / Decimal("100") * RAY)
        result_hex = _build_reserve_data_hex(
            liquidity_rate_ray=supply_ray,
            total_atoken=0,
            total_variable_debt=0,
        )

        mock_client = _make_mock_client(_make_rpc_response(result_hex))
        with patch("httpx.AsyncClient", return_value=mock_client):
            rate = await monitor_with_rpc.get_lending_rate("aave_v3", "USDC", RateSide.SUPPLY)

        assert rate.utilization_percent is None


class TestRpcCallDetails:
    """Tests verifying the JSON-RPC call structure."""

    @pytest.mark.asyncio
    async def test_calls_data_provider_not_pool(self, monitor_with_rpc: RateMonitor) -> None:
        """eth_call targets the AaveProtocolDataProvider (not the Pool)."""
        result_hex = _build_reserve_data_hex(liquidity_rate_ray=int(Decimal("3") / 100 * RAY))
        mock_client = _make_mock_client(_make_rpc_response(result_hex))

        with patch("httpx.AsyncClient", return_value=mock_client):
            await monitor_with_rpc.get_lending_rate("aave_v3", "USDC", RateSide.SUPPLY)

        call_args = mock_client.post.call_args
        payload = call_args.kwargs.get("json") or call_args.args[1]
        assert payload["method"] == "eth_call"
        to_addr = payload["params"][0]["to"].lower()
        assert to_addr == ETHEREUM_DATA_PROVIDER.lower()

    @pytest.mark.asyncio
    async def test_calldata_uses_getreservedata_selector(self, monitor_with_rpc: RateMonitor) -> None:
        """eth_call data starts with getReserveData(address) selector 0x35ea6a75."""
        result_hex = _build_reserve_data_hex(liquidity_rate_ray=int(Decimal("3") / 100 * RAY))
        mock_client = _make_mock_client(_make_rpc_response(result_hex))

        with patch("httpx.AsyncClient", return_value=mock_client):
            await monitor_with_rpc.get_lending_rate("aave_v3", "WETH", RateSide.SUPPLY)

        call_args = mock_client.post.call_args
        payload = call_args.kwargs.get("json") or call_args.args[1]
        calldata: str = payload["params"][0]["data"]
        # Selector is the first 10 chars (0x + 8 hex chars)
        assert calldata[:10].lower() == "0x35ea6a75"

    @pytest.mark.asyncio
    async def test_calldata_encodes_token_address(self, monitor_with_rpc: RateMonitor) -> None:
        """Token address is ABI-encoded (padded to 32 bytes) in the calldata."""
        result_hex = _build_reserve_data_hex(liquidity_rate_ray=int(Decimal("2") / 100 * RAY))
        mock_client = _make_mock_client(_make_rpc_response(result_hex))

        with patch("httpx.AsyncClient", return_value=mock_client):
            await monitor_with_rpc.get_lending_rate("aave_v3", "USDC", RateSide.SUPPLY)

        call_args = mock_client.post.call_args
        payload = call_args.kwargs.get("json") or call_args.args[1]
        calldata: str = payload["params"][0]["data"]
        # Total: 0x + 8 selector + 64 address = 74 chars
        assert len(calldata) == 74
        # Address part (last 40 chars) should match USDC on ethereum (lowercase, no 0x)
        addr_part = calldata[-40:].lower()
        usdc_ethereum = "a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
        assert addr_part == usdc_ethereum


class TestErrorHandling:
    """Tests for error cases in on-chain fetching."""

    @pytest.mark.asyncio
    async def test_unknown_token_raises_not_supported(self, monitor_with_rpc: RateMonitor) -> None:
        """Token absent from BOTH AAVE_V3_TOKENS and TokenResolver raises TokenNotSupportedError.

        The rate monitor now falls back to TokenResolver for tokens not in the static
        AAVE_V3_TOKENS registry, so this test mocks the resolver to ALSO fail (the only
        way to deterministically force "unknown token" behavior).
        """
        from almanak.framework.data.tokens.exceptions import TokenNotFoundError

        # Force resolver to also fail, ensuring the test exercises the "truly unknown" path
        resolver_mock = MagicMock()
        resolver_mock.resolve = MagicMock(side_effect=TokenNotFoundError(token="UNKNOWN_XYZ", chain="ethereum"))

        with patch("almanak.framework.data.tokens.get_token_resolver", return_value=resolver_mock):
            with pytest.raises(TokenNotSupportedError):
                await monitor_with_rpc.get_lending_rate("aave_v3", "UNKNOWN_XYZ", RateSide.SUPPLY)

        # Without this assertion the test could pass even if the fallback path
        # were silently bypassed — make sure we actually hit the resolver.
        resolver_mock.resolve.assert_called_once_with("UNKNOWN_XYZ", "ethereum")

    @pytest.mark.asyncio
    async def test_token_not_in_static_registry_resolves_via_tokenresolver(
        self, monitor_with_rpc: RateMonitor
    ) -> None:
        """Token missing from AAVE_V3_TOKENS but resolvable via TokenResolver gets a rate.

        Regression for the USDE-on-Aave-V3-Ethereum bug: USDe is a listed Aave reserve
        but not in the SDK's static AAVE_V3_TOKENS registry. Strategies asking for its
        rate previously failed with "Rate data unavailable" — they should now resolve
        the address dynamically and complete the on-chain rate query.
        """
        # USDe is a real Aave V3 Ethereum reserve, but not in AAVE_V3_TOKENS["ethereum"]
        usde_address = "0x4c9edd5852cd905f086c759e8383e09bff1e68b3"
        resolved_mock = MagicMock()
        resolved_mock.address = usde_address
        resolver_mock = MagicMock()
        resolver_mock.resolve = MagicMock(return_value=resolved_mock)

        # liquidityRate = 0.05 * RAY = 5e25 -> apy_percent = 5e25 / RAY * 100 = 5.0
        # (the decoder does a plain unit conversion, no APR->APY compounding)
        supply_rate_ray = 5 * 10**25
        rpc_response = _make_rpc_response(_build_reserve_data_hex(liquidity_rate_ray=supply_rate_ray))
        mock_client = _make_mock_client(rpc_response)

        with patch("almanak.framework.data.tokens.get_token_resolver", return_value=resolver_mock):
            with patch("httpx.AsyncClient", return_value=mock_client):
                rate = await monitor_with_rpc.get_lending_rate("aave_v3", "USDE", RateSide.SUPPLY)

        # The resolver was consulted with the bare symbol and chain
        resolver_mock.resolve.assert_called_once_with("USDE", "ethereum")
        # Exact decode of supply_rate_ray, not just "not zero" — catches index / unit regressions
        assert rate.protocol == "aave_v3"
        assert rate.token == "USDE"
        assert abs(rate.apy_percent - Decimal("5")) < Decimal("0.001")
        # And the eth_call payload included the USDe address we resolved
        call_args = mock_client.post.call_args
        rpc_payload = call_args.kwargs.get("json") or call_args.args[1]
        assert usde_address[2:].lower() in rpc_payload["params"][0]["data"].lower()

    @pytest.mark.asyncio
    async def test_resolver_address_not_an_aave_reserve_raises_not_supported(
        self, monitor_with_rpc: RateMonitor
    ) -> None:
        """Resolver returns a real address, but Aave doesn't list it -> TokenNotSupportedError.

        Defense-in-depth for the safety hole called out by reviewers on the
        TokenResolver-fallback PR. Mainnet Aave's `AaveProtocolDataProvider`
        actually reverts for unlisted reserves (it internally calls
        `aToken.totalSupply()` against `address(0)`), so live behavior surfaces
        as `RateUnavailableError`. But if any fork / fork-of-Aave / future
        deployment returned an all-zero struct instead of reverting, the
        decoder would have happily emitted `apy_percent=0`. The all-zero guard
        catches that and raises `TokenNotSupportedError` instead.
        """
        from almanak.framework.data.rates.monitor import TokenNotSupportedError

        resolved_mock = MagicMock()
        resolved_mock.address = "0x1234567890abcdef1234567890abcdef12345678"
        resolver_mock = MagicMock()
        resolver_mock.resolve = MagicMock(return_value=resolved_mock)

        # Mock an all-zero 12-word response — the shape getReserveData would
        # have if it ever returned data instead of reverting for an unknown reserve.
        all_zero_hex = "0x" + ("00" * 32) * 12
        rpc_response = _make_rpc_response(all_zero_hex)
        mock_client = _make_mock_client(rpc_response)

        with patch("almanak.framework.data.tokens.get_token_resolver", return_value=resolver_mock):
            with patch("httpx.AsyncClient", return_value=mock_client):
                with pytest.raises(TokenNotSupportedError):
                    await monitor_with_rpc.get_lending_rate("aave_v3", "NOT_AN_AAVE_TOKEN", RateSide.SUPPLY)

        # Prove both halves of the fallback path ran: resolver was consulted
        # for the bare symbol, and the resolved address was actually sent to
        # the RPC layer (so the guard ran on the response, not earlier).
        resolver_mock.resolve.assert_called_once_with("NOT_AN_AAVE_TOKEN", "ethereum")
        assert mock_client.post.call_count == 1

    @pytest.mark.asyncio
    async def test_rpc_error_raises_rate_unavailable(self, monitor_with_rpc: RateMonitor) -> None:
        """JSON-RPC error response raises RateUnavailableError."""
        mock_client = _make_mock_client(_make_rpc_error("Internal JSON-RPC error"))

        with patch("httpx.AsyncClient", return_value=mock_client):
            with pytest.raises(RateUnavailableError) as exc_info:
                await monitor_with_rpc.get_lending_rate("aave_v3", "USDC", RateSide.SUPPLY)

        assert "Internal JSON-RPC error" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_empty_rpc_result_raises_not_supported(self, monitor_with_rpc: RateMonitor) -> None:
        """Empty result (0x) from RPC raises TokenNotSupportedError (token not in pool)."""
        mock_client = _make_mock_client(_make_rpc_response("0x"))

        with patch("httpx.AsyncClient", return_value=mock_client):
            with pytest.raises(TokenNotSupportedError):
                await monitor_with_rpc.get_lending_rate("aave_v3", "USDC", RateSide.SUPPLY)

    @pytest.mark.asyncio
    async def test_short_response_raises_rate_unavailable(self, monitor_with_rpc: RateMonitor) -> None:
        """Response with too few words raises RateUnavailableError."""
        # Only 3 words (need at least 7)
        short_hex = "0x" + ("00" * 32 * 3)
        mock_client = _make_mock_client(_make_rpc_response(short_hex))

        with patch("httpx.AsyncClient", return_value=mock_client):
            with pytest.raises(RateUnavailableError):
                await monitor_with_rpc.get_lending_rate("aave_v3", "USDC", RateSide.SUPPLY)

    @pytest.mark.asyncio
    async def test_unsupported_chain_raises_rate_unavailable(self) -> None:
        """Chain without Aave V3 data provider raises RateUnavailableError."""
        monitor = RateMonitor(chain="unknown_chain", rpc_url="http://localhost:8545")
        # Patch _protocols to include aave_v3 so validation passes
        monitor._protocols = ["aave_v3"]

        with pytest.raises(RateUnavailableError) as exc_info:
            await monitor.get_lending_rate("aave_v3", "USDC", RateSide.SUPPLY)

        assert "unknown_chain" in str(exc_info.value)


class TestPlaceholderFallback:
    """Tests for placeholder rate fallback when rpc_url is not set."""

    @pytest.mark.asyncio
    async def test_no_rpc_uses_placeholder(self, monitor_no_rpc: RateMonitor) -> None:
        """Without rpc_url, returns placeholder rate (no HTTP call made)."""
        rate = await monitor_no_rpc.get_lending_rate("aave_v3", "USDC", RateSide.SUPPLY)

        # Placeholder USDC supply rate
        assert rate.apy_percent == Decimal("4.25")
        assert rate.protocol == "aave_v3"

    @pytest.mark.asyncio
    async def test_no_rpc_borrow_uses_placeholder(self, monitor_no_rpc: RateMonitor) -> None:
        """Borrow rate falls back to placeholder when no rpc_url."""
        rate = await monitor_no_rpc.get_lending_rate("aave_v3", "USDC", RateSide.BORROW)
        assert rate.apy_percent == Decimal("5.75")

    @pytest.mark.asyncio
    async def test_no_rpc_unknown_token_raises(self, monitor_no_rpc: RateMonitor) -> None:
        """Unknown token raises TokenNotSupportedError even without rpc_url."""
        with pytest.raises(TokenNotSupportedError):
            await monitor_no_rpc.get_lending_rate("aave_v3", "UNKNOWN_TOKEN", RateSide.SUPPLY)

    @pytest.mark.asyncio
    async def test_rpc_overrides_placeholder(self, monitor_with_rpc: RateMonitor) -> None:
        """When rpc_url is set, on-chain rate overrides placeholder (different value)."""
        # Set a rate that differs from the placeholder (4.25%)
        live_rate_ray = int(Decimal("7.50") / Decimal("100") * RAY)
        result_hex = _build_reserve_data_hex(liquidity_rate_ray=live_rate_ray)
        mock_client = _make_mock_client(_make_rpc_response(result_hex))

        with patch("httpx.AsyncClient", return_value=mock_client):
            rate = await monitor_with_rpc.get_lending_rate("aave_v3", "USDC", RateSide.SUPPLY)

        # Should return on-chain rate (7.5%), not placeholder (4.25%)
        assert abs(rate.apy_percent - Decimal("7.50")) < Decimal("0.001")
