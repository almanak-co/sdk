"""Tests for RateMonitor on-chain Compound V3 rate fetching.

Verifies that _fetch_compound_v3_rate_onchain correctly:
- Calls Comet.getUtilization() then getSupplyRate()/getBorrowRate() via JSON-RPC
- Converts per-second rates to APY percentage
- Computes utilization from on-chain value
- Resolves token to correct Comet market address
- Raises TokenNotSupportedError for unknown tokens
- Raises RateUnavailableError on RPC errors
- Falls back to placeholder rates when rpc_url is not set
"""

from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.data.rates.monitor import (
    RAY,
    SECONDS_PER_YEAR,
    RateMonitor,
    RateSide,
    RateUnavailableError,
    TokenNotSupportedError,
    _COMPOUND_V3_RATE_SCALE,
)

# Ethereum USDC Comet address (from adapter)
ETHEREUM_USDC_COMET = "0xc3d688B66703497DAA19211EEdff47f25384cdc3"


def _make_rpc_response(result_hex: str, id: int = 1) -> MagicMock:
    """Create a mock httpx response returning the given hex result."""
    mock = MagicMock()
    mock.raise_for_status = MagicMock()
    mock.json.return_value = {"jsonrpc": "2.0", "id": id, "result": result_hex}
    return mock


def _make_rpc_error(message: str) -> MagicMock:
    """Create a mock httpx response with an RPC error."""
    mock = MagicMock()
    mock.raise_for_status = MagicMock()
    mock.json.return_value = {"jsonrpc": "2.0", "id": 1, "error": {"code": -32603, "message": message}}
    return mock


def _encode_uint256(value: int) -> str:
    """Encode a uint256 as 0x-prefixed hex."""
    return f"0x{value:064x}"


def _per_second_rate_for_apy(apy_percent: Decimal) -> int:
    """Calculate per-second rate (1e18 scale) that produces the given APY percentage.

    APY = rate_per_second * SECONDS_PER_YEAR / 1e18 * 100
    => rate_per_second = apy_percent / 100 * 1e18 / SECONDS_PER_YEAR
    """
    return int(apy_percent / Decimal("100") * _COMPOUND_V3_RATE_SCALE / Decimal(SECONDS_PER_YEAR))


def _make_sequential_mock_client(responses: list[MagicMock]) -> AsyncMock:
    """Create a mock httpx.AsyncClient that returns responses in order."""
    mock_client = AsyncMock()
    mock_client.post = AsyncMock(side_effect=responses)
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    return mock_client


@pytest.fixture
def monitor_with_rpc() -> RateMonitor:
    """RateMonitor configured with a (mocked) rpc_url for Compound V3."""
    return RateMonitor(chain="ethereum", rpc_url="http://localhost:8545")


@pytest.fixture
def monitor_no_rpc() -> RateMonitor:
    """RateMonitor without rpc_url -- uses placeholder rates."""
    return RateMonitor(chain="ethereum")


class TestCompoundV3OnchainSupplyRate:
    """Tests for fetching supply rate from Compound V3 on-chain."""

    @pytest.mark.asyncio
    async def test_supply_rate_converted_to_apy(self, monitor_with_rpc: RateMonitor) -> None:
        """Per-second supply rate is correctly converted to APY percentage."""
        target_apy = Decimal("4.50")
        rate_per_sec = _per_second_rate_for_apy(target_apy)
        utilization = int(Decimal("0.80") * _COMPOUND_V3_RATE_SCALE)  # 80%

        util_response = _make_rpc_response(_encode_uint256(utilization))
        rate_response = _make_rpc_response(_encode_uint256(rate_per_sec))
        mock_client = _make_sequential_mock_client([util_response, rate_response])

        with patch("httpx.AsyncClient", return_value=mock_client):
            rate = await monitor_with_rpc.get_lending_rate("compound_v3", "USDC", RateSide.SUPPLY)

        assert rate.protocol == "compound_v3"
        assert rate.token == "USDC"
        assert rate.side == "supply"
        assert rate.chain == "ethereum"
        assert rate.market_id == "usdc"
        # Allow small rounding from int truncation of per-second rate
        assert abs(rate.apy_percent - target_apy) < Decimal("0.01")

    @pytest.mark.asyncio
    async def test_utilization_parsed_correctly(self, monitor_with_rpc: RateMonitor) -> None:
        """Utilization is converted from 1e18 scale to percentage."""
        utilization = int(Decimal("0.825") * _COMPOUND_V3_RATE_SCALE)  # 82.5%
        rate_per_sec = _per_second_rate_for_apy(Decimal("3.0"))

        util_response = _make_rpc_response(_encode_uint256(utilization))
        rate_response = _make_rpc_response(_encode_uint256(rate_per_sec))
        mock_client = _make_sequential_mock_client([util_response, rate_response])

        with patch("httpx.AsyncClient", return_value=mock_client):
            rate = await monitor_with_rpc.get_lending_rate("compound_v3", "USDC", RateSide.SUPPLY)

        assert rate.utilization_percent is not None
        assert abs(rate.utilization_percent - Decimal("82.5")) < Decimal("0.01")


class TestCompoundV3OnchainBorrowRate:
    """Tests for fetching borrow rate from Compound V3 on-chain."""

    @pytest.mark.asyncio
    async def test_borrow_rate_uses_correct_selector(self, monitor_with_rpc: RateMonitor) -> None:
        """Borrow rate fetching uses getBorrowRate selector, not getSupplyRate."""
        target_apy = Decimal("6.00")
        rate_per_sec = _per_second_rate_for_apy(target_apy)
        utilization = int(Decimal("0.75") * _COMPOUND_V3_RATE_SCALE)

        util_response = _make_rpc_response(_encode_uint256(utilization))
        rate_response = _make_rpc_response(_encode_uint256(rate_per_sec))
        mock_client = _make_sequential_mock_client([util_response, rate_response])

        with patch("httpx.AsyncClient", return_value=mock_client):
            rate = await monitor_with_rpc.get_lending_rate("compound_v3", "USDC", RateSide.BORROW)

        assert rate.side == "borrow"
        assert abs(rate.apy_percent - target_apy) < Decimal("0.01")

        # Verify second call uses getBorrowRate selector (9fa83b5a)
        second_call = mock_client.post.call_args_list[1]
        payload = second_call.kwargs.get("json") or second_call.args[1]
        calldata = payload["params"][0]["data"]
        assert calldata[:10].lower() == "0x9fa83b5a"


class TestCompoundV3RpcCallDetails:
    """Tests verifying JSON-RPC call structure for Compound V3."""

    @pytest.mark.asyncio
    async def test_targets_correct_comet_address(self, monitor_with_rpc: RateMonitor) -> None:
        """eth_call targets the correct Comet contract for the token."""
        utilization = int(Decimal("0.75") * _COMPOUND_V3_RATE_SCALE)
        rate_per_sec = _per_second_rate_for_apy(Decimal("4.0"))

        util_response = _make_rpc_response(_encode_uint256(utilization))
        rate_response = _make_rpc_response(_encode_uint256(rate_per_sec))
        mock_client = _make_sequential_mock_client([util_response, rate_response])

        with patch("httpx.AsyncClient", return_value=mock_client):
            await monitor_with_rpc.get_lending_rate("compound_v3", "USDC", RateSide.SUPPLY)

        # Both calls should target the USDC Comet address
        for call in mock_client.post.call_args_list:
            payload = call.kwargs.get("json") or call.args[1]
            to_addr = payload["params"][0]["to"].lower()
            assert to_addr == ETHEREUM_USDC_COMET.lower()

    @pytest.mark.asyncio
    async def test_first_call_is_get_utilization(self, monitor_with_rpc: RateMonitor) -> None:
        """First eth_call uses getUtilization() selector (7eb71131)."""
        utilization = int(Decimal("0.75") * _COMPOUND_V3_RATE_SCALE)
        rate_per_sec = _per_second_rate_for_apy(Decimal("4.0"))

        util_response = _make_rpc_response(_encode_uint256(utilization))
        rate_response = _make_rpc_response(_encode_uint256(rate_per_sec))
        mock_client = _make_sequential_mock_client([util_response, rate_response])

        with patch("httpx.AsyncClient", return_value=mock_client):
            await monitor_with_rpc.get_lending_rate("compound_v3", "USDC", RateSide.SUPPLY)

        first_call = mock_client.post.call_args_list[0]
        payload = first_call.kwargs.get("json") or first_call.args[1]
        calldata = payload["params"][0]["data"]
        assert calldata[:10].lower() == "0x7eb71131"

    @pytest.mark.asyncio
    async def test_supply_uses_get_supply_rate_selector(self, monitor_with_rpc: RateMonitor) -> None:
        """Second eth_call for supply uses getSupplyRate selector (d955759d)."""
        utilization = int(Decimal("0.75") * _COMPOUND_V3_RATE_SCALE)
        rate_per_sec = _per_second_rate_for_apy(Decimal("4.0"))

        util_response = _make_rpc_response(_encode_uint256(utilization))
        rate_response = _make_rpc_response(_encode_uint256(rate_per_sec))
        mock_client = _make_sequential_mock_client([util_response, rate_response])

        with patch("httpx.AsyncClient", return_value=mock_client):
            await monitor_with_rpc.get_lending_rate("compound_v3", "USDC", RateSide.SUPPLY)

        second_call = mock_client.post.call_args_list[1]
        payload = second_call.kwargs.get("json") or second_call.args[1]
        calldata = payload["params"][0]["data"]
        assert calldata[:10].lower() == "0xd955759d"


class TestCompoundV3ErrorHandling:
    """Tests for error cases in Compound V3 on-chain fetching."""

    @pytest.mark.asyncio
    async def test_unknown_token_raises_not_supported(self, monitor_with_rpc: RateMonitor) -> None:
        """Token not in _COMPOUND_V3_TOKEN_TO_MARKET raises TokenNotSupportedError."""
        with pytest.raises(TokenNotSupportedError):
            await monitor_with_rpc.get_lending_rate("compound_v3", "UNKNOWN_TOKEN", RateSide.SUPPLY)

    @pytest.mark.asyncio
    async def test_rpc_error_on_utilization_raises(self, monitor_with_rpc: RateMonitor) -> None:
        """RPC error on getUtilization raises RateUnavailableError."""
        error_response = _make_rpc_error("execution reverted")
        mock_client = _make_sequential_mock_client([error_response])

        with patch("httpx.AsyncClient", return_value=mock_client):
            with pytest.raises(RateUnavailableError, match="getUtilization"):
                await monitor_with_rpc.get_lending_rate("compound_v3", "USDC", RateSide.SUPPLY)

    @pytest.mark.asyncio
    async def test_rpc_error_on_rate_raises(self, monitor_with_rpc: RateMonitor) -> None:
        """RPC error on getSupplyRate/getBorrowRate raises RateUnavailableError."""
        utilization = int(Decimal("0.75") * _COMPOUND_V3_RATE_SCALE)
        util_response = _make_rpc_response(_encode_uint256(utilization))
        error_response = _make_rpc_error("execution reverted")
        mock_client = _make_sequential_mock_client([util_response, error_response])

        with patch("httpx.AsyncClient", return_value=mock_client):
            with pytest.raises(RateUnavailableError, match="getRate"):
                await monitor_with_rpc.get_lending_rate("compound_v3", "USDC", RateSide.SUPPLY)

    @pytest.mark.asyncio
    async def test_unsupported_chain_raises(self) -> None:
        """Chain without Compound V3 Comet addresses raises RateUnavailableError."""
        monitor = RateMonitor(chain="unknown_chain", rpc_url="http://localhost:8545")
        monitor._protocols = ["compound_v3"]

        with pytest.raises(RateUnavailableError, match="unknown_chain"):
            await monitor.get_lending_rate("compound_v3", "USDC", RateSide.SUPPLY)


class TestCompoundV3PlaceholderFallback:
    """Tests for placeholder rate fallback when rpc_url is not set."""

    @pytest.mark.asyncio
    async def test_no_rpc_uses_placeholder(self, monitor_no_rpc: RateMonitor) -> None:
        """Without rpc_url, returns placeholder rate."""
        rate = await monitor_no_rpc.get_lending_rate("compound_v3", "USDC", RateSide.SUPPLY)
        assert rate.apy_percent == Decimal("4.85")
        assert rate.protocol == "compound_v3"

    @pytest.mark.asyncio
    async def test_no_rpc_borrow_uses_placeholder(self, monitor_no_rpc: RateMonitor) -> None:
        """Borrow rate falls back to placeholder when no rpc_url."""
        rate = await monitor_no_rpc.get_lending_rate("compound_v3", "USDC", RateSide.BORROW)
        assert rate.apy_percent == Decimal("6.15")

    @pytest.mark.asyncio
    async def test_rpc_overrides_placeholder(self, monitor_with_rpc: RateMonitor) -> None:
        """When rpc_url is set, on-chain rate overrides placeholder."""
        target_apy = Decimal("7.50")
        rate_per_sec = _per_second_rate_for_apy(target_apy)
        utilization = int(Decimal("0.80") * _COMPOUND_V3_RATE_SCALE)

        util_response = _make_rpc_response(_encode_uint256(utilization))
        rate_response = _make_rpc_response(_encode_uint256(rate_per_sec))
        mock_client = _make_sequential_mock_client([util_response, rate_response])

        with patch("httpx.AsyncClient", return_value=mock_client):
            rate = await monitor_with_rpc.get_lending_rate("compound_v3", "USDC", RateSide.SUPPLY)

        # On-chain (7.50%), not placeholder (4.85%)
        assert abs(rate.apy_percent - target_apy) < Decimal("0.01")


class TestCompoundV3RayConversion:
    """Tests for APY ray conversion consistency."""

    @pytest.mark.asyncio
    async def test_apy_ray_consistent_with_percent(self, monitor_with_rpc: RateMonitor) -> None:
        """apy_ray should equal apy_percent * RAY / 100."""
        target_apy = Decimal("5.25")
        rate_per_sec = _per_second_rate_for_apy(target_apy)
        utilization = int(Decimal("0.75") * _COMPOUND_V3_RATE_SCALE)

        util_response = _make_rpc_response(_encode_uint256(utilization))
        rate_response = _make_rpc_response(_encode_uint256(rate_per_sec))
        mock_client = _make_sequential_mock_client([util_response, rate_response])

        with patch("httpx.AsyncClient", return_value=mock_client):
            rate = await monitor_with_rpc.get_lending_rate("compound_v3", "USDC", RateSide.SUPPLY)

        expected_ray = rate.apy_percent * RAY / Decimal("100")
        assert rate.apy_ray == expected_ray
