"""Tests for RpcService gateway implementation."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.rpc_service import (
    ALLOWED_CHAINS,
    ChainRateLimiter,
    RpcServiceServicer,
)


@pytest.fixture
def settings():
    """Create test settings."""
    return GatewaySettings()


@pytest.fixture
def rpc_service(settings):
    """Create RpcService instance."""
    return RpcServiceServicer(settings)


@pytest.fixture
def mock_context():
    """Create mock gRPC context."""
    context = MagicMock()
    context.set_code = MagicMock()
    context.set_details = MagicMock()
    return context


class TestChainRateLimiter:
    """Tests for ChainRateLimiter."""

    @pytest.mark.asyncio
    async def test_allows_requests_under_limit(self):
        """Requests under limit are allowed."""
        limiter = ChainRateLimiter(requests_per_minute=10)

        for _ in range(5):
            allowed, wait_time = await limiter.check_rate_limit()
            assert allowed is True
            assert wait_time == 0.0
            await limiter.record_request()

    @pytest.mark.asyncio
    async def test_blocks_requests_over_limit(self):
        """Requests over limit are blocked."""
        limiter = ChainRateLimiter(requests_per_minute=3)

        # Make 3 requests
        for _ in range(3):
            await limiter.record_request()

        # 4th request should be blocked
        allowed, wait_time = await limiter.check_rate_limit()
        assert allowed is False
        assert wait_time > 0

    @pytest.mark.asyncio
    async def test_reserve_records_slots_atomically(self):
        """reserve=True records the slots so the budget is consumed in one step."""
        limiter = ChainRateLimiter(requests_per_minute=10)

        allowed, wait_time = await limiter.check_rate_limit(5, reserve=True)
        assert allowed is True
        assert wait_time == 0.0
        assert len(limiter.request_times) == 5  # reserved, not just checked

        # A second 5-slot reservation now fits exactly (5 + 5 == 10).
        allowed2, _ = await limiter.check_rate_limit(5, reserve=True)
        assert allowed2 is True
        assert len(limiter.request_times) == 10

        # An 11th slot is over the cap.
        allowed3, wait3 = await limiter.check_rate_limit(1)
        assert allowed3 is False
        assert wait3 > 0

    @pytest.mark.asyncio
    async def test_concurrent_reservations_cannot_oversubscribe(self):
        """Two concurrent reserve(count) calls cannot both pass a near-full budget.

        The check-then-record race (CodeRabbit): without atomic reservation, both
        callers could pass an inspect-only check before either records. reserve=True
        holds the lock across check+record, so at most the cap is consumed.
        """
        import asyncio

        limiter = ChainRateLimiter(requests_per_minute=5)
        results = await asyncio.gather(
            limiter.check_rate_limit(5, reserve=True),
            limiter.check_rate_limit(5, reserve=True),
        )
        allowed_count = sum(1 for ok, _ in results if ok)
        assert allowed_count == 1  # exactly one reservation fits; the other is rejected
        assert len(limiter.request_times) == 5  # never oversubscribed past the cap


class TestRpcServiceCall:
    """Tests for RpcService.Call."""

    @pytest.mark.asyncio
    async def test_rejects_unknown_chain(self, rpc_service, mock_context):
        """Call rejects unknown chains."""
        request = gateway_pb2.RpcRequest(
            chain="unknown_chain",
            method="eth_blockNumber",
            params="[]",
            id="1",
        )

        response = await rpc_service.Call(request, mock_context)

        assert response.success is False
        assert "not allowed" in json.loads(response.error)["message"]
        mock_context.set_code.assert_called()

    @pytest.mark.asyncio
    async def test_accepts_allowed_chains(self, rpc_service, mock_context):
        """Allowed chains list is correct."""
        # Verify expected chains are in allowlist
        expected_chains = {"ethereum", "arbitrum", "base", "optimism", "polygon", "avalanche"}
        assert expected_chains.issubset(ALLOWED_CHAINS)

    @pytest.mark.asyncio
    async def test_validates_params_json(self, rpc_service, mock_context):
        """Call validates params JSON."""
        request = gateway_pb2.RpcRequest(
            chain="arbitrum",
            method="eth_call",
            params="invalid json {{{",
            id="1",
        )

        # Mock the RPC URL lookup to return None (chain not configured)
        with patch.object(rpc_service, "_get_rpc_url", return_value="http://test"):
            response = await rpc_service.Call(request, mock_context)

            assert response.success is False
            assert "Invalid params JSON" in json.loads(response.error)["message"]

    @pytest.mark.asyncio
    async def test_successful_rpc_call(self, rpc_service, mock_context):
        """Successful RPC call returns result."""
        request = gateway_pb2.RpcRequest(
            chain="arbitrum",
            method="eth_blockNumber",
            params="[]",
            id="1",
        )

        # Mock the RPC call
        with patch.object(rpc_service, "_get_rpc_url", return_value="http://test"):
            with patch.object(
                rpc_service,
                "_make_rpc_call",
                return_value=("0x123", None),
            ):
                response = await rpc_service.Call(request, mock_context)

                assert response.success is True
                assert json.loads(response.result) == "0x123"

    @pytest.mark.asyncio
    async def test_rpc_call_error_handling(self, rpc_service, mock_context):
        """RPC call errors are returned correctly."""
        request = gateway_pb2.RpcRequest(
            chain="arbitrum",
            method="eth_call",
            params="[]",
            id="1",
        )

        error = {"code": -32000, "message": "execution reverted"}

        with patch.object(rpc_service, "_get_rpc_url", return_value="http://test"):
            with patch.object(
                rpc_service,
                "_make_rpc_call",
                return_value=(None, error),
            ):
                response = await rpc_service.Call(request, mock_context)

                assert response.success is False
                assert json.loads(response.error) == error


class TestRpcServiceChainAllowlist:
    """Tests for gateway-level settings.chains enforcement (VIB-2864 / DogFooding bug 1).

    Regression: a gateway started with --chains zerog would silently forward
    RPC calls for any other chain (e.g. "arbitrum") back to a valid RPC
    endpoint, returning Arbitrum state for what the caller thought was zerog.
    The fix enforces settings.chains at the gateway so the error message
    guides callers to pass --chain zerog.
    """

    @pytest.fixture
    def zerog_only_service(self):
        """RpcService configured to serve only zerog."""
        settings = GatewaySettings(chains=["zerog"])
        return RpcServiceServicer(settings)

    @pytest.mark.asyncio
    async def test_call_rejects_unconfigured_chain(self, zerog_only_service, mock_context):
        """Call against a zerog-only gateway rejects arbitrum with FAILED_PRECONDITION."""
        request = gateway_pb2.RpcRequest(
            chain="arbitrum",
            method="eth_call",
            params='[{"to":"0xC36442b4a4522E871399CD717aBDD847Ab11FE88","data":"0x99fbab88"},"latest"]',
            id="lp_position",
        )

        response = await zerog_only_service.Call(request, mock_context)

        assert response.success is False
        err = json.loads(response.error)
        assert "not configured" in err["message"]
        assert "zerog" in err["message"]

    @pytest.mark.asyncio
    async def test_call_accepts_configured_chain(self, zerog_only_service, mock_context):
        """Call against a zerog-only gateway accepts zerog."""
        request = gateway_pb2.RpcRequest(
            chain="zerog",
            method="eth_blockNumber",
            params="[]",
            id="1",
        )

        with patch.object(zerog_only_service, "_get_rpc_url", return_value="http://zerog-rpc"):
            with patch.object(zerog_only_service, "_make_rpc_call", return_value=("0x42", None)):
                response = await zerog_only_service.Call(request, mock_context)

        assert response.success is True
        assert json.loads(response.result) == "0x42"

    @pytest.mark.asyncio
    async def test_call_unrestricted_gateway_accepts_any_chain(self, rpc_service, mock_context):
        """Empty settings.chains (unrestricted) accepts any allowed chain."""
        request = gateway_pb2.RpcRequest(
            chain="arbitrum",
            method="eth_blockNumber",
            params="[]",
            id="1",
        )

        with patch.object(rpc_service, "_get_rpc_url", return_value="http://test"):
            with patch.object(rpc_service, "_make_rpc_call", return_value=("0x1", None)):
                response = await rpc_service.Call(request, mock_context)

        assert response.success is True

    @pytest.mark.asyncio
    async def test_batch_call_rejects_unconfigured_chain(self, zerog_only_service, mock_context):
        """BatchCall against a zerog-only gateway rejects arbitrum."""
        request = gateway_pb2.RpcBatchRequest(
            chain="arbitrum",
            requests=[
                gateway_pb2.RpcRequest(method="eth_blockNumber", params="[]", id="1"),
            ],
        )

        response = await zerog_only_service.BatchCall(request, mock_context)

        assert len(response.responses) == 0
        mock_context.set_code.assert_called()

    @pytest.mark.asyncio
    async def test_query_allowance_rejects_unconfigured_chain(self, zerog_only_service, mock_context):
        """QueryAllowance against a zerog-only gateway rejects arbitrum."""
        request = gateway_pb2.AllowanceRequest(
            chain="arbitrum",
            token_address="0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            owner_address="0x0000000000000000000000000000000000000001",
            spender_address="0x0000000000000000000000000000000000000002",
        )

        response = await zerog_only_service.QueryAllowance(request, mock_context)

        assert response.success is False
        assert "not configured" in response.error

    @pytest.mark.asyncio
    async def test_query_position_liquidity_rejects_unconfigured_chain(self, zerog_only_service, mock_context):
        """QueryPositionLiquidity against a zerog-only gateway rejects arbitrum.

        This mirrors the DogFooding bug reproduction: `ax lp-info 2359` on a
        zerog gateway used to silently query Arbitrum's NPM for position #2359.
        Now the gateway rejects the cross-chain call with a clear error.
        """
        request = gateway_pb2.PositionLiquidityRequest(
            chain="arbitrum",
            position_manager="0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
            token_id=2359,
        )

        response = await zerog_only_service.QueryPositionLiquidity(request, mock_context)

        assert response.success is False
        assert "not configured" in response.error
        assert "zerog" in response.error

    @pytest.mark.asyncio
    async def test_query_balance_rejects_unconfigured_chain(self, zerog_only_service, mock_context):
        """QueryBalance against a zerog-only gateway rejects arbitrum.

        Mirrors the Call/BatchCall/QueryAllowance/QueryPositionLiquidity tests —
        every RPC endpoint must enforce the same allowlist or the fix is
        incomplete. A balanceOf query routed to the wrong chain returns
        nonsense wei values that would flow straight into slippage
        calculation for a cross-chain swap.
        """
        request = gateway_pb2.BalanceQueryRequest(
            chain="arbitrum",
            token_address="0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",  # noqa: S106 - test placeholder
            wallet_address="0x0000000000000000000000000000000000000001",
        )

        response = await zerog_only_service.QueryBalance(request, mock_context)

        assert response.success is False
        assert "not configured" in response.error
        assert "zerog" in response.error

    @pytest.mark.asyncio
    async def test_query_position_tokens_owed_rejects_unconfigured_chain(self, zerog_only_service, mock_context):
        """QueryPositionTokensOwed against a zerog-only gateway rejects arbitrum.

        tokensOwed feeds into LP teardown valuation — reading the wrong
        chain's NPM here would produce nonsense fee values and
        under/over-estimate the LP's worth by orders of magnitude.
        """
        request = gateway_pb2.PositionTokensOwedRequest(
            chain="arbitrum",
            position_manager="0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
            token_id=2359,
        )

        response = await zerog_only_service.QueryPositionTokensOwed(request, mock_context)

        assert response.success is False
        assert "not configured" in response.error
        assert "zerog" in response.error

    @pytest.mark.asyncio
    async def test_query_balance_accepts_configured_chain(self, zerog_only_service, mock_context):
        """Positive path: QueryBalance accepts zerog on a zerog-only gateway."""
        request = gateway_pb2.BalanceQueryRequest(
            chain="zerog",
            token_address="0x1Cd0690fF9a693f5EF2dD976660a8dAFc81A109c",  # noqa: S106 - W0G on 0G
            wallet_address="0x54776446Aa29Fc49d152B4850bD410eA1E4d24bF",
        )

        with patch.object(zerog_only_service, "_get_rpc_url", return_value="http://zerog-rpc"):
            with patch.object(zerog_only_service, "_make_rpc_call", return_value=("0xde0b6b3a7640000", None)):
                response = await zerog_only_service.QueryBalance(request, mock_context)

        assert response.success is True
        assert int(response.balance) == 10**18


class TestRpcServiceBlockPinning:
    """VIB-5140: the four typed queries pin their eth_call to ``request.block``.

    An omitted ``block`` (proto3 default "") MUST fall back to "latest"
    (backward-compatible). A supplied block tag MUST be threaded into the
    eth_call params as the block reference so a post-tx read pins to the
    confirmed receipt's block instead of an unpinned "latest" that a
    trailing read replica can answer with PRE-tx state.
    """

    @staticmethod
    def _block_arg(make_rpc_call_mock) -> str:
        """Extract the eth_call block tag from the captured _make_rpc_call.

        Signature: ``_make_rpc_call(rpc_url, method, params, label, ...)`` —
        ``params`` is the 3rd positional arg; its 2nd element is the block tag.
        """
        params = make_rpc_call_mock.call_args.args[2]
        return params[1]

    @pytest.mark.asyncio
    async def test_balance_omitted_block_defaults_to_latest(self, rpc_service, mock_context):
        request = gateway_pb2.BalanceQueryRequest(
            chain="arbitrum",
            token_address="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            wallet_address="0x54776446Aa29Fc49d152B4850bD410eA1E4d24bF",
        )
        with patch.object(rpc_service, "_get_rpc_url", return_value="http://test"):
            with patch.object(
                rpc_service, "_make_rpc_call", new=AsyncMock(return_value=("0x1", None))
            ) as call:
                await rpc_service.QueryBalance(request, mock_context)
        assert self._block_arg(call) == "latest"

    @pytest.mark.asyncio
    async def test_balance_pinned_block_threaded(self, rpc_service, mock_context):
        request = gateway_pb2.BalanceQueryRequest(
            chain="arbitrum",
            token_address="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            wallet_address="0x54776446Aa29Fc49d152B4850bD410eA1E4d24bF",
            block="0x1234abc",
        )
        with patch.object(rpc_service, "_get_rpc_url", return_value="http://test"):
            with patch.object(
                rpc_service, "_make_rpc_call", new=AsyncMock(return_value=("0x1", None))
            ) as call:
                await rpc_service.QueryBalance(request, mock_context)
        assert self._block_arg(call) == "0x1234abc"

    @pytest.mark.asyncio
    async def test_allowance_omitted_block_defaults_to_latest(self, rpc_service, mock_context):
        request = gateway_pb2.AllowanceRequest(
            chain="arbitrum",
            token_address="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            owner_address="0x54776446Aa29Fc49d152B4850bD410eA1E4d24bF",
            spender_address="0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
        )
        with patch.object(rpc_service, "_get_rpc_url", return_value="http://test"):
            with patch.object(
                rpc_service, "_make_rpc_call", new=AsyncMock(return_value=("0x1", None))
            ) as call:
                await rpc_service.QueryAllowance(request, mock_context)
        assert self._block_arg(call) == "latest"

    @pytest.mark.asyncio
    async def test_allowance_pinned_block_threaded(self, rpc_service, mock_context):
        request = gateway_pb2.AllowanceRequest(
            chain="arbitrum",
            token_address="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            owner_address="0x54776446Aa29Fc49d152B4850bD410eA1E4d24bF",
            spender_address="0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
            block="0xdeadbeef",
        )
        with patch.object(rpc_service, "_get_rpc_url", return_value="http://test"):
            with patch.object(
                rpc_service, "_make_rpc_call", new=AsyncMock(return_value=("0x1", None))
            ) as call:
                await rpc_service.QueryAllowance(request, mock_context)
        assert self._block_arg(call) == "0xdeadbeef"

    @pytest.mark.asyncio
    async def test_position_liquidity_omitted_block_defaults_to_latest(self, rpc_service, mock_context):
        request = gateway_pb2.PositionLiquidityRequest(
            chain="arbitrum",
            position_manager="0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
            token_id=5460223,
        )
        with patch.object(rpc_service, "_get_rpc_url", return_value="http://test"):
            with patch.object(
                rpc_service, "_make_rpc_call", new=AsyncMock(return_value=("0x" + "00" * 256, None))
            ) as call:
                await rpc_service.QueryPositionLiquidity(request, mock_context)
        assert self._block_arg(call) == "latest"

    @pytest.mark.asyncio
    async def test_position_liquidity_pinned_block_threaded(self, rpc_service, mock_context):
        request = gateway_pb2.PositionLiquidityRequest(
            chain="arbitrum",
            position_manager="0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
            token_id=5460223,
            block="0x1220a40",
        )
        with patch.object(rpc_service, "_get_rpc_url", return_value="http://test"):
            with patch.object(
                rpc_service, "_make_rpc_call", new=AsyncMock(return_value=("0x" + "00" * 256, None))
            ) as call:
                await rpc_service.QueryPositionLiquidity(request, mock_context)
        assert self._block_arg(call) == "0x1220a40"

    @pytest.mark.asyncio
    async def test_position_tokens_owed_omitted_block_defaults_to_latest(self, rpc_service, mock_context):
        request = gateway_pb2.PositionTokensOwedRequest(
            chain="arbitrum",
            position_manager="0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
            token_id=5460223,
        )
        with patch.object(rpc_service, "_get_rpc_url", return_value="http://test"):
            with patch.object(
                rpc_service, "_make_rpc_call", new=AsyncMock(return_value=("0x" + "00" * 256, None))
            ) as call:
                await rpc_service.QueryPositionTokensOwed(request, mock_context)
        assert self._block_arg(call) == "latest"

    @pytest.mark.asyncio
    async def test_position_tokens_owed_pinned_block_threaded(self, rpc_service, mock_context):
        request = gateway_pb2.PositionTokensOwedRequest(
            chain="arbitrum",
            position_manager="0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
            token_id=5460223,
            block="0x1220a40",
        )
        with patch.object(rpc_service, "_get_rpc_url", return_value="http://test"):
            with patch.object(
                rpc_service, "_make_rpc_call", new=AsyncMock(return_value=("0x" + "00" * 256, None))
            ) as call:
                await rpc_service.QueryPositionTokensOwed(request, mock_context)
        assert self._block_arg(call) == "0x1220a40"


class TestPositionQueryParsing:
    """Parse / validation branch coverage for the V3 position handlers.

    These exercise the result-decode and guard branches that the
    block-pinning tests (which stop at the eth_call boundary) do not, so the
    handlers stay well within the CRAP gate after the VIB-5140 block plumbing
    landed. The Anvil intent LP suite covers the same paths end-to-end in CI;
    these unit tests pin the behaviour without a live chain.
    """

    NPM = "0xC36442b4a4522E871399CD717aBDD847Ab11FE88"

    @pytest.mark.asyncio
    async def test_liquidity_rejects_unknown_chain(self, rpc_service, mock_context):
        request = gateway_pb2.PositionLiquidityRequest(chain="not_a_chain", position_manager=self.NPM, token_id=1)
        response = await rpc_service.QueryPositionLiquidity(request, mock_context)
        assert response.success is False

    @pytest.mark.asyncio
    async def test_liquidity_decodes_nonzero(self, rpc_service, mock_context):
        # 8 words; liquidity (word 7) = 0x...64 (=100).
        words = ["00" * 32] * 7 + [f"{100:064x}"]
        result = "0x" + "".join(words)
        with patch.object(rpc_service, "_get_rpc_url", return_value="http://test"):
            with patch.object(rpc_service, "_make_rpc_call", new=AsyncMock(return_value=(result, None))):
                request = gateway_pb2.PositionLiquidityRequest(chain="arbitrum", position_manager=self.NPM, token_id=1)
                response = await rpc_service.QueryPositionLiquidity(request, mock_context)
        assert response.success is True
        assert int(response.liquidity) == 100

    @pytest.mark.asyncio
    async def test_liquidity_position_not_found_on_empty(self, rpc_service, mock_context):
        with patch.object(rpc_service, "_get_rpc_url", return_value="http://test"):
            with patch.object(rpc_service, "_make_rpc_call", new=AsyncMock(return_value=("0x", None))):
                request = gateway_pb2.PositionLiquidityRequest(chain="arbitrum", position_manager=self.NPM, token_id=1)
                response = await rpc_service.QueryPositionLiquidity(request, mock_context)
        assert response.success is False
        assert "not found" in response.error

    @pytest.mark.asyncio
    async def test_liquidity_short_result_length(self, rpc_service, mock_context):
        with patch.object(rpc_service, "_get_rpc_url", return_value="http://test"):
            with patch.object(rpc_service, "_make_rpc_call", new=AsyncMock(return_value=("0x" + "00" * 16, None))):
                request = gateway_pb2.PositionLiquidityRequest(chain="arbitrum", position_manager=self.NPM, token_id=1)
                response = await rpc_service.QueryPositionLiquidity(request, mock_context)
        assert response.success is False
        assert "length" in response.error

    @pytest.mark.asyncio
    async def test_liquidity_rpc_error_propagates(self, rpc_service, mock_context):
        with patch.object(rpc_service, "_get_rpc_url", return_value="http://test"):
            with patch.object(
                rpc_service, "_make_rpc_call", new=AsyncMock(return_value=(None, {"message": "boom"}))
            ):
                request = gateway_pb2.PositionLiquidityRequest(chain="arbitrum", position_manager=self.NPM, token_id=1)
                response = await rpc_service.QueryPositionLiquidity(request, mock_context)
        assert response.success is False
        assert response.error == "boom"

    @pytest.mark.asyncio
    async def test_tokens_owed_decodes_nonzero(self, rpc_service, mock_context):
        # 12 words; tokensOwed0 (word 10) = 7, tokensOwed1 (word 11) = 9.
        words = ["00" * 32] * 10 + [f"{7:064x}", f"{9:064x}"]
        result = "0x" + "".join(words)
        with patch.object(rpc_service, "_get_rpc_url", return_value="http://test"):
            with patch.object(rpc_service, "_make_rpc_call", new=AsyncMock(return_value=(result, None))):
                request = gateway_pb2.PositionTokensOwedRequest(chain="arbitrum", position_manager=self.NPM, token_id=1)
                response = await rpc_service.QueryPositionTokensOwed(request, mock_context)
        assert response.success is True
        assert int(response.tokens_owed0) == 7
        assert int(response.tokens_owed1) == 9

    @pytest.mark.asyncio
    async def test_tokens_owed_position_not_found_on_empty(self, rpc_service, mock_context):
        with patch.object(rpc_service, "_get_rpc_url", return_value="http://test"):
            with patch.object(rpc_service, "_make_rpc_call", new=AsyncMock(return_value=("0x", None))):
                request = gateway_pb2.PositionTokensOwedRequest(chain="arbitrum", position_manager=self.NPM, token_id=1)
                response = await rpc_service.QueryPositionTokensOwed(request, mock_context)
        assert response.success is False
        assert "not found" in response.error

    @pytest.mark.asyncio
    async def test_tokens_owed_short_result_length(self, rpc_service, mock_context):
        with patch.object(rpc_service, "_get_rpc_url", return_value="http://test"):
            with patch.object(rpc_service, "_make_rpc_call", new=AsyncMock(return_value=("0x" + "00" * 16, None))):
                request = gateway_pb2.PositionTokensOwedRequest(chain="arbitrum", position_manager=self.NPM, token_id=1)
                response = await rpc_service.QueryPositionTokensOwed(request, mock_context)
        assert response.success is False
        assert "length" in response.error

    @pytest.mark.asyncio
    async def test_tokens_owed_rpc_error_propagates(self, rpc_service, mock_context):
        with patch.object(rpc_service, "_get_rpc_url", return_value="http://test"):
            with patch.object(
                rpc_service, "_make_rpc_call", new=AsyncMock(return_value=(None, {"message": "boom"}))
            ):
                request = gateway_pb2.PositionTokensOwedRequest(chain="arbitrum", position_manager=self.NPM, token_id=1)
                response = await rpc_service.QueryPositionTokensOwed(request, mock_context)
        assert response.success is False
        assert response.error == "boom"

    @pytest.mark.asyncio
    async def test_liquidity_negative_token_id_rejected(self, rpc_service, mock_context):
        # token_id is uint64 on the wire; negative cannot be constructed, so
        # cover the invalid-address guard instead (a sibling early-return).
        request = gateway_pb2.PositionLiquidityRequest(chain="arbitrum", position_manager="not_an_address", token_id=1)
        response = await rpc_service.QueryPositionLiquidity(request, mock_context)
        assert response.success is False

    @pytest.mark.asyncio
    async def test_tokens_owed_invalid_address_rejected(self, rpc_service, mock_context):
        request = gateway_pb2.PositionTokensOwedRequest(chain="arbitrum", position_manager="not_an_address", token_id=1)
        response = await rpc_service.QueryPositionTokensOwed(request, mock_context)
        assert response.success is False


class TestRpcServiceBatchCall:
    """Tests for RpcService.BatchCall."""

    @pytest.mark.asyncio
    async def test_batch_rejects_unknown_chain(self, rpc_service, mock_context):
        """BatchCall rejects unknown chains."""
        request = gateway_pb2.RpcBatchRequest(
            chain="unknown_chain",
            requests=[
                gateway_pb2.RpcRequest(method="eth_blockNumber", params="[]", id="1"),
            ],
        )

        response = await rpc_service.BatchCall(request, mock_context)

        assert len(response.responses) == 0
        mock_context.set_code.assert_called()

    @pytest.mark.asyncio
    async def test_batch_executes_multiple_calls(self, rpc_service, mock_context):
        """BatchCall executes multiple RPC calls."""
        request = gateway_pb2.RpcBatchRequest(
            chain="arbitrum",
            requests=[
                gateway_pb2.RpcRequest(method="eth_blockNumber", params="[]", id="1"),
                gateway_pb2.RpcRequest(method="eth_chainId", params="[]", id="2"),
            ],
        )

        with patch.object(rpc_service, "_get_rpc_url", return_value="http://test"):
            with patch.object(
                rpc_service,
                "_make_rpc_call",
                side_effect=[
                    ("0x100", None),
                    ("0xa4b1", None),  # Arbitrum chain ID
                ],
            ):
                response = await rpc_service.BatchCall(request, mock_context)

                assert len(response.responses) == 2
                assert response.responses[0].success is True
                assert response.responses[1].success is True


class TestRpcServiceMetrics:
    """Tests for RpcService metrics."""

    def test_metrics_initialized(self, rpc_service):
        """Metrics are initialized to zero."""
        metrics = rpc_service.get_metrics()

        assert metrics["total_requests"] == 0
        assert metrics["successful_requests"] == 0
        assert metrics["failed_requests"] == 0
        assert metrics["rate_limited_requests"] == 0
        # VIB-4985 / ALM-2777: indexer-lag retry counter is exposed for callers.
        assert metrics["indexer_lag_retries"] == 0


class TestMakeRpcCallErrorMessages:
    """Tests for _make_rpc_call error message clarity."""

    def _make_client_error_session(self, error_cls, *args):
        """Build a mock aiohttp session whose post() raises a ClientError from __aenter__."""
        import aiohttp

        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(side_effect=error_cls(*args))
        mock_cm.__aexit__ = AsyncMock(return_value=False)
        mock_session = MagicMock()
        mock_session.post.return_value = mock_cm
        return mock_session

    @pytest.mark.asyncio
    async def test_localhost_connection_error_mentions_local_rpc(self, rpc_service):
        """When connecting to localhost fails, error message mentions the local RPC."""
        import aiohttp

        mock_session = self._make_client_error_session(
            aiohttp.ClientConnectorError, MagicMock(), OSError("Connect call failed")
        )

        with patch.object(rpc_service, "_get_session", new=AsyncMock(return_value=mock_session)):
            result, error = await rpc_service._make_rpc_call("http://127.0.0.1:8546", "eth_blockNumber", [], "test")

        assert result is None
        assert error is not None
        assert "local RPC" in error["message"]
        assert "127.0.0.1" in error["message"]

    @pytest.mark.asyncio
    async def test_external_connection_error_generic_message(self, rpc_service):
        """When connecting to an external RPC fails, error message is generic."""
        import aiohttp

        mock_session = self._make_client_error_session(
            aiohttp.ClientConnectorError, MagicMock(), OSError("Connection refused")
        )

        with patch.object(rpc_service, "_get_session", new=AsyncMock(return_value=mock_session)):
            result, error = await rpc_service._make_rpc_call(
                "https://arb1.arbitrum.io/rpc", "eth_blockNumber", [], "test"
            )

        assert result is None
        assert error is not None
        assert "Network error" in error["message"]
        # Should NOT mention Anvil for external RPCs
        assert "Anvil" not in error["message"]
