"""Tests for AerodromeSDK transaction builders + pool queries.

Targets uncovered branches in:
- Pool queries (get_pool_address, get_pool_info, get_amount_out, get_amounts_out)
- TX builders (approve, swap, addLiquidity, removeLiquidity, wrap/unwrap)
- CL queries (get_cl_pool_address, get_cl_pool_slot0, get_cl_position)
- CL builders (build_cl_mint_tx, build_cl_decrease_liquidity_tx, build_cl_collect_tx)
- to_dict round trips for PoolInfo, CLPositionInfo, SwapRoute, SwapQuote
- get_token_symbol address branch
"""

from unittest.mock import MagicMock

import pytest

from almanak.connectors.aerodrome.sdk import (
    MAX_UINT256,
    AerodromeSDK,
    CLPositionInfo,
    PoolInfo,
    SwapQuote,
    SwapRoute,
)
from almanak.framework.data.tokens.exceptions import TokenResolutionError
from almanak.framework.data.tokens.models import ResolvedToken

USDC_ADDRESS = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"
WETH_ADDRESS = "0x4200000000000000000000000000000000000006"
TEST_WALLET = "0x1234567890123456789012345678901234567890"


def _make_resolver() -> MagicMock:
    mock = MagicMock()

    def _resolve(symbol_or_addr: str, *args: object, **kwargs: object) -> ResolvedToken:
        addr = symbol_or_addr.lower() if symbol_or_addr.startswith("0x") else None
        if symbol_or_addr in ("USDC",) or addr == USDC_ADDRESS.lower():
            return ResolvedToken(symbol="USDC", address=USDC_ADDRESS, decimals=6, chain="base", chain_id=8453)
        if symbol_or_addr in ("WETH",) or addr == WETH_ADDRESS.lower():
            return ResolvedToken(symbol="WETH", address=WETH_ADDRESS, decimals=18, chain="base", chain_id=8453)
        raise TokenResolutionError(token=symbol_or_addr, chain="base", reason="x")

    mock.resolve.side_effect = _resolve
    return mock


@pytest.fixture
def sdk() -> AerodromeSDK:
    return AerodromeSDK(chain="base", token_resolver=_make_resolver())


def _make_web3_with_contract(contract: MagicMock) -> MagicMock:
    """Return a MagicMock web3 whose `eth.contract(...)` returns the provided contract."""
    web3 = MagicMock()
    web3.eth.contract.return_value = contract
    web3.to_checksum_address.side_effect = lambda a: a  # passthrough
    web3.eth.get_transaction_count.return_value = 1
    return web3


# =============================================================================
# Pool queries
# =============================================================================


class TestGetPoolAddress:
    def test_returns_none_when_no_gateway_no_rpc(self, sdk: AerodromeSDK) -> None:
        sdk._gateway_client = None
        sdk.rpc_url = None
        # Patch get_rpc_url to also return None
        from unittest.mock import patch

        with patch("almanak.gateway.utils.rpc_provider.get_rpc_url", side_effect=ValueError("nope")):
            out = sdk.get_pool_address(USDC_ADDRESS, WETH_ADDRESS, False)
        assert out is None

    def test_returns_none_on_eth_call_exception(self, sdk: AerodromeSDK) -> None:
        sdk._gateway_client = MagicMock()
        from unittest.mock import patch

        with patch(
            "almanak.framework.web3.gateway_provider.GatewayWeb3Provider",
            side_effect=RuntimeError("boom"),
        ):
            out = sdk.get_pool_address(USDC_ADDRESS, WETH_ADDRESS, False)
        assert out is None


class TestGetPoolAddressFromFactory:
    def test_returns_address_when_pool_exists(self, sdk: AerodromeSDK) -> None:
        contract = MagicMock()
        pool_addr = "0xcDAC0d6c6C59727a65F871236188350531885C43"
        contract.functions.getPool.return_value.call.return_value = pool_addr
        web3 = _make_web3_with_contract(contract)
        out = sdk.get_pool_address_from_factory(USDC_ADDRESS, WETH_ADDRESS, False, web3)
        assert out == pool_addr

    def test_returns_none_when_zero_address(self, sdk: AerodromeSDK) -> None:
        contract = MagicMock()
        contract.functions.getPool.return_value.call.return_value = "0x0000000000000000000000000000000000000000"
        web3 = _make_web3_with_contract(contract)
        out = sdk.get_pool_address_from_factory(USDC_ADDRESS, WETH_ADDRESS, False, web3)
        assert out is None


class TestGetPoolInfo:
    def test_returns_none_when_pool_not_found(self, sdk: AerodromeSDK) -> None:
        contract = MagicMock()
        contract.functions.getPool.return_value.call.return_value = (
            "0x0000000000000000000000000000000000000000"
        )
        web3 = _make_web3_with_contract(contract)
        out = sdk.get_pool_info(USDC_ADDRESS, WETH_ADDRESS, False, web3)
        assert out is None

    def test_returns_pool_info_when_pool_found(self, sdk: AerodromeSDK) -> None:
        pool_addr = "0xcDAC0d6c6C59727a65F871236188350531885C43"
        # First call: getPool. Second contract: pool.metadata.
        web3 = MagicMock()
        web3.to_checksum_address.side_effect = lambda a: a

        factory_contract = MagicMock()
        factory_contract.functions.getPool.return_value.call.return_value = pool_addr

        pool_contract = MagicMock()
        # metadata returns (decimals0, decimals1, reserve0, reserve1, stable, token0, token1)
        pool_contract.functions.metadata.return_value.call.return_value = (
            6,                  # decimals0
            18,                 # decimals1
            1_000_000_000,      # reserve0
            5 * 10**17,         # reserve1
            False,              # stable
            USDC_ADDRESS,       # token0
            WETH_ADDRESS,       # token1
        )

        contracts = [factory_contract, pool_contract]
        web3.eth.contract.side_effect = lambda **kw: contracts.pop(0)

        out = sdk.get_pool_info(USDC_ADDRESS, WETH_ADDRESS, False, web3)
        assert out is not None
        assert out.address == pool_addr
        assert out.token0 == USDC_ADDRESS
        assert out.token1 == WETH_ADDRESS
        assert out.reserve0 == 1_000_000_000
        assert out.decimals0 == 6


class TestGetAmountOut:
    def test_returns_none_when_pool_not_found(self, sdk: AerodromeSDK) -> None:
        contract = MagicMock()
        contract.functions.getPool.return_value.call.return_value = (
            "0x0000000000000000000000000000000000000000"
        )
        web3 = _make_web3_with_contract(contract)
        out = sdk.get_amount_out(1_000_000, USDC_ADDRESS, WETH_ADDRESS, False, web3)
        assert out is None

    def test_returns_amount_when_pool_succeeds(self, sdk: AerodromeSDK) -> None:
        pool_addr = "0xcDAC0d6c6C59727a65F871236188350531885C43"
        web3 = MagicMock()
        web3.to_checksum_address.side_effect = lambda a: a
        factory_c = MagicMock()
        factory_c.functions.getPool.return_value.call.return_value = pool_addr
        pool_c = MagicMock()
        pool_c.functions.getAmountOut.return_value.call.return_value = 5 * 10**14
        contracts = [factory_c, pool_c]
        web3.eth.contract.side_effect = lambda **kw: contracts.pop(0)
        out = sdk.get_amount_out(1_000_000, USDC_ADDRESS, WETH_ADDRESS, False, web3)
        assert out == 5 * 10**14

    def test_returns_none_on_pool_exception(self, sdk: AerodromeSDK) -> None:
        pool_addr = "0xcDAC0d6c6C59727a65F871236188350531885C43"
        web3 = MagicMock()
        web3.to_checksum_address.side_effect = lambda a: a
        factory_c = MagicMock()
        factory_c.functions.getPool.return_value.call.return_value = pool_addr
        pool_c = MagicMock()
        pool_c.functions.getAmountOut.return_value.call.side_effect = RuntimeError("revert")
        contracts = [factory_c, pool_c]
        web3.eth.contract.side_effect = lambda **kw: contracts.pop(0)
        out = sdk.get_amount_out(1_000_000, USDC_ADDRESS, WETH_ADDRESS, False, web3)
        assert out is None


class TestGetAmountsOut:
    def test_returns_amounts_list(self, sdk: AerodromeSDK) -> None:
        contract = MagicMock()
        contract.functions.getAmountsOut.return_value.call.return_value = [1_000_000, 5 * 10**14]
        web3 = _make_web3_with_contract(contract)
        routes = [SwapRoute(from_token=USDC_ADDRESS, to_token=WETH_ADDRESS, stable=False)]
        out = sdk.get_amounts_out(1_000_000, routes, web3)
        assert out == [1_000_000, 5 * 10**14]

    def test_returns_none_on_router_exception(self, sdk: AerodromeSDK) -> None:
        contract = MagicMock()
        contract.functions.getAmountsOut.return_value.call.side_effect = RuntimeError("revert")
        web3 = _make_web3_with_contract(contract)
        routes = [SwapRoute(from_token=USDC_ADDRESS, to_token=WETH_ADDRESS, stable=False)]
        out = sdk.get_amounts_out(1_000_000, routes, web3)
        assert out is None


# =============================================================================
# TX builders
# =============================================================================


class TestSDKBuildTxs:
    def test_build_approve_tx(self, sdk: AerodromeSDK) -> None:
        contract = MagicMock()
        contract.functions.approve.return_value.build_transaction.return_value = {
            "to": USDC_ADDRESS, "gas": 50000,
        }
        web3 = _make_web3_with_contract(contract)
        tx = sdk.build_approve_tx(USDC_ADDRESS, "0xspender", MAX_UINT256, TEST_WALLET, web3)
        assert "to" in tx

    def test_build_swap_exact_tokens_tx_applies_gas_buffer(self, sdk: AerodromeSDK) -> None:
        contract = MagicMock()
        contract.functions.swapExactTokensForTokens.return_value.build_transaction.return_value = {
            "gas": 100_000,
        }
        web3 = _make_web3_with_contract(contract)
        routes = [SwapRoute(from_token=USDC_ADDRESS, to_token=WETH_ADDRESS, stable=False)]
        tx = sdk.build_swap_exact_tokens_tx(
            amount_in=1_000_000,
            amount_out_min=900_000,
            routes=routes,
            recipient=TEST_WALLET,
            deadline=1234567890,
            sender=TEST_WALLET,
            web3=web3,
        )
        # gas_buffer is 0.5 → 100k → 150k
        assert tx["gas"] == 150_000

    def test_build_add_liquidity_tx_applies_gas_buffer(self, sdk: AerodromeSDK) -> None:
        contract = MagicMock()
        contract.functions.addLiquidity.return_value.build_transaction.return_value = {"gas": 200_000}
        web3 = _make_web3_with_contract(contract)
        tx = sdk.build_add_liquidity_tx(
            token_a=USDC_ADDRESS,
            token_b=WETH_ADDRESS,
            stable=False,
            amount_a_desired=1_000_000,
            amount_b_desired=10**15,
            amount_a_min=0,
            amount_b_min=0,
            recipient=TEST_WALLET,
            deadline=1234567890,
            sender=TEST_WALLET,
            web3=web3,
        )
        assert tx["gas"] == 300_000

    def test_build_remove_liquidity_tx(self, sdk: AerodromeSDK) -> None:
        contract = MagicMock()
        contract.functions.removeLiquidity.return_value.build_transaction.return_value = {"gas": 200_000}
        web3 = _make_web3_with_contract(contract)
        tx = sdk.build_remove_liquidity_tx(
            token_a=USDC_ADDRESS,
            token_b=WETH_ADDRESS,
            stable=False,
            liquidity=10**18,
            amount_a_min=0,
            amount_b_min=0,
            recipient=TEST_WALLET,
            deadline=1234567890,
            sender=TEST_WALLET,
            web3=web3,
        )
        assert tx["gas"] == 300_000


class TestWrapUnwrapEth:
    def test_build_wrap_eth_tx_resolves_weth(self, sdk: AerodromeSDK) -> None:
        contract = MagicMock()
        contract.functions.deposit.return_value.build_transaction.return_value = {
            "to": WETH_ADDRESS,
            "gas": 30000,
        }
        web3 = _make_web3_with_contract(contract)
        tx = sdk.build_wrap_eth_tx(amount=10**18, sender=TEST_WALLET, web3=web3)
        assert tx["gas"] == 30000

    def test_build_wrap_eth_tx_raises_when_resolve_fails(self) -> None:
        # Resolver raises TokenResolutionError; SDK propagates as TokenResolutionError
        resolver = MagicMock()
        resolver.resolve.side_effect = TokenResolutionError(token="WETH", chain="base", reason="missing")
        sdk = AerodromeSDK(chain="base", token_resolver=resolver)
        web3 = _make_web3_with_contract(MagicMock())
        with pytest.raises(TokenResolutionError):
            sdk.build_wrap_eth_tx(amount=10**18, sender=TEST_WALLET, web3=web3)

    def test_build_unwrap_eth_tx(self, sdk: AerodromeSDK) -> None:
        contract = MagicMock()
        contract.functions.withdraw.return_value.build_transaction.return_value = {
            "gas": 30000,
        }
        web3 = _make_web3_with_contract(contract)
        tx = sdk.build_unwrap_eth_tx(amount=10**18, sender=TEST_WALLET, web3=web3)
        assert tx["gas"] == 30000

    def test_build_unwrap_eth_tx_raises_when_resolve_fails(self) -> None:
        resolver = MagicMock()
        resolver.resolve.side_effect = TokenResolutionError(token="WETH", chain="base", reason="missing")
        sdk = AerodromeSDK(chain="base", token_resolver=resolver)
        web3 = _make_web3_with_contract(MagicMock())
        with pytest.raises(TokenResolutionError):
            sdk.build_unwrap_eth_tx(amount=10**18, sender=TEST_WALLET, web3=web3)


# =============================================================================
# CL queries / builders
# =============================================================================


class TestCLPoolQueries:
    def test_get_cl_pool_address_no_factory_returns_none(self) -> None:
        sdk = AerodromeSDK(chain="optimism", token_resolver=_make_resolver())
        web3 = MagicMock()
        out = sdk.get_cl_pool_address(USDC_ADDRESS, WETH_ADDRESS, 100, web3)
        assert out is None

    def test_get_cl_pool_address_zero_returns_none(self, sdk: AerodromeSDK) -> None:
        contract = MagicMock()
        contract.functions.getPool.return_value.call.return_value = (
            "0x0000000000000000000000000000000000000000"
        )
        web3 = _make_web3_with_contract(contract)
        out = sdk.get_cl_pool_address(USDC_ADDRESS, WETH_ADDRESS, 100, web3)
        assert out is None

    def test_get_cl_pool_address_returns_address(self, sdk: AerodromeSDK) -> None:
        pool = "0xcDAC0d6c6C59727a65F871236188350531885C43"
        contract = MagicMock()
        contract.functions.getPool.return_value.call.return_value = pool
        web3 = _make_web3_with_contract(contract)
        out = sdk.get_cl_pool_address(USDC_ADDRESS, WETH_ADDRESS, 100, web3)
        assert out == pool

    def test_get_cl_pool_address_exception_returns_none(self, sdk: AerodromeSDK) -> None:
        web3 = MagicMock()
        web3.eth.contract.side_effect = RuntimeError("rpc")
        out = sdk.get_cl_pool_address(USDC_ADDRESS, WETH_ADDRESS, 100, web3)
        assert out is None

    def test_get_cl_pool_slot0_returns_tuple(self, sdk: AerodromeSDK) -> None:
        contract = MagicMock()
        contract.functions.slot0.return_value.call.return_value = (
            2**96, 100, 0, 1, 1, True,
        )
        web3 = _make_web3_with_contract(contract)
        out = sdk.get_cl_pool_slot0("0xcDAC0d6c6C59727a65F871236188350531885C43", web3)
        assert out is not None
        assert out[0] == 2**96
        assert out[1] == 100

    def test_get_cl_pool_slot0_exception_returns_none(self, sdk: AerodromeSDK) -> None:
        web3 = MagicMock()
        web3.eth.contract.side_effect = RuntimeError("rpc")
        out = sdk.get_cl_pool_slot0("0xcDAC0d6c6C59727a65F871236188350531885C43", web3)
        assert out is None


class TestCLPositionQueries:
    def test_get_cl_position_no_nft_returns_none(self) -> None:
        sdk = AerodromeSDK(chain="optimism", token_resolver=_make_resolver())
        web3 = MagicMock()
        out = sdk.get_cl_position(42, web3)
        assert out is None

    def test_get_cl_position_returns_info(self, sdk: AerodromeSDK) -> None:
        contract = MagicMock()
        contract.functions.positions.return_value.call.return_value = (
            0,                # nonce
            "0x0",            # operator
            USDC_ADDRESS,     # token0
            WETH_ADDRESS,     # token1
            100,              # tickSpacing
            -1000,            # tickLower
            1000,             # tickUpper
            10**18,           # liquidity
            0, 0,             # feeGrowth0/1
            500,              # tokensOwed0
            600,              # tokensOwed1
        )
        web3 = _make_web3_with_contract(contract)
        out = sdk.get_cl_position(42, web3)
        assert out is not None
        assert out.token_id == 42
        assert out.liquidity == 10**18
        assert out.tokens_owed0 == 500
        assert out.tokens_owed1 == 600

    def test_get_cl_position_exception_returns_none(self, sdk: AerodromeSDK) -> None:
        web3 = MagicMock()
        web3.eth.contract.side_effect = RuntimeError("rpc")
        out = sdk.get_cl_position(42, web3)
        assert out is None


class TestCLBuilders:
    def test_build_cl_mint_tx(self, sdk: AerodromeSDK) -> None:
        contract = MagicMock()
        contract.functions.mint.return_value.build_transaction.return_value = {
            "to": sdk.addresses["cl_nft"], "data": b"\x00" * 4,
        }
        web3 = _make_web3_with_contract(contract)
        tx = sdk.build_cl_mint_tx(
            token0=USDC_ADDRESS,
            token1=WETH_ADDRESS,
            tick_spacing=100,
            tick_lower=-100,
            tick_upper=100,
            amount0_desired=1_000_000,
            amount1_desired=10**15,
            amount0_min=0,
            amount1_min=0,
            recipient=TEST_WALLET,
            deadline=1234567890,
            sender=TEST_WALLET,
            web3=web3,
        )
        assert "to" in tx

    def test_build_cl_mint_tx_raises_when_no_cl_nft(self) -> None:
        sdk = AerodromeSDK(chain="optimism", token_resolver=_make_resolver())
        web3 = MagicMock()
        with pytest.raises(ValueError, match="cl_nft not configured"):
            sdk.build_cl_mint_tx(
                token0=USDC_ADDRESS,
                token1=WETH_ADDRESS,
                tick_spacing=100,
                tick_lower=-100,
                tick_upper=100,
                amount0_desired=1,
                amount1_desired=1,
                amount0_min=0,
                amount1_min=0,
                recipient=TEST_WALLET,
                deadline=1,
                sender=TEST_WALLET,
                web3=web3,
            )

    def test_build_cl_decrease_liquidity_tx(self, sdk: AerodromeSDK) -> None:
        contract = MagicMock()
        contract.functions.decreaseLiquidity.return_value.build_transaction.return_value = {
            "to": sdk.addresses["cl_nft"], "data": b"",
        }
        web3 = _make_web3_with_contract(contract)
        tx = sdk.build_cl_decrease_liquidity_tx(
            token_id=42,
            liquidity=10**18,
            amount0_min=0,
            amount1_min=0,
            deadline=1,
            sender=TEST_WALLET,
            web3=web3,
        )
        assert "to" in tx

    def test_build_cl_decrease_liquidity_tx_raises_when_no_cl_nft(self) -> None:
        sdk = AerodromeSDK(chain="optimism", token_resolver=_make_resolver())
        web3 = MagicMock()
        with pytest.raises(ValueError, match="cl_nft not configured"):
            sdk.build_cl_decrease_liquidity_tx(
                token_id=42,
                liquidity=1,
                amount0_min=0,
                amount1_min=0,
                deadline=1,
                sender=TEST_WALLET,
                web3=web3,
            )

    def test_build_cl_collect_tx(self, sdk: AerodromeSDK) -> None:
        contract = MagicMock()
        contract.functions.collect.return_value.build_transaction.return_value = {
            "to": sdk.addresses["cl_nft"], "data": b"",
        }
        web3 = _make_web3_with_contract(contract)
        tx = sdk.build_cl_collect_tx(
            token_id=42,
            recipient=TEST_WALLET,
            amount0_max=2**128 - 1,
            amount1_max=2**128 - 1,
            sender=TEST_WALLET,
            web3=web3,
        )
        assert "to" in tx

    def test_build_cl_collect_tx_raises_when_no_cl_nft(self) -> None:
        sdk = AerodromeSDK(chain="optimism", token_resolver=_make_resolver())
        web3 = MagicMock()
        with pytest.raises(ValueError, match="cl_nft not configured"):
            sdk.build_cl_collect_tx(
                token_id=42,
                recipient=TEST_WALLET,
                amount0_max=1,
                amount1_max=1,
                sender=TEST_WALLET,
                web3=web3,
            )


# =============================================================================
# Helper methods
# =============================================================================


class TestSDKHelpers:
    def test_resolve_token_address_passthrough(self, sdk: AerodromeSDK) -> None:
        assert sdk.resolve_token(USDC_ADDRESS) == USDC_ADDRESS

    def test_resolve_token_unresolvable_raises(self, sdk: AerodromeSDK) -> None:
        with pytest.raises(TokenResolutionError):
            sdk.resolve_token("UNKNOWN_SYMBOL_XX")

    def test_get_token_symbol_address_resolves(self, sdk: AerodromeSDK) -> None:
        assert sdk.get_token_symbol(USDC_ADDRESS) == "USDC"

    def test_get_token_symbol_non_address_returns_as_is(self, sdk: AerodromeSDK) -> None:
        # Doesn't start with "0x" — returns the value unchanged
        assert sdk.get_token_symbol("USDC") == "USDC"

    def test_get_token_decimals_unresolvable_raises(self, sdk: AerodromeSDK) -> None:
        with pytest.raises(TokenResolutionError):
            sdk.get_token_decimals("BAD_TOKEN_SYM")

    def test_load_abi_missing_returns_empty_list(self, sdk: AerodromeSDK) -> None:
        out = sdk._load_abi("does_not_exist_abi_name")
        assert out == []


# =============================================================================
# Dataclass to_dict / from_dict
# =============================================================================


class TestSDKDataClasses:
    def test_pool_info_to_dict(self) -> None:
        p = PoolInfo(
            address="0xa", token0="0xb", token1="0xc", stable=False,
            reserve0=100, reserve1=200, decimals0=6, decimals1=18,
        )
        d = p.to_dict()
        assert d["reserve0"] == "100"
        assert d["decimals1"] == 18

    def test_cl_position_info_to_dict(self) -> None:
        p = CLPositionInfo(
            token_id=1, token0="0xa", token1="0xb",
            tick_spacing=100, tick_lower=-100, tick_upper=100,
            liquidity=10**18, tokens_owed0=10, tokens_owed1=20,
        )
        d = p.to_dict()
        assert d["liquidity"] == str(10**18)
        assert d["token_id"] == 1

    def test_swap_route_to_dict(self) -> None:
        r = SwapRoute(from_token=USDC_ADDRESS, to_token=WETH_ADDRESS, stable=True)
        d = r.to_dict()
        assert d["from"] == USDC_ADDRESS
        assert d["stable"] is True

    def test_swap_quote_to_dict(self) -> None:
        q = SwapQuote(
            amount_in=10, amount_out=20,
            routes=[SwapRoute(from_token=USDC_ADDRESS, to_token=WETH_ADDRESS, stable=False)],
        )
        d = q.to_dict()
        assert d["amount_in"] == "10"
        assert d["amount_out"] == "20"
        assert len(d["routes"]) == 1
