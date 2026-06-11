"""Unit tests for the paper-engine position query functions.

The six ``query_*_positions`` entry points (uniswap / gmx / aave, async +
sync) resolve connector-owned addresses through ``AddressRegistry`` and then
enumerate on-chain positions via per-item ``_query_*`` RPC helpers. These
tests patch the RPC helpers (never the registry) so the resolution, default
tables, loop dispatch, and empty-result paths run for real without a node.
"""

from unittest.mock import AsyncMock, patch

import pytest
from web3 import Web3

import almanak.framework.backtesting.paper.position_queries as pq

_WALLET = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
_MODULE = "almanak.framework.backtesting.paper.position_queries"


class _FakeWeb3:
    """Just enough web3 surface for the query functions."""

    @staticmethod
    def to_checksum_address(value: str) -> str:
        return Web3.to_checksum_address(value)


@pytest.fixture
def web3() -> _FakeWeb3:
    return _FakeWeb3()


def _univ3_position(*, token_id: int, fee: int, liquidity: int) -> "pq.UniswapV3Position":
    return pq.UniswapV3Position(
        token_id=token_id,
        nonce=0,
        operator="0x" + "00" * 20,
        token0="0x" + "11" * 20,
        token1="0x" + "22" * 20,
        fee=fee,
        tick_lower=-100,
        tick_upper=100,
        liquidity=liquidity,
        fee_growth_inside0_last_x128=0,
        fee_growth_inside1_last_x128=0,
        tokens_owed0=0,
        tokens_owed1=0,
    )



class TestUniswapV3PositionQueries:
    @pytest.mark.asyncio
    async def test_async_no_positions(self, web3):
        """Default position-manager resolution + zero-balance early exit."""
        with patch(f"{_MODULE}._query_balance_of", AsyncMock(return_value=0)) as balance_mock:
            positions = await pq.query_uniswap_v3_positions(_WALLET, web3, chain="ethereum")
        assert positions == []
        contract = balance_mock.call_args.args[1]
        assert contract.startswith("0x")  # registry-resolved position manager

    @pytest.mark.asyncio
    async def test_async_enumerates_and_skips_failed_token_ids(self, web3):
        """The token-id loop appends found positions and warns on None ids."""
        position = _univ3_position(token_id=101, fee=500, liquidity=1_000)
        with (
            patch(f"{_MODULE}._query_balance_of", AsyncMock(return_value=2)),
            patch(
                f"{_MODULE}._query_token_of_owner_by_index",
                AsyncMock(side_effect=[101, None]),
            ),
            patch(f"{_MODULE}._query_position", AsyncMock(return_value=position)),
        ):
            positions = await pq.query_uniswap_v3_positions(_WALLET, web3, chain="ethereum")
        assert positions == [position]

    @pytest.mark.asyncio
    async def test_async_unsupported_chain_raises(self, web3):
        with pytest.raises(ValueError, match="Unsupported chain: notachain"):
            await pq.query_uniswap_v3_positions(_WALLET, web3, chain="notachain")

    def test_sync_no_positions(self, web3):
        with patch(f"{_MODULE}._query_balance_of_sync", return_value=0):
            assert pq.query_uniswap_v3_positions_sync(_WALLET, web3, chain="ethereum") == []

    def test_sync_enumerates_positions(self, web3):
        position = _univ3_position(token_id=7, fee=3000, liquidity=5)
        with (
            patch(f"{_MODULE}._query_balance_of_sync", return_value=1),
            patch(f"{_MODULE}._query_token_of_owner_by_index_sync", return_value=7),
            patch(f"{_MODULE}._query_position_sync", return_value=position),
        ):
            assert pq.query_uniswap_v3_positions_sync(_WALLET, web3, chain="ethereum") == [position]


class TestGmxPositionQueries:
    @pytest.mark.asyncio
    async def test_async_default_markets_no_positions(self, web3):
        """Default market/collateral tables drive the full combination loop."""
        with patch(f"{_MODULE}._query_gmx_position", AsyncMock(return_value=None)) as query_mock:
            positions = await pq.query_gmx_positions(_WALLET, web3, chain="arbitrum")
        assert positions == []
        markets = pq.GMX_V2_MARKETS.get("arbitrum", {})
        collaterals = pq.GMX_V2_COLLATERAL_TOKENS.get("arbitrum", {})
        assert query_mock.await_count == len(markets) * len(collaterals) * 2

    @pytest.mark.asyncio
    async def test_async_unsupported_chain_raises(self, web3):
        with pytest.raises(ValueError, match="Unsupported chain: notachain"):
            await pq.query_gmx_positions(_WALLET, web3, chain="notachain")

    def test_sync_explicit_markets_no_positions(self, web3):
        market = "0x" + "33" * 20
        collateral = "0x" + "44" * 20
        with patch(f"{_MODULE}._query_gmx_position_sync", return_value=None) as query_mock:
            positions = pq.query_gmx_positions_sync(
                _WALLET,
                web3,
                chain="arbitrum",
                markets=[market],
                collateral_tokens=[collateral],
            )
        assert positions == []
        assert query_mock.call_count == 2  # one market x one collateral x 2 directions


class TestAavePositionQueries:
    @pytest.mark.asyncio
    async def test_async_default_assets_no_positions(self, web3):
        """Default per-chain token table drives the asset loop."""
        with patch(f"{_MODULE}._query_aave_user_reserve_data", AsyncMock(return_value=None)) as query_mock:
            positions = await pq.query_aave_positions(_WALLET, web3, chain="arbitrum")
        assert positions == []
        assert query_mock.await_count == len(pq.AAVE_V3_TOKENS.get("arbitrum", {}))

    @pytest.mark.asyncio
    async def test_async_unsupported_chain_raises(self, web3):
        with pytest.raises(ValueError, match="Unsupported chain: notachain"):
            await pq.query_aave_positions(_WALLET, web3, chain="notachain")

    def test_sync_collects_active_positions(self, web3):
        asset = "0x" + "55" * 20
        active = pq.AaveV3LendingPosition(
            asset="USDC",
            asset_address=asset,
            current_atoken_balance=1_000_000,
            current_stable_debt=0,
            current_variable_debt=0,
            principal_stable_debt=0,
            scaled_variable_debt=0,
            stable_borrow_rate=0,
            liquidity_rate=0,
            usage_as_collateral_enabled=True,
            decimals=6,
        )
        with patch(f"{_MODULE}._query_aave_user_reserve_data_sync", return_value=active):
            positions = pq.query_aave_positions_sync(_WALLET, web3, chain="arbitrum", assets=[asset])
        assert positions == [active]
