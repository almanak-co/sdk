"""Tests for TraderJoeV2Adapter higher-level methods.

Targets uncovered branches in `adapter.py`:
- get_swap_quote: success + getSwapOut failure
- build_swap_transaction: matched/mismatched quote
- swap_exact_input: missing private key short-circuit + execution failure
- get_position: pool-missing + balances-empty + happy path
- build_add_liquidity_transaction: single-sided LP variants
- build_remove_liquidity_transaction: missing-position + position-supplied paths
- get_pending_fees / build_collect_fees_transaction: present and absent positions
- TraderJoeV2Config / chain validation errors
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.connectors.traderjoe_v2.adapter import (
    LiquidityPosition,
    SwapQuote,
    TraderJoeV2Adapter,
    TraderJoeV2Config,
)
from almanak.framework.connectors.traderjoe_v2.sdk import (
    BIN_ID_OFFSET,
    PoolNotFoundError,
    TraderJoeV2SDKError,
)
from almanak.framework.data.tokens.models import ResolvedToken

WALLET = "0x1234567890123456789012345678901234567890"
TOKEN_X = "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7"  # WAVAX
TOKEN_Y = "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E"  # USDC
POOL_ADDR = "0xD446eb1660F766d533BeCeEf890Df7A69d26f7d1"
ROUTER = "0xb4315e873dBcf96Ffd0acd8EA43f689D8c20fB30"


def _resolved(symbol_or_addr: str, decimals: int = 18) -> ResolvedToken:
    """Map symbol to address, leave addresses untouched."""
    address_map = {
        "WAVAX": TOKEN_X,
        "USDC": TOKEN_Y,
    }
    address = address_map.get(symbol_or_addr, symbol_or_addr)
    return ResolvedToken(
        symbol=symbol_or_addr,
        address=address,
        decimals=decimals,
        chain="avalanche",
        chain_id=43114,
    )


@pytest.fixture
def mock_resolver() -> MagicMock:
    """Mock TokenResolver that returns 18-decimal tokens by default but
    handles USDC and TOKEN_Y address as 6 decimals."""
    resolver = MagicMock()

    def resolve(token: str, chain: str) -> ResolvedToken:
        if token == "USDC" or token.lower() == TOKEN_Y.lower():
            return _resolved(token, decimals=6)
        return _resolved(token, decimals=18)

    resolver.resolve.side_effect = resolve
    return resolver


@pytest.fixture
def config() -> TraderJoeV2Config:
    return TraderJoeV2Config(
        chain="avalanche",
        wallet_address=WALLET,
        rpc_url="http://anvil:8545",
    )


@pytest.fixture
def adapter_with_mock_sdk(config: TraderJoeV2Config, mock_resolver: MagicMock) -> tuple[TraderJoeV2Adapter, MagicMock]:
    """Return adapter + the SDK mock for direct manipulation."""
    with patch("almanak.framework.connectors.traderjoe_v2.adapter.TraderJoeV2SDK") as mock_sdk_cls:
        mock_sdk = MagicMock()
        mock_sdk.router_address = ROUTER
        mock_sdk_cls.return_value = mock_sdk
        adapter = TraderJoeV2Adapter(config, token_resolver=mock_resolver)
    return adapter, mock_sdk


# =============================================================================
# Config / construction
# =============================================================================


class TestConfigValidation:
    def test_config_post_init_requires_rpc_or_gateway(self) -> None:
        with pytest.raises(TraderJoeV2SDKError, match="requires either rpc_url"):
            TraderJoeV2Config(chain="avalanche", wallet_address=WALLET)

    def test_config_with_rpc_only(self) -> None:
        cfg = TraderJoeV2Config(chain="avalanche", wallet_address=WALLET, rpc_url="http://x")
        assert cfg.rpc_url == "http://x"

    def test_unsupported_chain_raises_in_adapter(self) -> None:
        cfg = TraderJoeV2Config(chain="solana", wallet_address=WALLET, rpc_url="http://x")
        with pytest.raises(TraderJoeV2SDKError, match="not supported"):
            TraderJoeV2Adapter(cfg)


# =============================================================================
# wei conversion + chain id
# =============================================================================


class TestWeiHelpers:
    def test_to_wei_18_decimals(self, adapter_with_mock_sdk) -> None:
        adapter, _ = adapter_with_mock_sdk
        # WAVAX → 18 decimals
        result = adapter.to_wei(Decimal("1.5"), "WAVAX")
        assert result == int(Decimal("1.5") * Decimal(10**18))

    def test_to_wei_6_decimals(self, adapter_with_mock_sdk) -> None:
        adapter, _ = adapter_with_mock_sdk
        result = adapter.to_wei(Decimal("100"), "USDC")
        assert result == 100 * 10**6

    def test_from_wei_round_trip(self, adapter_with_mock_sdk) -> None:
        adapter, _ = adapter_with_mock_sdk
        wei = adapter.to_wei(Decimal("2.5"), "WAVAX")
        recovered = adapter.from_wei(wei, "WAVAX")
        assert recovered == Decimal("2.5")

    def test_get_chain_id_avalanche(self, adapter_with_mock_sdk) -> None:
        adapter, _ = adapter_with_mock_sdk
        assert adapter._get_chain_id() == 43114


# =============================================================================
# get_swap_quote
# =============================================================================


def _wire_quote_dependencies(
    sdk: MagicMock,
    *,
    swap_for_y: bool = True,
    spot_rate: float = 25.0,
    swap_out_returns: tuple[int, int, int] = (0, 25 * 10**6, 100),
) -> None:
    """Wire SDK mock so get_swap_quote can succeed."""
    sdk.get_pool_address.return_value = POOL_ADDR

    pool_info = MagicMock()
    pool_info.token_x = TOKEN_X if swap_for_y else TOKEN_Y
    pool_info.token_y = TOKEN_Y if swap_for_y else TOKEN_X
    pool_info.bin_step = 20
    pool_info.active_id = BIN_ID_OFFSET
    sdk.get_pool_info.return_value = pool_info

    pair = MagicMock()
    pair.address = POOL_ADDR
    sdk.get_pair_contract.return_value = pair

    router = MagicMock()
    router.functions.getSwapOut.return_value.call.return_value = swap_out_returns
    sdk._router_contract = router

    sdk.get_pool_spot_rate.return_value = spot_rate


class TestGetSwapQuote:
    def test_swap_for_y_happy_path(self, adapter_with_mock_sdk) -> None:
        adapter, sdk = adapter_with_mock_sdk
        # 1 WAVAX in (TOKEN_X) -> ~25 USDC out (TOKEN_Y in 6 decimals: 25 * 10**6)
        _wire_quote_dependencies(sdk, swap_for_y=True, spot_rate=25.0, swap_out_returns=(0, 25 * 10**6, 100))

        quote = adapter.get_swap_quote("WAVAX", "USDC", Decimal("1"), bin_step=20)
        assert quote.amount_in == Decimal("1")
        assert quote.amount_out == Decimal("25")
        assert quote.bin_step == 20

    def test_swap_for_x_branch(self, adapter_with_mock_sdk) -> None:
        """When token_in is the pool's token_y, swap_for_y is False
        and price impact is computed via 1/price."""
        adapter, sdk = adapter_with_mock_sdk
        # USDC (TOKEN_Y) in -> WAVAX (TOKEN_X) out: pool_info.token_x=TOKEN_X, but token_in is USDC
        _wire_quote_dependencies(
            sdk, swap_for_y=False, spot_rate=0.04, swap_out_returns=(0, int(0.04 * 10**18), 100)
        )
        # Override pool_info so swap_for_y is False (token_in is not pool.token_x)
        pool_info = MagicMock()
        pool_info.token_x = TOKEN_X
        pool_info.token_y = TOKEN_Y
        pool_info.bin_step = 20
        pool_info.active_id = BIN_ID_OFFSET
        sdk.get_pool_info.return_value = pool_info

        quote = adapter.get_swap_quote("USDC", "WAVAX", Decimal("1"), bin_step=20)
        assert quote.amount_out == Decimal("0.04")
        # Price impact branch executed without error.
        assert quote.price_impact >= Decimal(0)

    def test_zero_amount_in_yields_zero_price(self, adapter_with_mock_sdk) -> None:
        adapter, sdk = adapter_with_mock_sdk
        _wire_quote_dependencies(sdk, swap_out_returns=(0, 0, 0))
        quote = adapter.get_swap_quote("WAVAX", "USDC", Decimal("0"), bin_step=20)
        assert quote.price == Decimal(0)
        assert quote.price_impact == Decimal(0)

    def test_router_quote_failure_raises(self, adapter_with_mock_sdk) -> None:
        adapter, sdk = adapter_with_mock_sdk
        _wire_quote_dependencies(sdk)
        sdk._router_contract.functions.getSwapOut.return_value.call.side_effect = Exception("revert: pool empty")

        with pytest.raises(TraderJoeV2SDKError, match="On-chain quote failed"):
            adapter.get_swap_quote("WAVAX", "USDC", Decimal("1"), bin_step=20)


# =============================================================================
# build_swap_transaction
# =============================================================================


class TestBuildSwapTransaction:
    def test_uses_provided_quote_when_matched(self, adapter_with_mock_sdk) -> None:
        adapter, sdk = adapter_with_mock_sdk
        sdk.build_swap_exact_tokens_for_tokens.return_value = (
            {"to": ROUTER, "data": "0xabcd", "value": 0},
            200_000,
        )
        # Provide a matched quote so get_swap_quote is NOT called.
        quote = SwapQuote(
            token_in="WAVAX",
            token_out="USDC",
            amount_in=Decimal("1"),
            amount_out=Decimal("25"),
            bin_step=20,
            price=Decimal("25"),
            price_impact=Decimal("0"),
            path=[TOKEN_X, TOKEN_Y],
        )

        tx = adapter.build_swap_transaction("WAVAX", "USDC", Decimal("1"), bin_step=20, quote=quote)
        assert tx.to == ROUTER
        # get_swap_quote should NOT have been triggered.
        sdk.get_pool_address.assert_not_called()

    def test_mismatched_quote_raises(self, adapter_with_mock_sdk) -> None:
        adapter, sdk = adapter_with_mock_sdk
        # Quote for WAVAX/USDC but request for USDC/WAVAX → mismatch.
        quote = SwapQuote(
            token_in="WAVAX",
            token_out="USDC",
            amount_in=Decimal("1"),
            amount_out=Decimal("25"),
            bin_step=20,
            price=Decimal("25"),
            price_impact=Decimal("0"),
            path=[TOKEN_X, TOKEN_Y],
        )
        with pytest.raises(TraderJoeV2SDKError, match="does not match"):
            adapter.build_swap_transaction("USDC", "WAVAX", Decimal("1"), bin_step=20, quote=quote)

    def test_fetches_quote_when_not_provided(self, adapter_with_mock_sdk) -> None:
        adapter, sdk = adapter_with_mock_sdk
        _wire_quote_dependencies(sdk)
        sdk.build_swap_exact_tokens_for_tokens.return_value = (
            {"to": ROUTER, "data": b"\xab\xcd", "value": 0},
            200_000,
        )

        tx = adapter.build_swap_transaction("WAVAX", "USDC", Decimal("1"), bin_step=20)
        # data was bytes — should be hex-encoded
        assert tx.data == b"\xab\xcd".hex()
        sdk.get_pool_address.assert_called_once()


# =============================================================================
# swap_exact_input
# =============================================================================


class TestSwapExactInput:
    def test_returns_failure_when_no_private_key(self, adapter_with_mock_sdk) -> None:
        adapter, _ = adapter_with_mock_sdk
        result = adapter.swap_exact_input("WAVAX", "USDC", Decimal("1"))
        assert result.success is False
        assert "Private key" in (result.error or "")

    def test_catches_build_failure(self, mock_resolver: MagicMock) -> None:
        """Adapter wraps build/sign/send exceptions into a failed SwapResult."""
        cfg = TraderJoeV2Config(
            chain="avalanche",
            wallet_address=WALLET,
            rpc_url="http://anvil:8545",
            private_key="0x" + "11" * 32,
        )
        with patch("almanak.framework.connectors.traderjoe_v2.adapter.TraderJoeV2SDK") as mock_sdk_cls:
            mock_sdk = MagicMock()
            mock_sdk.router_address = ROUTER
            mock_sdk_cls.return_value = mock_sdk
            adapter = TraderJoeV2Adapter(cfg, token_resolver=mock_resolver)
            # Force build_swap_transaction to fail by making get_pool_address raise.
            mock_sdk.get_pool_address.side_effect = Exception("rpc down")

            result = adapter.swap_exact_input("WAVAX", "USDC", Decimal("1"))
        assert result.success is False
        assert result.error  # populated


# =============================================================================
# get_position
# =============================================================================


class TestGetPosition:
    def test_returns_none_when_pool_missing(self, adapter_with_mock_sdk) -> None:
        adapter, sdk = adapter_with_mock_sdk
        sdk.get_pool_address.side_effect = PoolNotFoundError(TOKEN_X, TOKEN_Y, 20)
        result = adapter.get_position("WAVAX", "USDC", bin_step=20)
        assert result is None

    def test_returns_none_when_no_balances(self, adapter_with_mock_sdk) -> None:
        adapter, sdk = adapter_with_mock_sdk
        sdk.get_pool_address.return_value = POOL_ADDR
        sdk.get_position_balances.return_value = {}
        result = adapter.get_position("WAVAX", "USDC", bin_step=20)
        assert result is None

    def test_returns_position_when_balances_present(self, adapter_with_mock_sdk) -> None:
        adapter, sdk = adapter_with_mock_sdk
        sdk.get_pool_address.return_value = POOL_ADDR
        sdk.get_position_balances.return_value = {BIN_ID_OFFSET: 1_000}
        sdk.get_total_position_value.return_value = (10, 20)
        pool_info = MagicMock()
        pool_info.token_x = TOKEN_X
        pool_info.token_y = TOKEN_Y
        pool_info.bin_step = 20
        pool_info.active_id = BIN_ID_OFFSET
        sdk.get_pool_info.return_value = pool_info

        position = adapter.get_position("WAVAX", "USDC", bin_step=20)
        assert position is not None
        assert position.amount_x == 10
        assert position.amount_y == 20
        assert position.bin_ids == [BIN_ID_OFFSET]

    def test_uses_explicit_wallet_when_provided(self, adapter_with_mock_sdk) -> None:
        adapter, sdk = adapter_with_mock_sdk
        sdk.get_pool_address.return_value = POOL_ADDR
        sdk.get_position_balances.return_value = {}
        custom_wallet = "0xabc0123456789012345678901234567890123def"
        adapter.get_position("WAVAX", "USDC", bin_step=20, wallet=custom_wallet)
        # Verify the wallet override propagated.
        sdk.get_position_balances.assert_called_once_with(POOL_ADDR, custom_wallet)


# =============================================================================
# build_add_liquidity_transaction
# =============================================================================


class TestBuildAddLiquidity:
    def _wire(self, sdk: MagicMock) -> None:
        sdk.get_pool_address.return_value = POOL_ADDR
        pool_info = MagicMock()
        pool_info.token_x = TOKEN_X
        pool_info.token_y = TOKEN_Y
        pool_info.bin_step = 20
        pool_info.active_id = BIN_ID_OFFSET
        sdk.get_pool_info.return_value = pool_info
        sdk.build_add_liquidity.return_value = (
            {"to": ROUTER, "data": "0xa3c7271a", "value": 0},
            700_000,
        )

    def test_dual_sided_lp_distribution(self, adapter_with_mock_sdk) -> None:
        adapter, sdk = adapter_with_mock_sdk
        self._wire(sdk)

        tx = adapter.build_add_liquidity_transaction(
            "WAVAX",
            "USDC",
            amount_x=Decimal("1"),
            amount_y=Decimal("25"),
            bin_step=20,
            bin_range=2,
        )
        assert tx.to == ROUTER
        assert tx.gas == 700_000

        # Check distributions: amount_x_wei > 0 → distribution_x has shares above active.
        # amount_y_wei > 0 → distribution_y has shares including active and below.
        kwargs = sdk.build_add_liquidity.call_args.kwargs
        assert sum(kwargs["distribution_x"]) == 10**18
        assert sum(kwargs["distribution_y"]) == 10**18
        # bin_range=2 → 5 bins total (-2,-1,0,+1,+2)
        assert len(kwargs["delta_ids"]) == 5

    def test_single_sided_lp_x_only(self, adapter_with_mock_sdk) -> None:
        adapter, sdk = adapter_with_mock_sdk
        self._wire(sdk)

        adapter.build_add_liquidity_transaction(
            "WAVAX", "USDC", amount_x=Decimal("1"), amount_y=Decimal("0"), bin_step=20, bin_range=2
        )
        kwargs = sdk.build_add_liquidity.call_args.kwargs
        # Y distribution must be all zero since amount_y_wei=0.
        assert sum(kwargs["distribution_y"]) == 0
        # X distribution must sum to 10**18.
        assert sum(kwargs["distribution_x"]) == 10**18

    def test_single_sided_lp_y_only(self, adapter_with_mock_sdk) -> None:
        adapter, sdk = adapter_with_mock_sdk
        self._wire(sdk)

        adapter.build_add_liquidity_transaction(
            "WAVAX", "USDC", amount_x=Decimal("0"), amount_y=Decimal("100"), bin_step=20, bin_range=3
        )
        kwargs = sdk.build_add_liquidity.call_args.kwargs
        assert sum(kwargs["distribution_x"]) == 0
        assert sum(kwargs["distribution_y"]) == 10**18


# =============================================================================
# build_remove_liquidity_transaction
# =============================================================================


class TestBuildRemoveLiquidity:
    def test_returns_none_when_no_position(self, adapter_with_mock_sdk) -> None:
        adapter, sdk = adapter_with_mock_sdk
        sdk.get_pool_address.return_value = POOL_ADDR
        sdk.get_position_balances.return_value = {}
        result = adapter.build_remove_liquidity_transaction("WAVAX", "USDC", bin_step=20)
        assert result is None

    def test_uses_supplied_position(self, adapter_with_mock_sdk) -> None:
        adapter, sdk = adapter_with_mock_sdk
        sdk.build_remove_liquidity.return_value = (
            {"to": ROUTER, "data": "0xc22159b6", "value": 0},
            400_000,
        )
        position = LiquidityPosition(
            pool_address=POOL_ADDR,
            token_x=TOKEN_X,
            token_y=TOKEN_Y,
            bin_step=20,
            bin_ids=[BIN_ID_OFFSET],
            balances={BIN_ID_OFFSET: 1_000},
            amount_x=10**18,
            amount_y=25 * 10**6,
            active_bin=BIN_ID_OFFSET,
        )
        tx = adapter.build_remove_liquidity_transaction("WAVAX", "USDC", bin_step=20, position=position)
        assert tx is not None
        # get_position should not be called (position supplied).
        sdk.get_position_balances.assert_not_called()
        kwargs = sdk.build_remove_liquidity.call_args.kwargs
        # amount_x_min computed from position.amount_x with default slippage 50 bps
        # → amount_x * (10000 - 50) / 10000 = amount_x * 9950 / 10000
        assert kwargs["amount_x_min"] == int(position.amount_x * 9950 // 10000)

    def test_explicit_amount_min_overrides_slippage_calc(self, adapter_with_mock_sdk) -> None:
        adapter, sdk = adapter_with_mock_sdk
        sdk.build_remove_liquidity.return_value = (
            {"to": ROUTER, "data": "0xdeadbeef", "value": 0},
            400_000,
        )
        position = LiquidityPosition(
            pool_address=POOL_ADDR,
            token_x=TOKEN_X,
            token_y=TOKEN_Y,
            bin_step=20,
            bin_ids=[BIN_ID_OFFSET],
            balances={BIN_ID_OFFSET: 1_000},
            amount_x=10**18,
            amount_y=25 * 10**6,
            active_bin=BIN_ID_OFFSET,
        )
        adapter.build_remove_liquidity_transaction(
            "WAVAX", "USDC", bin_step=20, position=position, amount_x_min=42, amount_y_min=84
        )
        kwargs = sdk.build_remove_liquidity.call_args.kwargs
        assert kwargs["amount_x_min"] == 42
        assert kwargs["amount_y_min"] == 84

    def test_partial_explicit_amount_min_calcs_other_side(self, adapter_with_mock_sdk) -> None:
        """If only amount_x_min is supplied, amount_y_min still uses slippage calc."""
        adapter, sdk = adapter_with_mock_sdk
        sdk.build_remove_liquidity.return_value = (
            {"to": ROUTER, "data": "0xdeadbeef", "value": 0},
            400_000,
        )
        position = LiquidityPosition(
            pool_address=POOL_ADDR,
            token_x=TOKEN_X,
            token_y=TOKEN_Y,
            bin_step=20,
            bin_ids=[BIN_ID_OFFSET],
            balances={BIN_ID_OFFSET: 1_000},
            amount_x=10**18,
            amount_y=25 * 10**6,
            active_bin=BIN_ID_OFFSET,
        )
        adapter.build_remove_liquidity_transaction(
            "WAVAX", "USDC", bin_step=20, position=position, amount_x_min=999
        )
        kwargs = sdk.build_remove_liquidity.call_args.kwargs
        assert kwargs["amount_x_min"] == 999
        # amount_y_min still gets slippage discount from position.amount_y.
        assert kwargs["amount_y_min"] == int(position.amount_y * 9950 // 10000)


# =============================================================================
# Fee operations
# =============================================================================


class TestPendingFeesAndCollect:
    def test_get_pending_fees_returns_none_without_position(self, adapter_with_mock_sdk) -> None:
        adapter, sdk = adapter_with_mock_sdk
        sdk.get_pool_address.return_value = POOL_ADDR
        sdk.get_position_balances.return_value = {}
        result = adapter.get_pending_fees("WAVAX", "USDC", bin_step=20)
        assert result is None

    def test_get_pending_fees_aggregates(self, adapter_with_mock_sdk) -> None:
        adapter, sdk = adapter_with_mock_sdk
        sdk.get_pool_address.return_value = POOL_ADDR
        sdk.get_position_balances.return_value = {BIN_ID_OFFSET: 1_000}
        sdk.get_total_position_value.return_value = (1, 2)
        pool_info = MagicMock()
        pool_info.token_x = TOKEN_X
        pool_info.token_y = TOKEN_Y
        pool_info.bin_step = 20
        pool_info.active_id = BIN_ID_OFFSET
        sdk.get_pool_info.return_value = pool_info
        sdk.get_pending_fees.return_value = (15, 30)

        result = adapter.get_pending_fees("WAVAX", "USDC", bin_step=20)
        assert result is not None
        assert result.pool_address == POOL_ADDR
        assert result.pending_fees_x == 15
        assert result.pending_fees_y == 30
        assert result.has_fees is True

    def test_get_pending_fees_zero_when_no_fees(self, adapter_with_mock_sdk) -> None:
        adapter, sdk = adapter_with_mock_sdk
        sdk.get_pool_address.return_value = POOL_ADDR
        sdk.get_position_balances.return_value = {BIN_ID_OFFSET: 1_000}
        sdk.get_total_position_value.return_value = (1, 2)
        pool_info = MagicMock()
        pool_info.token_x = TOKEN_X
        pool_info.token_y = TOKEN_Y
        pool_info.bin_step = 20
        pool_info.active_id = BIN_ID_OFFSET
        sdk.get_pool_info.return_value = pool_info
        sdk.get_pending_fees.return_value = (0, 0)

        result = adapter.get_pending_fees("WAVAX", "USDC", bin_step=20)
        assert result is not None
        assert result.has_fees is False

    def test_build_collect_fees_returns_none_without_position(self, adapter_with_mock_sdk) -> None:
        adapter, sdk = adapter_with_mock_sdk
        sdk.get_pool_address.return_value = POOL_ADDR
        sdk.get_position_balances.return_value = {}
        result = adapter.build_collect_fees_transaction("WAVAX", "USDC", bin_step=20)
        assert result is None

    def test_build_collect_fees_with_position(self, adapter_with_mock_sdk) -> None:
        adapter, sdk = adapter_with_mock_sdk
        sdk.get_pool_address.return_value = POOL_ADDR
        sdk.get_position_balances.return_value = {BIN_ID_OFFSET: 1_000}
        sdk.get_total_position_value.return_value = (1, 2)
        pool_info = MagicMock()
        pool_info.token_x = TOKEN_X
        pool_info.token_y = TOKEN_Y
        pool_info.bin_step = 20
        pool_info.active_id = BIN_ID_OFFSET
        sdk.get_pool_info.return_value = pool_info
        sdk.build_collect_fees.return_value = (
            {"to": POOL_ADDR, "data": "0x225b20b9", "value": 0},
            200_000,
        )
        tx = adapter.build_collect_fees_transaction("WAVAX", "USDC", bin_step=20)
        assert tx is not None
        assert tx.to == POOL_ADDR
        assert tx.gas == 200_000
