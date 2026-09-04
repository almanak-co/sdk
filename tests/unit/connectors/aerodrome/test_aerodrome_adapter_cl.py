"""Tests for AerodromeAdapter Slipstream CL liquidity operations + on-chain quoting.

Targets uncovered branches in:
- add_cl_liquidity success path (Decimal mode, both token-orderings, missing reviewed generation)
- remove_cl_liquidity / collect_cl_fees (empty position, decreaseLiquidity+collect, exception)
- _get_web3 (rpc_url branch, gateway branch, no-config error)
- _try_get_amount_out_onchain / _try_get_cl_amount_out_onchain
- _get_quote_exact_input on-chain success branch

Every CL money path takes the reviewed Slipstream generation that owns the pool
or position; there is no default position manager, router, or quoter.
"""

from decimal import Decimal
from unittest.mock import ANY, MagicMock, patch

import pytest

from almanak.connectors.aerodrome.adapter import (
    AerodromeAdapter,
    AerodromeConfig,
    CLLiquidityResult,
)
from almanak.connectors.aerodrome.addresses import SlipstreamDeployment, slipstream_deployment_for_factory
from almanak.connectors.aerodrome.sdk import CLPositionInfo
from almanak.framework.data.tokens.exceptions import TokenResolutionError
from almanak.framework.data.tokens.models import ResolvedToken

TEST_WALLET = "0x1234567890123456789012345678901234567890"
USDC_ADDRESS = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"
WETH_ADDRESS = "0x4200000000000000000000000000000000000006"


def _generation(factory: str) -> SlipstreamDeployment:
    deployment = slipstream_deployment_for_factory("base", factory)
    assert deployment is not None, factory
    return deployment


CURRENT = _generation("0xf8f2eB4940CFE7d13603DDDD87f123820Fc061Ef")
LEGACY = _generation("0x5e7BB104d84c7CB9B682AaC2F3d509f5F406809A")


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
def adapter() -> AerodromeAdapter:
    cfg = AerodromeConfig(chain="base", wallet_address=TEST_WALLET, allow_placeholder_prices=True)
    return AerodromeAdapter(cfg, token_resolver=_make_resolver())


@pytest.fixture
def opt_adapter() -> AerodromeAdapter:
    """Optimism adapter — no reviewed Slipstream generation, exercises the unsupported-chain branch."""
    cfg = AerodromeConfig(chain="optimism", wallet_address=TEST_WALLET, allow_placeholder_prices=True)
    return AerodromeAdapter(cfg, token_resolver=_make_resolver())


def _position(liquidity: int, owed0: int = 0, owed1: int = 0) -> CLPositionInfo:
    return CLPositionInfo(
        token_id=42,
        token0=USDC_ADDRESS,
        token1=WETH_ADDRESS,
        tick_spacing=200,
        tick_lower=-100,
        tick_upper=100,
        liquidity=liquidity,
        tokens_owed0=owed0,
        tokens_owed1=owed1,
    )


# add_cl_liquidity — full success paths


class TestAddCLLiquidityHappyPath:
    """Decimal-mode add_cl_liquidity end-to-end through SDK build_cl_mint_tx."""

    def test_add_cl_liquidity_decimal_mode_success(self, adapter: AerodromeAdapter) -> None:
        # Mock SDK + web3
        adapter._get_web3 = MagicMock(return_value=MagicMock())  # type: ignore[method-assign]
        adapter.sdk.build_cl_mint_tx = MagicMock(  # type: ignore[method-assign]
            return_value={"to": CURRENT.position_manager, "value": 0, "data": b"\x00\x01\x02\x03"},
        )
        # VIB-4468 W7 — Decimal-mode emits a DeprecationWarning.
        with pytest.warns(DeprecationWarning, match="Decimal-mode"):
            result = adapter.add_cl_liquidity(
                token_a="USDC",
                token_b="WETH",
                tick_spacing=200,
                tick_lower=-1000,
                tick_upper=1000,
                amount_a=Decimal("100"),
                amount_b=Decimal("0.05"),
                deployment=CURRENT,
            )
        assert result.success is True
        assert isinstance(result, CLLiquidityResult)
        # Token sorting: USDC > WETH lex, so token0 should be WETH
        assert result.token0 == WETH_ADDRESS
        assert result.token1 == USDC_ADDRESS
        # Hex-converted bytes data on mint tx
        mint_tx = result.transactions[-1]
        assert mint_tx.tx_type == "add_liquidity"
        assert mint_tx.to == CURRENT.position_manager
        assert mint_tx.data == "00010203"
        assert adapter.sdk.build_cl_mint_tx.call_args.kwargs["deployment"] is CURRENT

    def test_add_cl_liquidity_data_already_hex_string(self, adapter: AerodromeAdapter) -> None:
        adapter._get_web3 = MagicMock(return_value=MagicMock())  # type: ignore[method-assign]
        adapter.sdk.build_cl_mint_tx = MagicMock(  # type: ignore[method-assign]
            return_value={"to": CURRENT.position_manager, "data": "0xdeadbeef"},
        )
        with pytest.warns(DeprecationWarning, match="Decimal-mode"):
            result = adapter.add_cl_liquidity(
                token_a="USDC",
                token_b="WETH",
                tick_spacing=100,
                tick_lower=-100,
                tick_upper=100,
                amount_a=Decimal("1"),
                amount_b=Decimal("0.001"),
                deployment=CURRENT,
            )
        assert result.success
        assert result.transactions[-1].data == "0xdeadbeef"

    def test_add_cl_liquidity_unsupported_chain_returns_error(self, opt_adapter: AerodromeAdapter) -> None:
        result = opt_adapter.add_cl_liquidity(
            token_a="USDC",
            token_b="WETH",
            tick_spacing=200,
            tick_lower=-100,
            tick_upper=100,
            amount_a=Decimal("1"),
            amount_b=Decimal("0.001"),
            deployment=CURRENT,
        )
        assert result.success is False
        assert "Slipstream CL not supported" in (result.error or "")

    def test_add_cl_liquidity_without_deployment_is_refused_before_any_build(self, adapter: AerodromeAdapter) -> None:
        adapter._get_web3 = MagicMock(return_value=MagicMock())  # type: ignore[method-assign]
        adapter.sdk.build_cl_mint_tx = MagicMock()  # type: ignore[method-assign]
        result = adapter.add_cl_liquidity(
            token_a="USDC",
            token_b="WETH",
            tick_spacing=200,
            tick_lower=-100,
            tick_upper=100,
            amount_a=Decimal("0"),
            amount_b=Decimal("0"),
            amount_a_wei=1_000_000,
            amount_b_wei=5 * 10**14,
            amount_a_min_wei=900_000,
            amount_b_min_wei=4 * 10**14,
        )
        assert result.success is False
        assert "requires the reviewed generation" in (result.error or "")
        assert result.transactions == []
        adapter.sdk.build_cl_mint_tx.assert_not_called()

    @pytest.mark.parametrize("deployment", [CURRENT, LEGACY], ids=["current", "legacy"])
    def test_add_cl_liquidity_approves_and_mints_on_the_supplied_generation_manager(
        self, adapter: AerodromeAdapter, deployment: SlipstreamDeployment
    ) -> None:
        """The NPM that owns the new NFT is the supplied generation's, for every reviewed generation."""
        adapter._get_web3 = MagicMock(return_value=MagicMock())  # type: ignore[method-assign]
        adapter._build_approve_tx = MagicMock(return_value=None)  # type: ignore[method-assign]
        adapter.sdk.build_cl_mint_tx = MagicMock(  # type: ignore[method-assign]
            side_effect=lambda **kw: {"to": kw["deployment"].position_manager, "data": b""},
        )
        result = adapter.add_cl_liquidity(
            token_a="USDC",
            token_b="WETH",
            tick_spacing=200,
            tick_lower=-100,
            tick_upper=100,
            amount_a=Decimal("0"),
            amount_b=Decimal("0"),
            amount_a_wei=1_000_000,
            amount_b_wei=5 * 10**14,
            amount_a_min_wei=900_000,
            amount_b_min_wei=4 * 10**14,
            deployment=deployment,
        )
        assert result.success, result.error
        spenders = {call.args[1] for call in adapter._build_approve_tx.call_args_list}
        assert spenders == {deployment.position_manager}
        assert result.transactions[-1].to == deployment.position_manager
        assert adapter.sdk.build_cl_mint_tx.call_args.kwargs["deployment"] is deployment

    def test_add_cl_liquidity_exception_caught(self, adapter: AerodromeAdapter) -> None:
        adapter._get_web3 = MagicMock(side_effect=RuntimeError("no web3"))  # type: ignore[method-assign]
        # Decimal-mode path: the DeprecationWarning fires before _get_web3
        # raises, so we still expect the warning to be emitted even though
        # the overall call ultimately returns success=False.
        with pytest.warns(DeprecationWarning, match="Decimal-mode"):
            result = adapter.add_cl_liquidity(
                token_a="USDC",
                token_b="WETH",
                tick_spacing=200,
                tick_lower=-100,
                tick_upper=100,
                amount_a=Decimal("1"),
                amount_b=Decimal("0.001"),
                deployment=CURRENT,
            )
        assert result.success is False
        assert "no web3" in (result.error or "")

    def test_add_cl_liquidity_wei_overload_uses_overrides(self, adapter: AerodromeAdapter) -> None:
        """Wei-overload path threads pre-computed mins all the way to the mint TX."""
        captured: dict = {}

        def _mock_mint(**kw: object) -> dict:
            captured.update(kw)
            return {"to": CURRENT.position_manager, "data": b""}

        adapter._get_web3 = MagicMock(return_value=MagicMock())  # type: ignore[method-assign]
        adapter.sdk.build_cl_mint_tx = MagicMock(side_effect=_mock_mint)  # type: ignore[method-assign]
        result = adapter.add_cl_liquidity(
            token_a="USDC",
            token_b="WETH",
            tick_spacing=200,
            tick_lower=-100,
            tick_upper=100,
            amount_a=Decimal("0"),
            amount_b=Decimal("0"),
            amount_a_wei=1_000_000,
            amount_b_wei=5 * 10**14,
            amount_a_min_wei=900_000,
            amount_b_min_wei=4 * 10**14,
            deployment=CURRENT,
        )
        assert result.success
        # The mint tx received the overrides (sorted by token order)
        # USDC > WETH, so token0=WETH, amount0_min should be the b-min
        assert captured["amount0_min"] == 4 * 10**14
        assert captured["amount1_min"] == 900_000
        assert captured["deployment"] is CURRENT

    def test_add_cl_liquidity_token_a_lex_lower(self, adapter: AerodromeAdapter) -> None:
        """Reverse the token order to exercise the alternate sort branch."""
        adapter._get_web3 = MagicMock(return_value=MagicMock())  # type: ignore[method-assign]
        adapter.sdk.build_cl_mint_tx = MagicMock(  # type: ignore[method-assign]
            return_value={"to": CURRENT.position_manager, "data": b""},
        )
        # WETH < USDC lex (4200... < 833...), so token_a=WETH means token_a is lower
        with pytest.warns(DeprecationWarning, match="Decimal-mode"):
            result = adapter.add_cl_liquidity(
                token_a="WETH",
                token_b="USDC",
                tick_spacing=200,
                tick_lower=-100,
                tick_upper=100,
                amount_a=Decimal("0.05"),
                amount_b=Decimal("100"),
                deployment=CURRENT,
            )
        assert result.success
        # token0 should be WETH (the smaller addr)
        assert result.token0 == WETH_ADDRESS
        assert result.token1 == USDC_ADDRESS


# remove_cl_liquidity


class TestRemoveCLLiquidity:
    def test_remove_cl_liquidity_without_deployment_is_refused_before_any_read(self, adapter: AerodromeAdapter) -> None:
        adapter._get_web3 = MagicMock(return_value=MagicMock())  # type: ignore[method-assign]
        adapter.sdk.get_cl_position = MagicMock()  # type: ignore[method-assign]
        result = adapter.remove_cl_liquidity(token_id=42)
        assert result.success is False
        assert "requires the reviewed generation" in (result.error or "")
        adapter.sdk.get_cl_position.assert_not_called()

    def test_remove_cl_position_not_found_returns_error(self, adapter: AerodromeAdapter) -> None:
        adapter._get_web3 = MagicMock(return_value=MagicMock())  # type: ignore[method-assign]
        adapter.sdk.get_cl_position = MagicMock(return_value=None)  # type: ignore[method-assign]
        result = adapter.remove_cl_liquidity(token_id=42, deployment=CURRENT)
        assert result.success is False
        assert "Could not query CL position" in (result.error or "")
        adapter.sdk.get_cl_position.assert_called_once_with(42, ANY, deployment=CURRENT)

    def test_remove_cl_zero_liquidity_no_owed_returns_noop(self, adapter: AerodromeAdapter) -> None:
        adapter._get_web3 = MagicMock(return_value=MagicMock())  # type: ignore[method-assign]
        adapter.sdk.get_cl_position = MagicMock(return_value=_position(liquidity=0))  # type: ignore[method-assign]
        result = adapter.remove_cl_liquidity(token_id=42, deployment=CURRENT)
        assert result.success is True
        assert result.transactions == []
        assert result.gas_estimate == 0

    @pytest.mark.parametrize("deployment", [CURRENT, LEGACY], ids=["current", "legacy"])
    def test_remove_cl_liquidity_with_liquidity_builds_decrease_and_collect(
        self,
        adapter: AerodromeAdapter,
        deployment: SlipstreamDeployment,
    ) -> None:
        adapter._get_web3 = MagicMock(return_value=MagicMock())  # type: ignore[method-assign]
        adapter.sdk.get_cl_position = MagicMock(return_value=_position(liquidity=10**18))  # type: ignore[method-assign]
        adapter.sdk.build_cl_decrease_liquidity_tx = MagicMock(  # type: ignore[method-assign]
            return_value={"to": deployment.position_manager, "data": b"\xde\xad"},
        )
        adapter.sdk.build_cl_collect_tx = MagicMock(  # type: ignore[method-assign]
            return_value={"to": deployment.position_manager, "data": "0xcafe"},
        )
        result = adapter.remove_cl_liquidity(token_id=42, deployment=deployment)
        assert result.success
        # decrease + collect
        assert len(result.transactions) == 2
        assert result.transactions[0].tx_type == "remove_liquidity"
        assert result.transactions[1].tx_type == "remove_liquidity"
        # Bytes data hex-converted, str data passed through
        assert result.transactions[0].data == "dead"
        assert result.transactions[1].data == "0xcafe"
        # Both legs are built against the generation that owns the NFT.
        assert adapter.sdk.build_cl_decrease_liquidity_tx.call_args.kwargs["deployment"] is deployment
        assert adapter.sdk.build_cl_collect_tx.call_args.kwargs["deployment"] is deployment
        assert {tx.to for tx in result.transactions} == {deployment.position_manager}

    def test_remove_cl_zero_liquidity_with_owed_only_collect(self, adapter: AerodromeAdapter) -> None:
        """has_owed=True with liquidity=0 → only collect, no decreaseLiquidity."""
        adapter._get_web3 = MagicMock(return_value=MagicMock())  # type: ignore[method-assign]
        adapter.sdk.get_cl_position = MagicMock(return_value=_position(liquidity=0, owed0=100))  # type: ignore[method-assign]
        adapter.sdk.build_cl_collect_tx = MagicMock(  # type: ignore[method-assign]
            return_value={"to": CURRENT.position_manager, "data": b""},
        )
        result = adapter.remove_cl_liquidity(token_id=42, deployment=CURRENT)
        assert result.success
        assert len(result.transactions) == 1
        assert "collect" in result.transactions[0].description.lower()

    def test_remove_cl_liquidity_exception_caught(self, adapter: AerodromeAdapter) -> None:
        adapter._get_web3 = MagicMock(side_effect=RuntimeError("rpc fail"))  # type: ignore[method-assign]
        result = adapter.remove_cl_liquidity(token_id=42, deployment=CURRENT)
        assert result.success is False
        assert "rpc fail" in (result.error or "")

    def test_lp_close_survives_single_transient_rpc_error(
        self, adapter: AerodromeAdapter, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """ALM-2892: a single transient RESOURCE_EXHAUSTED on the CL position read
        no longer aborts the LP_CLOSE build — it retries and succeeds.

        Drives the REAL ``sdk.get_cl_position`` retry loop (not a mock) through the
        adapter's remove-liquidity (LP_CLOSE) build path.
        """
        monkeypatch.setattr("almanak.connectors.aerodrome.sdk.time.sleep", lambda _s: None)
        position_tuple = (
            0,
            "0x0",
            USDC_ADDRESS,
            WETH_ADDRESS,
            200,
            -100,
            100,
            10**18,
            0,
            0,
            0,
            0,
        )
        contract = MagicMock()
        contract.functions.positions.return_value.call.side_effect = [
            RuntimeError('RESOURCE_EXHAUSTED "Rate limited, retry after 0.01s"'),
            position_tuple,
        ]
        web3 = MagicMock()
        web3.eth.contract.return_value = contract
        web3.to_checksum_address.side_effect = lambda a: a
        adapter._get_web3 = MagicMock(return_value=web3)  # type: ignore[method-assign]
        adapter.sdk.build_cl_decrease_liquidity_tx = MagicMock(  # type: ignore[method-assign]
            return_value={"to": CURRENT.position_manager, "data": b"\xde\xad"},
        )
        adapter.sdk.build_cl_collect_tx = MagicMock(  # type: ignore[method-assign]
            return_value={"to": CURRENT.position_manager, "data": "0xcafe"},
        )

        result = adapter.remove_cl_liquidity(token_id=72457473, deployment=CURRENT)

        assert result.success is True
        assert len(result.transactions) == 2  # decrease + collect
        # The position read was retried exactly once (1 failure + 1 success).
        assert contract.functions.positions.return_value.call.call_count == 2
        # The read targets the supplied generation's manager, not a default.
        assert web3.eth.contract.call_args.kwargs["address"] == CURRENT.position_manager


# collect_cl_fees


class TestCollectCLFees:
    def test_collect_cl_fees_without_deployment_is_refused_before_any_build(self, adapter: AerodromeAdapter) -> None:
        adapter._get_web3 = MagicMock(return_value=MagicMock())  # type: ignore[method-assign]
        adapter.sdk.build_cl_collect_tx = MagicMock()  # type: ignore[method-assign]
        result = adapter.collect_cl_fees(token_id=42)
        assert result.success is False
        assert "requires the reviewed generation" in (result.error or "")
        adapter.sdk.build_cl_collect_tx.assert_not_called()

    @pytest.mark.parametrize("deployment", [CURRENT, LEGACY], ids=["current", "legacy"])
    def test_collect_cl_fees_builds_collect_on_the_supplied_generation(
        self, adapter: AerodromeAdapter, deployment: SlipstreamDeployment
    ) -> None:
        adapter._get_web3 = MagicMock(return_value=MagicMock())  # type: ignore[method-assign]
        adapter.sdk.build_cl_collect_tx = MagicMock(  # type: ignore[method-assign]
            return_value={"to": deployment.position_manager, "data": b"\xca\xfe"},
        )
        result = adapter.collect_cl_fees(token_id=42, deployment=deployment)
        assert result.success, result.error
        assert len(result.transactions) == 1
        assert result.transactions[0].tx_type == "lp_collect_fees"
        assert result.transactions[0].to == deployment.position_manager
        assert result.transactions[0].data == "cafe"
        assert adapter.sdk.build_cl_collect_tx.call_args.kwargs["deployment"] is deployment


# _get_web3


class TestGetWeb3:
    def test_get_web3_no_config_raises(self, adapter: AerodromeAdapter) -> None:
        # Both gateway_client and rpc_url are None on the default fixture
        adapter.config.gateway_client = None
        adapter.config.rpc_url = None
        with pytest.raises(ValueError, match="No gateway_client or rpc_url"):
            adapter._get_web3()

    def test_get_web3_with_rpc_url(self) -> None:
        cfg = AerodromeConfig(
            chain="base",
            wallet_address=TEST_WALLET,
            rpc_url="https://localhost:8545",
            allow_placeholder_prices=True,
        )
        adapter = AerodromeAdapter(cfg, token_resolver=_make_resolver())
        # Should construct a Web3 with HTTPProvider
        with patch("web3.Web3") as mock_web3:
            adapter._get_web3()
            # First positional arg is the HTTPProvider
            assert mock_web3.called

    def test_get_web3_with_gateway_client(self) -> None:
        cfg = AerodromeConfig(
            chain="base",
            wallet_address=TEST_WALLET,
            gateway_client=MagicMock(),
            allow_placeholder_prices=True,
        )
        adapter = AerodromeAdapter(cfg, token_resolver=_make_resolver())
        with patch("almanak.framework.web3.gateway_provider.GatewayWeb3Provider"):
            with patch("web3.Web3") as mock_web3:
                adapter._get_web3()
                assert mock_web3.called


# _try_get_amount_out_onchain


class TestTryGetAmountOutOnchain:
    def test_returns_none_when_no_rpc_or_gateway(self, adapter: AerodromeAdapter) -> None:
        adapter.config.rpc_url = None
        adapter.config.gateway_client = None
        adapter._web3 = None
        out = adapter._try_get_amount_out_onchain(USDC_ADDRESS, WETH_ADDRESS, 1_000_000, False)
        assert out is None

    def test_returns_amount_when_rpc_succeeds(self, adapter: AerodromeAdapter) -> None:
        adapter._web3 = MagicMock()
        adapter.sdk.get_amounts_out = MagicMock(return_value=[1_000_000, 5 * 10**14])  # type: ignore[method-assign]
        out = adapter._try_get_amount_out_onchain(USDC_ADDRESS, WETH_ADDRESS, 1_000_000, False)
        assert out == 5 * 10**14

    def test_returns_none_when_rpc_returns_empty(self, adapter: AerodromeAdapter) -> None:
        adapter._web3 = MagicMock()
        adapter.sdk.get_amounts_out = MagicMock(return_value=None)  # type: ignore[method-assign]
        out = adapter._try_get_amount_out_onchain(USDC_ADDRESS, WETH_ADDRESS, 1_000_000, False)
        assert out is None

    def test_returns_none_on_sdk_exception(self, adapter: AerodromeAdapter) -> None:
        adapter._web3 = MagicMock()
        adapter.sdk.get_amounts_out = MagicMock(side_effect=RuntimeError("rpc"))  # type: ignore[method-assign]
        out = adapter._try_get_amount_out_onchain(USDC_ADDRESS, WETH_ADDRESS, 1_000_000, False)
        assert out is None

    def test_quote_uses_onchain_amount_when_available(self, adapter: AerodromeAdapter) -> None:
        """_get_quote_exact_input takes the onchain branch when amount_out resolves."""
        adapter._try_get_amount_out_onchain = MagicMock(  # type: ignore[method-assign]
            return_value=5 * 10**14,
        )
        quote = adapter._get_quote_exact_input(USDC_ADDRESS, WETH_ADDRESS, 1_000_000, False, skip_onchain=False)
        assert quote.amount_out == 5 * 10**14
        # effective_price = 0.0005 / 1 = 0.0005
        assert quote.effective_price == Decimal("0.0005") / Decimal("1")


# CL quoting goes through the owning generation's quoter


class TestCLQuoteVenue:
    def test_cl_quote_is_asked_of_the_supplied_quoter(self, adapter: AerodromeAdapter) -> None:
        adapter._try_get_cl_amount_out_onchain = MagicMock(return_value=5 * 10**14)  # type: ignore[method-assign]
        quote = adapter._get_quote_exact_input(
            USDC_ADDRESS, WETH_ADDRESS, 1_000_000, False, tick_spacing=100, use_cl=True, quoter=LEGACY.quoter
        )
        assert quote.amount_out == 5 * 10**14
        assert quote.is_onchain is True
        assert adapter._try_get_cl_amount_out_onchain.call_args.kwargs["quoter"] == LEGACY.quoter

    def test_cl_onchain_quote_without_a_quoter_never_touches_the_chain(self, adapter: AerodromeAdapter) -> None:
        adapter.config.rpc_url = "http://localhost:8545"
        with patch("almanak.connectors._strategy_base.rpc.eth_call") as call:
            out = adapter._try_get_cl_amount_out_onchain(USDC_ADDRESS, WETH_ADDRESS, 1_000_000, 100, quoter=None)
        assert out is None
        call.assert_not_called()

    def test_cl_onchain_quote_targets_the_supplied_quoter(self, adapter: AerodromeAdapter) -> None:
        adapter.config.rpc_url = "http://localhost:8545"
        with patch("almanak.connectors._strategy_base.rpc.eth_call", return_value=(7).to_bytes(32, "big")) as call:
            out = adapter._try_get_cl_amount_out_onchain(
                USDC_ADDRESS, WETH_ADDRESS, 1_000_000, 100, quoter=CURRENT.quoter
            )
        assert out == 7
        assert call.call_args.kwargs["to"] == CURRENT.quoter

    def test_quote_swap_output_cl_requires_a_deployment(self, adapter: AerodromeAdapter) -> None:
        adapter._try_get_cl_amount_out_onchain = MagicMock(return_value=1)  # type: ignore[method-assign]
        with pytest.raises(ValueError, match="requires the reviewed generation"):
            adapter.quote_swap_output(
                token_in=USDC_ADDRESS, token_out=WETH_ADDRESS, amount_in_wei=1_000_000, use_cl=True
            )
        adapter._try_get_cl_amount_out_onchain.assert_not_called()

    def test_quote_swap_output_cl_uses_the_deployment_quoter(self, adapter: AerodromeAdapter) -> None:
        adapter._try_get_cl_amount_out_onchain = MagicMock(return_value=9)  # type: ignore[method-assign]
        out = adapter.quote_swap_output(
            token_in=USDC_ADDRESS,
            token_out=WETH_ADDRESS,
            amount_in_wei=1_000_000,
            use_cl=True,
            require_onchain=True,
            deployment=LEGACY,
        )
        assert out == 9
        assert adapter._try_get_cl_amount_out_onchain.call_args.kwargs["quoter"] == LEGACY.quoter

    def test_quote_swap_output_classic_needs_no_deployment(self, adapter: AerodromeAdapter) -> None:
        adapter._try_get_amount_out_onchain = MagicMock(return_value=3)  # type: ignore[method-assign]
        out = adapter.quote_swap_output(
            token_in=USDC_ADDRESS, token_out=WETH_ADDRESS, amount_in_wei=1_000_000, use_cl=False, require_onchain=True
        )
        assert out == 3
