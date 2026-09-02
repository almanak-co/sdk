"""Fail-closed exact-address TraderJoe V2 LB pair on LP_OPEN and LP_CLOSE.

Before this lane existed, both TraderJoe V2 LP verbs rejected every bare
``0x…`` pool address at their format gate ("Invalid pool format for TraderJoe
V2") even though an LB position is identified by pair + bin ids and a strategy
that opened by address must be able to close by address. These tests pin the
new contract, mirrored from the Uniswap V3 and Aerodrome Slipstream exact lanes:

- a bare address is an execution constraint, never a discovery hint;
- the pair's own ``getTokenX/getTokenY/getBinStep`` are read through the
  gateway boundary only;
- the registered LB factory must round-trip the tuple to the same address
  (no alternate-pool substitution);
- bin ids on close still come from ``protocol_params["bin_ids"]``.
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from almanak.connectors._strategy_base.pool_validation_base import PoolValidationReason, PoolValidationResult
from almanak.connectors.traderjoe_v2 import compiler as traderjoe_compiler
from almanak.connectors.traderjoe_v2.addresses import TRADERJOE_V2
from almanak.connectors.traderjoe_v2.compiler import TraderJoeV2Compiler, _TraderJoeV2CompileImpl
from almanak.connectors.traderjoe_v2.pool_validation import (
    LB_PAIR_GET_BIN_STEP_SELECTOR,
    LB_PAIR_GET_TOKEN_X_SELECTOR,
    LB_PAIR_GET_TOKEN_Y_SELECTOR,
    LBPairBinding,
    read_lb_pair_binding,
)
from almanak.framework.intents.compiler import IntentCompiler, IntentCompilerConfig
from almanak.framework.intents.compiler_models import CompilationResult, CompilationStatus, TokenInfo
from almanak.framework.intents.vocabulary import LPCloseIntent, LPOpenIntent

# Real Arbitrum addresses. WETH/USDC bin_step=15 is the registered LB pair
# (``TRADERJOE_V2_LBPAIRS["arbitrum"]``); tokenX = WETH, tokenY = USDC.
POOL = "0x69f1216cb2905bf0852f74624d5fa7b5fc4da710"
OTHER_POOL = "0x7ec6c9d993d9832aa654593f2dbc21303650bc6c"
WETH = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"
USDC = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
WALLET = "0x" + "11" * 20
FACTORY = TRADERJOE_V2["arbitrum"]["factory"]
BIN_STEP = 15

_TOKENS = {
    WETH: TokenInfo("WETH", WETH, 18),
    USDC: TokenInfo("USDC", USDC, 6),
}


class _Gateway:
    is_connected = True


class _DisconnectedGateway:
    is_connected = False


def _impl(
    *,
    gateway: object | None = _Gateway(),
    internal_preflight: bool = False,
    chain: str = "arbitrum",
) -> _TraderJoeV2CompileImpl:
    """A real per-call compile impl with the gateway/token seams overridden."""
    compiler = IntentCompiler(
        chain=chain,
        wallet_address=WALLET,
        rpc_url="http://gateway-internal:8545",
        config=IntentCompilerConfig(allow_placeholder_prices=True),
    )
    ctx = compiler._build_compiler_context("traderjoe_v2", TraderJoeV2Compiler())
    impl = _TraderJoeV2CompileImpl(ctx)
    impl._gateway_client = gateway  # type: ignore[assignment]
    impl._gateway_internal_preflight = internal_preflight
    impl._resolve_token = lambda token, chain=None: _TOKENS.get(token.lower())  # type: ignore[method-assign]
    return impl


def _binding(*, bin_step: int = BIN_STEP) -> LBPairBinding:
    return LBPairBinding(token_x=WETH, token_y=USDC, bin_step=bin_step)


def _confirmed(pool: str = POOL) -> PoolValidationResult:
    return PoolValidationResult(exists=True, reason=PoolValidationReason.CONFIRMED, pool_address=pool)


def _resolve(impl: _TraderJoeV2CompileImpl, pool: str = POOL):
    return impl._resolve_exact_lb_pair(pool, "lp-1")


def _failed(result: object) -> CompilationResult:
    assert isinstance(result, CompilationResult)
    assert result.status is CompilationStatus.FAILED
    assert result.intent_id == "lp-1"
    return result


_READ = "almanak.connectors.traderjoe_v2.pool_validation.read_lb_pair_binding"
_VALIDATE = "almanak.connectors.traderjoe_v2.pool_validation.validate_traderjoe_pool"


# Resolver: boundary and input gates (no reads issued)
def test_exact_pair_rejects_malformed_address_before_reads() -> None:
    with patch(_READ) as read:
        result = _failed(_resolve(_impl(), "0x1234"))
    assert result.error == "Invalid exact TraderJoe V2 LB pair address: 0x1234"
    read.assert_not_called()


@pytest.mark.parametrize("gateway", [None, _DisconnectedGateway()])
def test_exact_pair_requires_gateway_boundary(gateway: object | None) -> None:
    with patch(_READ) as read:
        result = _failed(_resolve(_impl(gateway=gateway)))
    assert "connected gateway is required" in (result.error or "")
    read.assert_not_called()


def test_exact_pair_rejects_unregistered_chain_before_reads() -> None:
    with patch(_READ) as read:
        result = _failed(_resolve(_impl(chain="optimism")))
    assert "LB factory not registered for chain 'optimism'" in (result.error or "")
    read.assert_not_called()


def test_exact_pair_strategy_side_reads_never_carry_a_direct_rpc_url() -> None:
    """Outside the gateway process the reader must receive rpc_url=None (blueprint 20)."""
    with (
        patch(_READ, return_value=_binding()) as read,
        patch(_VALIDATE, return_value=_confirmed()) as validate,
    ):
        _resolve(_impl(internal_preflight=False))
    assert read.call_args.args[1] is None
    assert validate.call_args.args[4] is None


def test_exact_pair_gateway_internal_preflight_may_use_internal_rpc() -> None:
    with (
        patch(_READ, return_value=_binding()) as read,
        patch(_VALIDATE, return_value=_confirmed()),
    ):
        resolved = _resolve(_impl(gateway=None, internal_preflight=True))
    assert not isinstance(resolved, CompilationResult)
    assert read.call_args.args[1] == "http://gateway-internal:8545"
    # The adapter transport follows the same normalisation as the symbolic lane.
    assert resolved.gateway_client is None
    assert resolved.rpc_url == "http://gateway-internal:8545"


# Resolver: on-chain authentication
def test_exact_pair_fails_closed_when_pair_reads_fail() -> None:
    with (
        patch(_READ, return_value=None),
        patch(_VALIDATE) as validate,
    ):
        result = _failed(_resolve(_impl()))
    assert "reads failed or returned non-pool values" in (result.error or "")
    validate.assert_not_called()


def test_exact_pair_fails_closed_on_missing_token_metadata() -> None:
    impl = _impl()
    impl._resolve_token = lambda token, chain=None: _TOKENS.get(token.lower()) if token.lower() == WETH else None  # type: ignore[method-assign]
    with (
        patch(_READ, return_value=_binding()),
        patch(_VALIDATE) as validate,
    ):
        result = _failed(_resolve(impl))
    assert f"Cannot resolve token metadata for {USDC}" in (result.error or "")
    validate.assert_not_called()


def test_exact_pair_round_trips_tuple_through_the_registered_factory() -> None:
    gateway = _Gateway()
    with (
        patch(_READ, return_value=_binding()),
        patch(_VALIDATE, return_value=_confirmed()) as validate,
    ):
        resolved = _resolve(_impl(gateway=gateway))

    assert not isinstance(resolved, CompilationResult)
    assert (resolved.token_x.address, resolved.token_y.address) == (WETH, USDC)
    assert (resolved.token_x_symbol, resolved.token_y_symbol) == ("WETH", "USDC")
    assert resolved.bin_step == BIN_STEP
    # Identity only: an exact address must authenticate even on an empty pair.
    assert validate.call_args.args[:4] == ("arbitrum", WETH, USDC, BIN_STEP)
    assert validate.call_args.kwargs["allow_empty_reserves"] is True
    assert validate.call_args.kwargs["gateway_client"] is gateway
    # Connected gateway wins the adapter transport; no direct RPC leaks through.
    assert resolved.gateway_client is gateway
    assert resolved.rpc_url is None


@pytest.mark.parametrize(
    "factory_result",
    [
        PoolValidationResult(exists=False, reason=PoolValidationReason.NOT_FOUND, error="no pool"),
        PoolValidationResult(exists=None, reason=PoolValidationReason.RPC_FAILED, warning="rpc down"),
        PoolValidationResult(exists=None, reason=PoolValidationReason.RPC_UNAVAILABLE, warning="no rpc"),
        PoolValidationResult(exists=None, reason=PoolValidationReason.FACTORY_MISSING, warning="no factory"),
    ],
)
def test_exact_pair_requires_a_positive_factory_answer(factory_result: PoolValidationResult) -> None:
    """Unlike the symbolic lane, 'could not verify' is not admission for an exact address."""
    with (
        patch(_READ, return_value=_binding()),
        patch(_VALIDATE, return_value=factory_result),
    ):
        result = _failed(_resolve(_impl()))
    assert "Cannot authenticate exact TraderJoe V2 LB pair" in (result.error or "")
    assert FACTORY in (result.error or "")


def test_exact_pair_refuses_alternate_pool_substitution() -> None:
    with (
        patch(_READ, return_value=_binding()),
        patch(_VALIDATE, return_value=_confirmed(OTHER_POOL)),
    ):
        result = _failed(_resolve(_impl()))
    assert "Refusing alternate-pool substitution" in (result.error or "")
    assert OTHER_POOL in (result.error or "")


# LP_OPEN dispatch: the bare address clears the format gate and binds the mint
def _open_intent(pool: str = POOL) -> LPOpenIntent:
    return LPOpenIntent(
        pool=pool,
        amount0=Decimal("0.05"),
        amount1=Decimal("150"),
        range_lower=Decimal("200"),
        range_upper=Decimal("20000"),
        protocol="traderjoe_v2",
        chain="arbitrum",
    )


def test_open_bare_address_is_no_longer_rejected_at_the_format_gate() -> None:
    """The signature failure: a bare address used to die with 'Invalid pool format'."""
    impl = _impl(gateway=None)  # stops at the gateway boundary, AFTER the format gate
    result = impl._compile_lp_open_traderjoe_v2(_open_intent())
    assert result.status is CompilationStatus.FAILED
    assert "Invalid pool format" not in (result.error or "")
    assert "connected gateway is required" in (result.error or "")


def test_open_symbolic_format_gate_error_advertises_the_exact_address_form() -> None:
    result = _impl()._compile_lp_open_traderjoe_v2(_open_intent("WETH"))
    assert result.status is CompilationStatus.FAILED
    assert "Invalid pool format for TraderJoe V2: WETH. Expected format: TOKEN_X/TOKEN_Y/BIN_STEP" in (
        result.error or ""
    )
    assert "exact 0x pool address" in (result.error or "")


def test_open_bare_address_compiles_add_liquidity_bound_to_the_exact_pair() -> None:
    impl = _impl()
    impl._build_approve_tx = lambda token, spender, amount: []  # type: ignore[method-assign]
    impl._resolve_traderjoe_v2_lp_router = lambda intent: "0x" + "22" * 20  # type: ignore[method-assign]
    lp_tx = SimpleNamespace(to="0x" + "22" * 20, value=0, data="0xabcd", gas=400_000)

    with (
        patch(_READ, return_value=_binding()),
        patch(_VALIDATE, return_value=_confirmed()),
        patch("almanak.connectors.traderjoe_v2.TraderJoeV2Config"),
        patch("almanak.connectors.traderjoe_v2.TraderJoeV2Adapter") as adapter_cls,
    ):
        adapter_cls.return_value.build_add_liquidity_transaction.return_value = lp_tx
        result = impl._compile_lp_open_traderjoe_v2(_open_intent())

    assert result.status is CompilationStatus.SUCCESS, result.error
    # Tokens and bin step come from the pair contract, not from the intent.
    add = adapter_cls.return_value.build_add_liquidity_transaction.call_args.kwargs
    assert (add["token_x"], add["token_y"], add["bin_step"]) == (WETH, USDC, BIN_STEP)
    assert (add["amount_x"], add["amount_y"]) == (Decimal("0.05"), Decimal("150"))
    meta = result.action_bundle.metadata
    assert meta["pool"] == POOL
    assert meta["bin_step"] == BIN_STEP
    assert meta["token_x"]["address"] == WETH
    assert meta["token_y"]["address"] == USDC
    assert meta["amount_x"] == str(int(Decimal("0.05") * 10**18))
    assert meta["amount_y"] == str(int(Decimal("150") * 10**6))
    assert result.transactions[-1].tx_type == "traderjoe_v2_add_liquidity"
    assert "bin_step=15" in result.transactions[-1].description


def test_open_exact_lane_refusal_precedes_any_adapter_work() -> None:
    with (
        patch(_READ, return_value=_binding()),
        patch(_VALIDATE, return_value=_confirmed(OTHER_POOL)),
        patch("almanak.connectors.traderjoe_v2.TraderJoeV2Adapter") as adapter_cls,
    ):
        result = _impl()._compile_lp_open_traderjoe_v2(_open_intent())

    assert result.status is CompilationStatus.FAILED
    assert "Refusing alternate-pool substitution" in (result.error or "")
    adapter_cls.assert_not_called()


# LP_CLOSE dispatch: close by the same exact address, bin ids from the caller
def _close_intent(pool: str | None = POOL, *, bin_ids: list[int] | None = None) -> LPCloseIntent:
    return LPCloseIntent(
        position_id="0",
        pool=pool,
        collect_fees=True,
        protocol="traderjoe_v2",
        chain="arbitrum",
        protocol_params=None if bin_ids is None else {"bin_ids": bin_ids},
    )


def _close_adapter() -> MagicMock:
    adapter = MagicMock()
    adapter.sdk.router_address = "0x" + "44" * 20
    adapter.sdk.get_pool_address.return_value = POOL
    adapter.sdk.get_position_balances_for_ids.return_value = {8388600: 111, 8388601: 222}
    adapter.sdk.get_total_position_value.return_value = (1000, 2000)
    adapter.sdk.get_pool_info.return_value = SimpleNamespace(active_id=8388600)
    adapter.sdk.build_approve_for_all_transaction.return_value = (
        {"to": POOL, "data": "0xaaaa", "value": 0},
        12345,
    )
    adapter.build_remove_liquidity_transaction.return_value = SimpleNamespace(
        to="0x" + "44" * 20,
        data="0xbbbb",
        value=0,
        gas=54321,
    )
    return adapter


def test_close_bare_address_is_no_longer_rejected_at_the_format_gate() -> None:
    impl = _impl(gateway=None)
    result = impl._compile_lp_close_traderjoe_v2(_close_intent(bin_ids=[8388600]))
    assert result.status is CompilationStatus.FAILED
    assert "Invalid pool format" not in (result.error or "")
    assert "connected gateway is required" in (result.error or "")


def test_close_symbolic_format_gate_error_advertises_the_exact_address_form() -> None:
    result = _impl()._compile_lp_close_traderjoe_v2(_close_intent("WETH"))
    assert result.status is CompilationStatus.FAILED
    assert "Invalid pool format for TraderJoe V2: WETH. Expected format: TOKEN_X/TOKEN_Y/BIN_STEP" in (
        result.error or ""
    )
    assert "exact 0x pool address" in (result.error or "")


def test_close_still_requires_a_pool() -> None:
    result = _impl()._compile_lp_close_traderjoe_v2(_close_intent(None))
    assert result.status is CompilationStatus.FAILED
    assert result.error == "pool is required for TraderJoe V2 LP close"


def test_close_bare_address_targets_caller_bin_ids_on_the_exact_pair() -> None:
    impl = _impl()
    adapter = _close_adapter()

    with (
        patch(_READ, return_value=_binding()),
        patch(_VALIDATE, return_value=_confirmed()),
        patch("almanak.connectors.traderjoe_v2.TraderJoeV2Config"),
        patch("almanak.connectors.traderjoe_v2.TraderJoeV2Adapter", return_value=adapter),
    ):
        result = impl._compile_lp_close_traderjoe_v2(_close_intent(bin_ids=[8388600, 8388601]))

    assert result.status is CompilationStatus.SUCCESS, result.error
    assert result.action_bundle is not None
    assert len(result.action_bundle.transactions) == 2
    # The pair key handed to the SDK is the contract's own tuple.
    adapter.sdk.get_pool_address.assert_called_once_with(WETH, USDC, BIN_STEP)
    adapter.sdk.get_position_balances_for_ids.assert_called_once_with(POOL, WALLET, [8388600, 8388601])
    adapter.get_position.assert_not_called()
    remove = adapter.build_remove_liquidity_transaction.call_args.kwargs
    assert (remove["token_x"], remove["token_y"], remove["bin_step"]) == (WETH, USDC, BIN_STEP)
    assert remove["position"].pool_address == POOL
    assert remove["position"].bin_ids == [8388600, 8388601]
    meta = result.action_bundle.metadata
    assert meta["pool"] == POOL
    assert meta["protocol"] == "traderjoe_v2"
    assert "WETH/USDC" in result.transactions[-1].description


def test_close_exact_lane_refusal_precedes_any_adapter_work() -> None:
    with (
        patch(_READ, return_value=_binding()),
        patch(_VALIDATE, return_value=_confirmed(OTHER_POOL)),
        patch("almanak.connectors.traderjoe_v2.TraderJoeV2Adapter") as adapter_cls,
    ):
        result = _impl()._compile_lp_close_traderjoe_v2(_close_intent(bin_ids=[8388600]))

    assert result.status is CompilationStatus.FAILED
    assert "Refusing alternate-pool substitution" in (result.error or "")
    adapter_cls.assert_not_called()


def test_symbolic_close_lane_is_unchanged_and_skips_factory_authentication() -> None:
    """Symbolic keys keep the pre-lane behaviour: no reader, no factory round-trip here."""
    impl = _impl(gateway=None)
    impl._resolve_token = MagicMock(  # type: ignore[method-assign]
        side_effect=[SimpleNamespace(address=WETH), SimpleNamespace(address=USDC)]
    )
    adapter = _close_adapter()
    with (
        patch(_READ) as read,
        patch(_VALIDATE) as validate,
        patch("almanak.connectors.traderjoe_v2.TraderJoeV2Config"),
        patch("almanak.connectors.traderjoe_v2.TraderJoeV2Adapter", return_value=adapter),
    ):
        result = impl._compile_lp_close_traderjoe_v2(_close_intent("WETH/USDC/15", bin_ids=[8388600, 8388601]))

    assert result.status is CompilationStatus.SUCCESS, result.error
    read.assert_not_called()
    validate.assert_not_called()
    adapter.sdk.get_pool_address.assert_called_once_with(WETH, USDC, BIN_STEP)


# Reader: ABI decode of the pair-side tuple
def _word_address(address: str) -> bytes:
    return bytes.fromhex(address[2:].rjust(64, "0"))


def _word_uint(value: int) -> bytes:
    return value.to_bytes(32, "big")


_SELECTOR_WORDS = {
    LB_PAIR_GET_TOKEN_X_SELECTOR: _word_address(WETH),
    LB_PAIR_GET_TOKEN_Y_SELECTOR: _word_address(USDC),
    LB_PAIR_GET_BIN_STEP_SELECTOR: _word_uint(BIN_STEP),
}


def _fake_eth_call(overrides: dict[str, bytes | None] | None = None):
    words = {**_SELECTOR_WORDS, **(overrides or {})}

    def _call(rpc_url, to, data, *args, **kwargs):
        assert to == POOL
        return words[data]

    return _call


def test_reader_selectors_match_the_lb_pair_abi() -> None:
    """Selectors are keccak4 of the LBPair.json signatures, not guessed constants."""
    from eth_utils import keccak

    assert LB_PAIR_GET_TOKEN_X_SELECTOR == "0x" + keccak(text="getTokenX()")[:4].hex()
    assert LB_PAIR_GET_TOKEN_Y_SELECTOR == "0x" + keccak(text="getTokenY()")[:4].hex()
    assert LB_PAIR_GET_BIN_STEP_SELECTOR == "0x" + keccak(text="getBinStep()")[:4].hex()


def test_reader_decodes_the_pair_tuple() -> None:
    with patch("almanak.connectors.traderjoe_v2.pool_validation.eth_call", side_effect=_fake_eth_call()):
        binding = read_lb_pair_binding(POOL, None, chain="arbitrum", gateway_client=_Gateway())
    assert binding == LBPairBinding(token_x=WETH, token_y=USDC, bin_step=BIN_STEP)


def test_reader_returns_none_without_any_transport() -> None:
    with patch("almanak.connectors.traderjoe_v2.pool_validation.eth_call") as call:
        assert read_lb_pair_binding(POOL, None, chain="arbitrum", gateway_client=None) is None
    call.assert_not_called()


@pytest.mark.parametrize(
    "overrides",
    [
        {LB_PAIR_GET_TOKEN_X_SELECTOR: None},
        {LB_PAIR_GET_TOKEN_Y_SELECTOR: _word_address("0x" + "00" * 20)},
        {LB_PAIR_GET_TOKEN_Y_SELECTOR: _word_address(WETH)},
        {LB_PAIR_GET_BIN_STEP_SELECTOR: None},
        {LB_PAIR_GET_BIN_STEP_SELECTOR: _word_uint(0)},
        {LB_PAIR_GET_BIN_STEP_SELECTOR: _word_uint(1 << 16)},
        {LB_PAIR_GET_BIN_STEP_SELECTOR: b"\x00" * 16},
    ],
    ids=[
        "token-x-read-fails",
        "token-y-zero",
        "token-y-equals-token-x",
        "bin-step-read-fails",
        "bin-step-zero",
        "bin-step-overflows-uint16",
        "bin-step-short-word",
    ],
)
def test_reader_returns_none_on_non_pool_values(overrides: dict[str, bytes | None]) -> None:
    with patch("almanak.connectors.traderjoe_v2.pool_validation.eth_call", side_effect=_fake_eth_call(overrides)):
        assert read_lb_pair_binding(POOL, None, chain="arbitrum", gateway_client=_Gateway()) is None


def test_bare_address_detector_matches_the_sibling_lanes() -> None:
    assert traderjoe_compiler._looks_like_bare_lb_pair(POOL)
    assert traderjoe_compiler._looks_like_bare_lb_pair("0X" + "ab" * 20)
    assert not traderjoe_compiler._looks_like_bare_lb_pair("WETH/USDC/15")
    assert not traderjoe_compiler._looks_like_bare_lb_pair("0xabc/USDC")
