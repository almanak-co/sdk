"""Fail-closed exact-address Aerodrome Slipstream LP_OPEN (ALM-3462).

Before this lane existed, ``compile_lp_open_aerodrome_slipstream`` rejected
every bare ``0x…`` pool address at its format gate ("Invalid pool format") even
though ``_verify_slipstream_binding`` already knew how to certify one. These
tests pin the new contract, mirrored from the Uniswap V3 exact lane:

- a bare address is an execution constraint, never a discovery hint;
- the pool's own ``token0/token1/tickSpacing/factory`` are read through the
  gateway boundary only;
- the pool's claimed factory must be a reviewed generation AND must round-trip
  the tuple to the same address (no alternate-pool substitution);
- new positions are still admitted only through the current generation.
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from almanak.connectors._strategy_base.pool_validation_base import PoolValidationReason, PoolValidationResult
from almanak.connectors.aerodrome import compiler as aerodrome_compiler
from almanak.connectors.aerodrome.addresses import slipstream_lp_deployments
from almanak.connectors.aerodrome.pool_validation import (
    SlipstreamPoolBinding,
    read_slipstream_cl_pool_binding,
)
from almanak.framework.intents.compiler_models import CompilationResult, CompilationStatus, TokenInfo
from almanak.framework.intents.vocabulary import LPOpenIntent, PriceBand

# Real Base addresses. WETH/VVV tick_spacing=100 on the CURRENT factory, as
# read on-chain 2026-09-02 (factory() == 0xf8f2…061Ef, tickSpacing() == 100).
POOL = "0xa135b59fe221c0c8d441294f97f96fbc37bc9fbe"
OTHER_POOL = "0x7ec6c9d993d9832aa654593f2dbc21303650bc6c"
WETH = "0x4200000000000000000000000000000000000006"
VVV = "0xacfe6019ed1a7dc6f7b508c02d1b04ec88cc21bf"
UNREVIEWED_FACTORY = "0x" + "ab" * 20

CURRENT, LEGACY = slipstream_lp_deployments("base")

_TOKENS = {
    WETH: TokenInfo("WETH", WETH, 18),
    VVV: TokenInfo("VVV", VVV, 18),
}


class _Gateway:
    is_connected = True


class _DisconnectedGateway:
    is_connected = False


def _compiler(*, gateway: object | None = _Gateway(), internal_preflight: bool = False) -> SimpleNamespace:
    return SimpleNamespace(
        chain="base",
        _gateway_client=gateway,
        _gateway_internal_preflight=internal_preflight,
        _get_chain_rpc_url=lambda: "http://gateway-internal:8545",
        _resolve_token=lambda token: _TOKENS.get(token.lower()),
        _validate_pool=lambda result, intent_id: None,
    )


def _binding(*, factory: str = CURRENT.factory, tick_spacing: int = 100) -> SlipstreamPoolBinding:
    return SlipstreamPoolBinding(token0=WETH, token1=VVV, tick_spacing=tick_spacing, factory=factory.lower())


def _confirmed(pool: str = POOL) -> PoolValidationResult:
    return PoolValidationResult(exists=True, reason=PoolValidationReason.CONFIRMED, pool_address=pool)


def _resolve(compiler: SimpleNamespace, pool: str = POOL):
    return aerodrome_compiler._resolve_exact_slipstream_pool(compiler, pool, "open-1")


def _failed(result: object) -> CompilationResult:
    assert isinstance(result, CompilationResult)
    assert result.status is CompilationStatus.FAILED
    assert result.intent_id == "open-1"
    return result


# Resolver: boundary and input gates (no reads issued)
def test_exact_pool_rejects_malformed_address_before_reads() -> None:
    with patch("almanak.connectors.aerodrome.pool_validation.read_slipstream_cl_pool_binding") as read:
        result = _failed(_resolve(_compiler(), "0x1234"))
    assert result.error == "Invalid exact Slipstream pool address: 0x1234"
    read.assert_not_called()


@pytest.mark.parametrize("gateway", [None, _DisconnectedGateway()])
def test_exact_pool_requires_gateway_boundary(gateway: object | None) -> None:
    with patch("almanak.connectors.aerodrome.pool_validation.read_slipstream_cl_pool_binding") as read:
        result = _failed(_resolve(_compiler(gateway=gateway)))
    assert "connected gateway is required" in (result.error or "")
    read.assert_not_called()


def test_exact_pool_rejects_unsupported_chain_before_reads() -> None:
    compiler = _compiler()
    compiler.chain = "optimism"
    with patch("almanak.connectors.aerodrome.pool_validation.read_slipstream_cl_pool_binding") as read:
        result = _failed(_resolve(compiler))
    assert "not supported on chain 'optimism'" in (result.error or "")
    read.assert_not_called()


def test_exact_pool_strategy_side_reads_never_carry_a_direct_rpc_url() -> None:
    """Outside the gateway process the reader must receive rpc_url=None (blueprint 20)."""
    with (
        patch(
            "almanak.connectors.aerodrome.pool_validation.read_slipstream_cl_pool_binding",
            return_value=_binding(),
        ) as read,
        patch(
            "almanak.connectors.aerodrome.pool_validation.validate_aerodrome_cl_pool",
            return_value=_confirmed(),
        ) as validate,
    ):
        _resolve(_compiler(internal_preflight=False))
    assert read.call_args.args[1] is None
    assert validate.call_args.args[4] is None


def test_exact_pool_gateway_internal_preflight_may_use_internal_rpc() -> None:
    with (
        patch(
            "almanak.connectors.aerodrome.pool_validation.read_slipstream_cl_pool_binding",
            return_value=_binding(),
        ) as read,
        patch(
            "almanak.connectors.aerodrome.pool_validation.validate_aerodrome_cl_pool",
            return_value=_confirmed(),
        ),
    ):
        resolved = _resolve(_compiler(gateway=None, internal_preflight=True))
    assert not isinstance(resolved, CompilationResult)
    assert read.call_args.args[1] == "http://gateway-internal:8545"


# Resolver: on-chain authentication
def test_exact_pool_fails_closed_when_pool_reads_fail() -> None:
    with (
        patch(
            "almanak.connectors.aerodrome.pool_validation.read_slipstream_cl_pool_binding",
            return_value=None,
        ),
        patch("almanak.connectors.aerodrome.pool_validation.validate_aerodrome_cl_pool") as validate,
    ):
        result = _failed(_resolve(_compiler()))
    assert "reads failed or returned non-pool values" in (result.error or "")
    validate.assert_not_called()


def test_exact_pool_rejects_unreviewed_factory_before_round_trip() -> None:
    with (
        patch(
            "almanak.connectors.aerodrome.pool_validation.read_slipstream_cl_pool_binding",
            return_value=_binding(factory=UNREVIEWED_FACTORY),
        ),
        patch("almanak.connectors.aerodrome.pool_validation.validate_aerodrome_cl_pool") as validate,
    ):
        result = _failed(_resolve(_compiler()))
    assert f"unreviewed factory {UNREVIEWED_FACTORY}" in (result.error or "")
    validate.assert_not_called()


def test_exact_pool_fails_closed_on_missing_token_metadata() -> None:
    compiler = _compiler()
    compiler._resolve_token = lambda token: _TOKENS.get(token.lower()) if token.lower() == WETH else None
    with (
        patch(
            "almanak.connectors.aerodrome.pool_validation.read_slipstream_cl_pool_binding",
            return_value=_binding(),
        ),
        patch("almanak.connectors.aerodrome.pool_validation.validate_aerodrome_cl_pool") as validate,
    ):
        result = _failed(_resolve(compiler))
    assert f"Cannot resolve token metadata for {VVV}" in (result.error or "")
    validate.assert_not_called()


def test_exact_pool_round_trips_tuple_through_the_pool_claimed_factory() -> None:
    with (
        patch(
            "almanak.connectors.aerodrome.pool_validation.read_slipstream_cl_pool_binding",
            return_value=_binding(),
        ),
        patch(
            "almanak.connectors.aerodrome.pool_validation.validate_aerodrome_cl_pool",
            return_value=_confirmed(),
        ) as validate,
    ):
        resolved = _resolve(_compiler())

    assert not isinstance(resolved, CompilationResult)
    assert (resolved.token0.address, resolved.token1.address) == (WETH, VVV)
    assert resolved.tick_spacing == 100
    assert resolved.deployment == CURRENT
    assert resolved.pool_check.pool_address == POOL
    # The factory queried is the one the pool claims — passed as the exact
    # reviewed deployment, never the newest-first default.
    assert validate.call_args.args[:4] == ("base", WETH, VVV, 100)
    assert validate.call_args.kwargs["deployment"] == CURRENT


@pytest.mark.parametrize(
    "factory_result",
    [
        PoolValidationResult(exists=False, reason=PoolValidationReason.NOT_FOUND, error="no pool"),
        PoolValidationResult(exists=None, reason=PoolValidationReason.RPC_FAILED, warning="rpc down"),
        PoolValidationResult(exists=None, reason=PoolValidationReason.RPC_UNAVAILABLE, warning="no rpc"),
    ],
)
def test_exact_pool_requires_a_positive_factory_answer(factory_result: PoolValidationResult) -> None:
    """Unlike the symbolic lane, 'could not verify' is not admission for an exact address."""
    with (
        patch(
            "almanak.connectors.aerodrome.pool_validation.read_slipstream_cl_pool_binding",
            return_value=_binding(),
        ),
        patch(
            "almanak.connectors.aerodrome.pool_validation.validate_aerodrome_cl_pool",
            return_value=factory_result,
        ),
    ):
        result = _failed(_resolve(_compiler()))
    assert "Cannot authenticate exact Slipstream pool" in (result.error or "")


def test_exact_pool_refuses_alternate_pool_substitution() -> None:
    with (
        patch(
            "almanak.connectors.aerodrome.pool_validation.read_slipstream_cl_pool_binding",
            return_value=_binding(),
        ),
        patch(
            "almanak.connectors.aerodrome.pool_validation.validate_aerodrome_cl_pool",
            return_value=_confirmed(OTHER_POOL),
        ),
    ):
        result = _failed(_resolve(_compiler()))
    assert "Refusing alternate-pool substitution" in (result.error or "")
    assert OTHER_POOL in (result.error or "")


def test_exact_pool_on_legacy_generation_is_refused_with_generation_named() -> None:
    """An authenticated legacy pool is a real pool, but mint is current-generation only (ALM-3451)."""
    with (
        patch(
            "almanak.connectors.aerodrome.pool_validation.read_slipstream_cl_pool_binding",
            return_value=_binding(factory=LEGACY.factory),
        ),
        patch(
            "almanak.connectors.aerodrome.pool_validation.validate_aerodrome_cl_pool",
            return_value=_confirmed(OTHER_POOL),
        ) as validate,
    ):
        result = _failed(_resolve(_compiler(), OTHER_POOL))
    assert validate.call_args.kwargs["deployment"] == LEGACY
    assert "legacy factory generation" in (result.error or "")
    assert "ALM-3451" in (result.error or "")


# Compiler dispatch: the bare address clears the format gate
def _intent(pool: str = POOL) -> LPOpenIntent:
    return LPOpenIntent(
        pool=pool,
        amount0=Decimal("0.01"),
        amount1=Decimal("100"),
        range_spec=PriceBand(lower=Decimal("1000"), upper=Decimal("5000")),
        protocol="aerodrome_slipstream",
    )


def _full_compiler() -> MagicMock:
    compiler = MagicMock()
    compiler.chain = "base"
    compiler._gateway_client = _Gateway()
    compiler._gateway_internal_preflight = False
    compiler._resolve_token = lambda token: _TOKENS.get(token.lower())
    compiler._fetch_lp_pool_slot0.return_value = (2**96, 50)
    compiler.default_lp_slippage = Decimal("0.99")
    compiler.wallet_address = "0x" + "33" * 20
    compiler.price_oracle = {}
    return compiler


def test_bare_address_is_no_longer_rejected_at_the_format_gate() -> None:
    """The ALM-3462 signature: a bare address used to die with 'Invalid pool format'."""
    compiler = _full_compiler()
    compiler._gateway_client = None  # stops at the gateway boundary, AFTER the format gate
    result = aerodrome_compiler.compile_lp_open_aerodrome_slipstream(compiler, _intent())
    assert result.status is CompilationStatus.FAILED
    assert "Invalid pool format" not in (result.error or "")
    assert "connected gateway is required" in (result.error or "")


def test_symbolic_format_gate_error_advertises_the_exact_address_form() -> None:
    result = aerodrome_compiler.compile_lp_open_aerodrome_slipstream(_full_compiler(), _intent("WETH/VVV"))
    assert result.status is CompilationStatus.FAILED
    assert "Invalid pool format" in (result.error or "")
    assert "exact 0x pool address" in (result.error or "")


def test_bare_address_compiles_mint_bound_to_the_exact_pool_without_substitution() -> None:
    compiler = _full_compiler()
    tx = MagicMock(gas_estimate=250_000, tx_type="mint")
    tx.to_dict.return_value = {"tx_type": "mint"}

    with (
        patch(
            "almanak.connectors.aerodrome.pool_validation.read_slipstream_cl_pool_binding",
            return_value=_binding(),
        ),
        patch(
            "almanak.connectors.aerodrome.pool_validation.validate_aerodrome_cl_pool",
            return_value=_confirmed(),
        ),
        patch.object(aerodrome_compiler, "_resolve_slipstream_ticks", return_value=(0, 100)) as ticks,
        patch.object(aerodrome_compiler, "maybe_recompute_lp_amounts_from_slot0", return_value=(10**16, 10**20)),
        patch.object(aerodrome_compiler, "compute_lp_slippage_mins", return_value=(1, 1)),
        patch.object(aerodrome_compiler, "_verify_slipstream_binding", return_value=None) as verify,
        patch("almanak.connectors.aerodrome.AerodromeConfig"),
        patch("almanak.connectors.aerodrome.AerodromeAdapter") as adapter_cls,
    ):
        adapter_cls.return_value.add_cl_liquidity.return_value = MagicMock(success=True, transactions=[tx], error=None)
        result = aerodrome_compiler.compile_lp_open_aerodrome_slipstream(compiler, _intent())

    assert result.status is CompilationStatus.SUCCESS, result.error
    # Tick spacing and decimals come from the pool contract, not the intent.
    assert ticks.call_args.args[1:] == (100, 18, 18)
    # The verifier certifies THE supplied address on the generation the pool claims.
    assert verify.call_args.kwargs["pool_address"] == POOL
    assert verify.call_args.kwargs["tick_spacing"] == 100
    assert verify.call_args.kwargs["expected_position_manager"] == CURRENT.position_manager
    # Mint is keyed by the pool's own canonical token addresses and NPM generation.
    mint = adapter_cls.return_value.add_cl_liquidity.call_args.kwargs
    assert (mint["token_a"], mint["token_b"], mint["tick_spacing"]) == (WETH, VVV, 100)
    assert mint["deployment"] == CURRENT
    meta = result.action_bundle.metadata
    assert meta["pool"] == POOL
    assert meta["tick_spacing"] == 100
    assert meta["nft_manager"] == CURRENT.position_manager
    assert meta["slipstream_deployment"] == "current"


def test_exact_lane_refusal_precedes_any_range_or_adapter_work() -> None:
    compiler = _full_compiler()
    with (
        patch(
            "almanak.connectors.aerodrome.pool_validation.read_slipstream_cl_pool_binding",
            return_value=_binding(),
        ),
        patch(
            "almanak.connectors.aerodrome.pool_validation.validate_aerodrome_cl_pool",
            return_value=_confirmed(OTHER_POOL),
        ),
        patch.object(aerodrome_compiler, "_resolve_slipstream_ticks") as ticks,
        patch("almanak.connectors.aerodrome.AerodromeAdapter") as adapter_cls,
    ):
        result = aerodrome_compiler.compile_lp_open_aerodrome_slipstream(compiler, _intent())

    assert result.status is CompilationStatus.FAILED
    assert "Refusing alternate-pool substitution" in (result.error or "")
    ticks.assert_not_called()
    adapter_cls.assert_not_called()


# Reader: ABI decode of the pool-side tuple
def _word_address(address: str) -> bytes:
    return bytes.fromhex(address[2:].rjust(64, "0"))


def _word_int(value: int) -> bytes:
    return value.to_bytes(32, "big", signed=True)


# Fake pool ABI answers keyed by selector, in order: token0, token1, tickSpacing, factory.
_SELECTOR_WORDS = {
    "0x0dfe1681": _word_address(WETH),
    "0xd21220a7": _word_address(VVV),
    "0xd0c93a7c": _word_int(100),
    "0xc45a0155": _word_address(CURRENT.factory),
}


def _fake_eth_call(overrides: dict[str, bytes | None] | None = None):
    words = {**_SELECTOR_WORDS, **(overrides or {})}

    def _call(rpc_url, to, data, *args, **kwargs):
        assert to == POOL
        return words[data]

    return _call


def test_reader_decodes_the_pool_tuple() -> None:
    with patch("almanak.connectors.aerodrome.pool_validation.eth_call", side_effect=_fake_eth_call()):
        binding = read_slipstream_cl_pool_binding(POOL, None, chain="base", gateway_client=_Gateway())
    assert binding == SlipstreamPoolBinding(token0=WETH, token1=VVV, tick_spacing=100, factory=CURRENT.factory.lower())


def test_reader_returns_none_without_any_transport() -> None:
    with patch("almanak.connectors.aerodrome.pool_validation.eth_call") as call:
        assert read_slipstream_cl_pool_binding(POOL, None, chain="base", gateway_client=None) is None
    call.assert_not_called()


@pytest.mark.parametrize(
    "overrides",
    [
        {"0x0dfe1681": None},
        {"0xd21220a7": _word_address("0x" + "00" * 20)},
        {"0xc45a0155": None},
        {"0xc45a0155": _word_address("0x" + "00" * 20)},
        {"0xd0c93a7c": _word_int(0)},
        {"0xd0c93a7c": _word_int(-100)},
        {"0xd0c93a7c": _word_int(1 << 23)},  # exceeds int24
        {"0xd0c93a7c": b"\x00" * 16},  # short word
    ],
    ids=[
        "token0-read-fails",
        "token1-zero",
        "factory-read-fails",
        "factory-zero",
        "tick-spacing-zero",
        "tick-spacing-negative",
        "tick-spacing-overflows-int24",
        "tick-spacing-short-word",
    ],
)
def test_reader_returns_none_on_non_pool_values(overrides: dict[str, bytes | None]) -> None:
    with patch("almanak.connectors.aerodrome.pool_validation.eth_call", side_effect=_fake_eth_call(overrides)):
        assert read_slipstream_cl_pool_binding(POOL, None, chain="base", gateway_client=_Gateway()) is None
