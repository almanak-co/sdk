"""Entry and withdrawal must preserve an executable, exact owned-pool lifecycle."""

import json
from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from almanak.connectors.uniswap_v4.adapter import UniswapV4Adapter, UniswapV4Config
from almanak.connectors.uniswap_v4.behavior import CallbackFreeOperationProfile
from almanak.connectors.uniswap_v4.compiler import UniswapV4Compiler
from almanak.connectors.uniswap_v4.operation import validate_execution
from almanak.connectors.uniswap_v4.pool_key import PoolKey
from almanak.framework.intents.vocabulary import CollectFeesIntent, LPCloseIntent, LPOpenIntent
from tests.unit.connectors.uniswap_v4.test_position_observation import KEY, TOKEN, WALLET, ZERO, PositionGateway


def adapter_for(gateway):
    return UniswapV4Adapter(
        config=UniswapV4Config(chain="base", wallet_address=WALLET),
        venue_verification_gateway_factory=lambda: gateway,
    )


def test_close_refuses_pool_hint_for_different_owned_nft():
    gateway = PositionGateway()
    adapter = adapter_for(gateway)
    intent = LPCloseIntent(position_id="42", pool=replace(KEY, fee=778).pool_id, protocol="uniswap_v4")
    with pytest.raises(ValueError, match="does not match"):
        adapter.compile_lp_close_intent(intent, gateway.liquidity, KEY.currency0, KEY.currency1)


@pytest.mark.parametrize("pin", ["pool", "pool_key", "pool_id", "fee_tier", "tick_spacing", "hooks"])
def test_collection_compiler_cannot_discard_explicit_identity(pin):
    gateway = PositionGateway()
    adapter = adapter_for(gateway)
    compiler = UniswapV4Compiler()
    params = {"position_id": "42", "currency0": KEY.currency0, "currency1": KEY.currency1}
    wrong = replace(KEY, fee=778)
    pool = KEY.pool_id
    if pin == "pool":
        pool = wrong.pool_id
    elif pin == "pool_key":
        params[pin] = wrong.to_wire()
    elif pin == "pool_id":
        params[pin] = wrong.pool_id
    elif pin == "fee_tier":
        params[pin] = wrong.fee
    elif pin == "tick_spacing":
        params[pin] = 11
    else:
        params[pin] = "0x" + "3" * 36 + "1000"
    intent = CollectFeesIntent(pool=pool, protocol="uniswap_v4", protocol_params=params)
    with patch.object(compiler, "_adapter", return_value=adapter):
        result = compiler.compile_collect_fees(SimpleNamespace(rpc_url=None), intent)
    assert result.status.value == "FAILED"
    assert "match" in result.error or "conflict" in result.error
    assert result.action_bundle is None


@pytest.mark.parametrize("zero", [0, "0"])
def test_explicit_zero_liquidity_retains_full_close_continuity(zero):
    gateway = PositionGateway()
    adapter = adapter_for(gateway)
    compiler = UniswapV4Compiler()
    intent = LPCloseIntent(
        position_id="42",
        pool=KEY.pool_id,
        protocol="uniswap_v4",
        protocol_params={
            "currency0": KEY.currency0,
            "currency1": KEY.currency1,
            "liquidity": zero,
        },
    )
    with (
        patch.object(compiler, "_adapter", return_value=adapter),
        patch.object(adapter, "get_position_liquidity", return_value=gateway.liquidity),
    ):
        result = compiler.compile_lp_close(SimpleNamespace(rpc_url=None), intent)
    assert result.status.value == "SUCCESS", result.error
    assert result.action_bundle.metadata["close_all"] is True
    gateway.liquidity *= 2
    with pytest.raises(ValueError, match="full-close liquidity changed"):
        validate_execution(result.action_bundle, chain="base", wallet=WALLET, is_safe=False, gateway=gateway)


def lp_open(adapter, key):
    intent = LPOpenIntent(
        pool=key.pool_id,
        protocol="uniswap_v4",
        amount0=Decimal("0.001"),
        amount1=Decimal("0.001"),
        range_lower=Decimal("0.5"),
        range_upper=Decimal("2"),
        protocol_params={"pool_key": key.to_wire(), "hook_data": "0x"},
    )
    with patch.object(adapter, "_resolve_token", side_effect=lambda token, **kw: (token, 18)):
        return adapter.compile_lp_open_intent(intent, {})


def test_lp_entry_refuses_hook_with_no_supported_withdrawal():
    gateway = PositionGateway()
    key = PoolKey(ZERO, TOKEN, 0x800000, 10, "0x" + "3" * 36 + "0200")
    bundle = lp_open(adapter_for(gateway), key)
    assert not bundle.transactions
    assert "lp_close" in bundle.metadata["error"]


def test_entry_revalidates_withdrawal_admission_before_submission():
    class ReviewedRemovalProfile(CallbackFreeOperationProfile):
        name = "test_reviewed_removal"

        def verify(self, **kwargs):
            if kwargs["operation"] != "lp_close":
                return None
            evidence = super().verify(**{**kwargs, "operation": "lp_open"})
            return replace(evidence, operation="lp_close")

    gateway = PositionGateway()
    key = PoolKey(ZERO, TOKEN, 0x800000, 10, "0x" + "3" * 36 + "0200")
    with patch(
        "almanak.connectors.uniswap_v4.behavior.REVIEWED_PROFILES",
        (
            CallbackFreeOperationProfile(),
            ReviewedRemovalProfile(),
        ),
    ):
        bundle = lp_open(adapter_for(gateway), key)
        assert bundle.transactions, bundle.metadata
        assert bundle.metadata["v4_operation"]["exit_hook_evidence"]["operation"] == "lp_close"
        validate_execution(bundle, chain="base", wallet=WALLET, is_safe=False, gateway=gateway)
    with pytest.raises(ValueError, match="lp_close"):
        validate_execution(bundle, chain="base", wallet=WALLET, is_safe=False, gateway=gateway)


@pytest.mark.parametrize("full_key", [True, False])
def test_collect_accepts_exact_pool_id_without_redundant_currency_arguments(full_key):
    gateway = PositionGateway()
    adapter = adapter_for(gateway)
    compiler = UniswapV4Compiler()
    params = {"position_id": "42"}
    if full_key:
        params["pool_key"] = KEY.to_wire()
    intent = CollectFeesIntent(pool=KEY.pool_id, protocol="uniswap_v4", protocol_params=params)
    with (
        patch.object(compiler, "_adapter", return_value=adapter),
        patch.object(adapter, "get_position_currencies", return_value=(KEY.currency0, KEY.currency1)),
    ):
        result = compiler.compile_collect_fees(SimpleNamespace(rpc_url=None), intent)
    assert result.status.value == "SUCCESS", result.error
    assert result.action_bundle.metadata["pool_id"] == KEY.pool_id
    validate_execution(result.action_bundle, chain="base", wallet=WALLET, is_safe=False, gateway=gateway)


class MovingHeadPositionGateway(PositionGateway):
    """Advance latest after the position bracket while honoring numbered reads."""

    def __init__(self):
        super().__init__()
        self.head_reads = 0
        self.numbered_reads = []

    def block_number(self, **kwargs):
        self.head_reads += 1
        return 9 + self.head_reads

    def block_hash(self, *, block_number, **kwargs):
        return f"0x{block_number:064x}"

    def code(self, *, block_number, **kwargs):
        self.numbered_reads.append(block_number)
        return super().code(block_number=block_number, **kwargs)

    def read(self, *, block_number, **kwargs):
        self.numbered_reads.append(block_number)
        self.price = 2**96 if block_number == 10 else 2**96 * 1001 // 1000
        return super().read(block_number=block_number, **kwargs)


def compile_withdrawal(adapter, gateway, operation):
    if operation == "lp_close":
        intent = LPCloseIntent(position_id="42", pool=KEY.pool_id, protocol="uniswap_v4", max_slippage=Decimal("0.005"))
        return adapter.compile_lp_close_intent(intent, gateway.liquidity, KEY.currency0, KEY.currency1), intent
    intent = CollectFeesIntent(pool=KEY.pool_id, protocol="uniswap_v4", protocol_params={"position_id": "42"})
    return adapter.compile_collect_fees_intent(42, KEY.currency0, KEY.currency1, pool=KEY.pool_id), intent


@pytest.mark.parametrize("operation", ["lp_close", "lp_collect_fees"])
def test_moving_head_cannot_separate_withdrawal_basis_from_persisted_quote_block(operation):
    from eth_abi import decode

    from almanak.connectors.uniswap_v4.position import observe_position, withdrawal_minima
    from almanak.framework.execution.result_enricher import ResultEnricher
    from almanak.framework.observability.ledger import deserialize_extracted_data, serialize_extracted_data

    gateway = MovingHeadPositionGateway()
    bundle, intent = compile_withdrawal(adapter_for(gateway), gateway, operation)
    assert gateway.head_reads == 1
    assert set(gateway.numbered_reads) == {10}
    artifact = bundle.metadata["v4_operation"]
    assert artifact["quote_block"] == 10
    assert artifact["quote_block_hash"] == f"0x{10:064x}"
    assert artifact["operation"] == operation
    if operation == "lp_close":
        position = observe_position(PositionGateway(), chain="base", token_id=42, wallet=WALLET)
        expected = withdrawal_minima(position, position.liquidity, 50)
        assert (int(bundle.metadata["amount0_min"]), int(bundle.metadata["amount1_min"])) == expected
        inner, _deadline = decode(["bytes", "uint256"], bytes.fromhex(bundle.transactions[-1]["data"][10:]))
        _actions, parameters = decode(["bytes", "bytes[]"], inner)
        token_id, liquidity, minimum0, minimum1, _hook_data = decode(
            ["uint256", "uint256", "uint128", "uint128", "bytes"], parameters[0]
        )
        assert (token_id, liquidity, minimum0, minimum1) == (42, position.liquidity, *expected)
        newer = replace(position, sqrt_price_x96=2**96 * 1001 // 1000)
        assert withdrawal_minima(newer, newer.liquidity, 50) != expected
    result = SimpleNamespace(success=True, extracted_data={}, transaction_results=[], extraction_warnings=[])
    ResultEnricher().enrich(
        result, intent, SimpleNamespace(chain="base", protocol="uniswap_v4"), bundle_metadata=bundle.metadata
    )
    persisted = deserialize_extracted_data(serialize_extracted_data(result.extracted_data))
    assert persisted["compiler_evidence"]["v4_operation"] == json.loads(json.dumps(artifact))


@pytest.mark.parametrize("operation", ["lp_close", "lp_collect_fees"])
def test_reorg_between_position_and_venue_brackets_refuses_compilation(operation):
    class ReorganizedPositionGateway(MovingHeadPositionGateway):
        def __init__(self):
            super().__init__()
            self.hash_reads = 0

        def block_hash(self, *, block_number, **kwargs):
            self.hash_reads += 1
            # Each observation is internally stable, but the two disagree.
            return "0x" + ("a" if self.hash_reads <= 2 else "b") * 64

    gateway = ReorganizedPositionGateway()
    with pytest.raises(ValueError, match="position observation was reorganized during venue verification"):
        compile_withdrawal(adapter_for(gateway), gateway, operation)


@pytest.mark.parametrize(
    "configured_bps,explicit_tolerance,expected_bps",
    [(50, None, 50), (125, None, 125), (0, None, 0), (125, Decimal("0"), 0), (50, Decimal("0.02"), 200)],
)
def test_close_encodes_configured_default_or_explicit_slippage(configured_bps, explicit_tolerance, expected_bps):
    from eth_abi import decode

    from almanak.connectors.uniswap_v4.position import observe_position, withdrawal_minima

    gateway = PositionGateway()
    adapter = UniswapV4Adapter(
        config=UniswapV4Config(chain="base", wallet_address=WALLET, default_slippage_bps=configured_bps),
        venue_verification_gateway_factory=lambda: gateway,
    )
    intent = LPCloseIntent(position_id="42", pool=KEY.pool_id, protocol="uniswap_v4", max_slippage=explicit_tolerance)
    bundle = adapter.compile_lp_close_intent(intent, gateway.liquidity, KEY.currency0, KEY.currency1)
    position = observe_position(gateway, chain="base", token_id=42, wallet=WALLET)
    expected = withdrawal_minima(position, position.liquidity, expected_bps)
    inner, _ = decode(["bytes", "uint256"], bytes.fromhex(bundle.transactions[-1]["data"][10:]))
    _, parameters = decode(["bytes", "bytes[]"], inner)
    token_id, liquidity, minimum0, minimum1, _ = decode(
        ["uint256", "uint256", "uint128", "uint128", "bytes"], parameters[0]
    )
    assert (token_id, liquidity, minimum0, minimum1) == (42, position.liquidity, *expected)
    assert (int(bundle.metadata["amount0_min"]), int(bundle.metadata["amount1_min"])) == expected
    assert all(value > 0 for value in expected)
    validate_execution(bundle, chain="base", wallet=WALLET, is_safe=False, gateway=gateway)


@pytest.mark.parametrize("configured_bps", [-1, 10000, True])
def test_invalid_default_close_tolerance_cannot_compile(configured_bps):
    gateway = PositionGateway()
    adapter = UniswapV4Adapter(
        config=UniswapV4Config(chain="base", wallet_address=WALLET, default_slippage_bps=configured_bps),
        venue_verification_gateway_factory=lambda: gateway,
    )
    intent = LPCloseIntent(position_id="42", pool=KEY.pool_id, protocol="uniswap_v4")
    with pytest.raises(ValueError, match="slippage below 100%"):
        adapter.compile_lp_close_intent(intent, gateway.liquidity, KEY.currency0, KEY.currency1)
