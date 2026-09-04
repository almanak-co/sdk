"""``swap_params={"pool": "0x..."}`` on Aerodrome / Slipstream is an exact pin, never ignored.

The SwapIntent contract documents the pool pin as an exact execution constraint.
Aerodrome's route resolver used to read only ``classic`` / ``tick_spacing`` /
``stable`` and silently auto-routed a pinned swap. These tests pin the
V3-mirrored contract: on-chain identity read, pair match, conflict detection,
factory round-trip to the same address, and no downgrade to the auto ladder.

A Slipstream router derives the pool from ITS factory, so a pinned CL pool is
routed through the router of the reviewed generation its own ``factory()``
names; an unreviewed factory, or a generation without a router, is refused.
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from almanak.connectors._strategy_base.pool_validation_base import PoolValidationReason, PoolValidationResult
from almanak.connectors.aerodrome import compiler as aerodrome_compiler
from almanak.connectors.aerodrome.addresses import SlipstreamDeployment, slipstream_deployment_for_factory
from almanak.connectors.aerodrome.pool_validation import SlipstreamPoolBinding
from almanak.framework.intents.compiler_models import CompilationResult, CompilationStatus
from almanak.framework.intents.vocabulary import Intent

WETH = "0x4200000000000000000000000000000000000006"
USDC = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
AERO = "0x940181a94a35a4569e4529a3cdfb74e38fd98631"
CLASSIC_POOL = "0xcdac0d6c6c59727a65f871236188350531885c43"  # WETH/USDC volatile
CL_POOL = "0xb2cc224c1c9fee385f8ad6a55b4d94e92359dc59"  # WETH/USDC ts=100 on the legacy Slipstream factory
OTHER_POOL = "0x7f670f78b17dec44d5ef68a48740b6f8849cc2e6"
UNREVIEWED_FACTORY = "0x" + "ab" * 20


def _generation(factory: str) -> SlipstreamDeployment:
    deployment = slipstream_deployment_for_factory("base", factory)
    assert deployment is not None, factory
    return deployment


CURRENT = _generation("0xf8f2eB4940CFE7d13603DDDD87f123820Fc061Ef")
LEGACY = _generation("0x5e7BB104d84c7CB9B682AaC2F3d509f5F406809A")


def _token(symbol: str, address: str, decimals: int) -> SimpleNamespace:
    return SimpleNamespace(symbol=symbol, address=address, decimals=decimals, is_native=False)


WETH_T = _token("WETH", WETH, 18)
USDC_T = _token("USDC", USDC, 6)


def _compiler(*, placeholders: bool = False, discovery: bool = False) -> SimpleNamespace:
    return SimpleNamespace(
        chain="base",
        _gateway_client=None,
        _get_chain_rpc_url=lambda: "http://localhost:8545",
        _config=SimpleNamespace(using_placeholders=placeholders, permission_discovery=discovery),
    )


def _intent(**swap_params) -> Intent:
    return Intent.swap(
        from_token="WETH",
        to_token="USDC",
        amount=Decimal("1"),
        protocol="aerodrome",
        chain="base",
        swap_params=swap_params,
    )


def _confirmed(pool: str, factory: str | None = None) -> PoolValidationResult:
    return PoolValidationResult(exists=True, reason=PoolValidationReason.CONFIRMED, pool_address=pool, factory=factory)


def _route(compiler=None, **swap_params):
    compiler = compiler or _compiler()
    intent = _intent(**swap_params)
    return aerodrome_compiler._resolve_aerodrome_route(compiler, intent, WETH_T, USDC_T, intent.swap_params)


def _classic(value=(WETH, USDC, False)):
    return patch.object(aerodrome_compiler, "get_aerodrome_pool_metadata", return_value=value)


def _cl(binding: SlipstreamPoolBinding | None):
    return patch("almanak.connectors.aerodrome.pool_validation.read_slipstream_cl_pool_binding", return_value=binding)


def _classic_factory(result: PoolValidationResult):
    return patch("almanak.connectors.aerodrome.pool_validation.validate_aerodrome_pool", return_value=result)


def _cl_factory(result: PoolValidationResult):
    return patch("almanak.connectors.aerodrome.pool_validation.validate_aerodrome_cl_pool", return_value=result)


def _cl_binding(*, factory: str = LEGACY.factory, tick_spacing: int = 100, token1: str = USDC) -> SlipstreamPoolBinding:
    return SlipstreamPoolBinding(token0=WETH, token1=token1, tick_spacing=tick_spacing, factory=factory.lower())


def _failed(result) -> CompilationResult:
    assert isinstance(result, CompilationResult), result
    assert result.status is CompilationStatus.FAILED
    return result


# Classic pins
def test_classic_pin_routes_to_that_exact_pool_with_no_fallback() -> None:
    with _classic(), _classic_factory(_confirmed(CLASSIC_POOL)) as factory:
        route = _route(pool=CLASSIC_POOL)
    assert isinstance(route, aerodrome_compiler._AerodromeRoute)
    assert (route.use_classic, route.stable, route.routing, route.fallback_used) == (True, False, "classic", False)
    assert route.pinned_pool == CLASSIC_POOL
    assert route.pool_check.pool_address == CLASSIC_POOL
    assert route.deployment is None
    assert factory.call_args.args[:4] == ("base", WETH, USDC, False)


def test_classic_pin_refuses_alternate_pool_substitution() -> None:
    with _classic(), _classic_factory(_confirmed(OTHER_POOL)):
        result = _failed(_route(pool=CLASSIC_POOL))
    assert "Refusing alternate-pool substitution" in (result.error or "")


def test_classic_pin_requires_factory_confirmation() -> None:
    unavailable = PoolValidationResult(exists=None, reason=PoolValidationReason.RPC_FAILED, warning="rpc down")
    with _classic(), _classic_factory(unavailable):
        result = _failed(_route(pool=CLASSIC_POOL))
    assert "Cannot authenticate exact Aerodrome pool" in (result.error or "")


def test_classic_pin_rejects_pair_mismatch() -> None:
    with _classic((WETH, AERO, False)), _classic_factory(_confirmed(CLASSIC_POOL)) as factory:
        result = _failed(_route(pool=CLASSIC_POOL))
    assert "does not match the swap pair" in (result.error or "")
    factory.assert_not_called()


@pytest.mark.parametrize(
    "extra, needle",
    [
        ({"classic": False}, "classic=False"),
        ({"tick_spacing": 100}, "tick_spacing was given"),
        ({"stable": True}, "stable=True was given"),
    ],
)
def test_classic_pin_rejects_conflicting_keys(extra: dict, needle: str) -> None:
    with _classic(), _classic_factory(_confirmed(CLASSIC_POOL)) as factory:
        result = _failed(_route(pool=CLASSIC_POOL, **extra))
    assert "swap_params conflict" in (result.error or "")
    assert needle in (result.error or "")
    factory.assert_not_called()


def test_classic_pin_accepts_consistent_stable_key() -> None:
    with _classic((WETH, USDC, True)), _classic_factory(_confirmed(CLASSIC_POOL)):
        route = _route(pool=CLASSIC_POOL, stable=True)
    assert isinstance(route, aerodrome_compiler._AerodromeRoute)
    assert route.stable is True


# Slipstream CL pins
def test_cl_pin_routes_at_the_contract_tick_spacing() -> None:
    with _classic(None), _cl(_cl_binding()), _cl_factory(_confirmed(CL_POOL, LEGACY.factory)) as factory:
        route = _route(pool=CL_POOL)
    assert isinstance(route, aerodrome_compiler._AerodromeRoute)
    assert (route.use_classic, route.tick_spacing, route.routing, route.fallback_used) == (False, 100, "cl", False)
    assert route.pinned_pool == CL_POOL
    assert route.deployment == LEGACY
    assert factory.call_args.args[:4] == ("base", WETH, USDC, 100)
    assert factory.call_args.kwargs["deployment"] == LEGACY


@pytest.mark.parametrize("generation", [CURRENT, LEGACY], ids=["current", "legacy"])
def test_cl_pin_routes_through_the_router_of_the_generation_the_pool_claims(
    generation: SlipstreamDeployment,
) -> None:
    """The pool's own factory() selects the generation; that generation's router and factory are used."""
    with (
        _classic(None),
        _cl(_cl_binding(factory=generation.factory)),
        _cl_factory(_confirmed(CL_POOL, generation.factory)) as factory,
    ):
        route = _route(pool=CL_POOL)
    assert isinstance(route, aerodrome_compiler._AerodromeRoute)
    assert route.deployment == generation
    assert route.deployment.swap_router == generation.swap_router
    assert factory.call_args.kwargs["deployment"] == generation


def test_cl_pin_on_an_unreviewed_factory_is_refused_before_the_round_trip() -> None:
    with _classic(None), _cl(_cl_binding(factory=UNREVIEWED_FACTORY)), _cl_factory(_confirmed(CL_POOL)) as factory:
        result = _failed(_route(pool=CL_POOL))
    assert "not a reviewed Slipstream factory generation" in (result.error or "")
    assert UNREVIEWED_FACTORY in (result.error or "")
    factory.assert_not_called()


def test_cl_pin_on_a_generation_without_a_router_is_refused() -> None:
    """A reviewed LP-only generation has pools no reviewed router can reach."""
    lp_only = SlipstreamDeployment(factory=UNREVIEWED_FACTORY, position_manager="0x" + "cd" * 20, generation="lp-only")
    with (
        _classic(None),
        _cl(_cl_binding(factory=UNREVIEWED_FACTORY)),
        patch.object(aerodrome_compiler, "slipstream_deployment_for_factory", return_value=lp_only),
        _cl_factory(_confirmed(CL_POOL)) as factory,
    ):
        result = _failed(_route(pool=CL_POOL))
    assert "has no reviewed swap router" in (result.error or "")
    assert "lp-only" in (result.error or "")
    factory.assert_not_called()


def test_cl_pin_refuses_alternate_pool_substitution() -> None:
    with _classic(None), _cl(_cl_binding()), _cl_factory(_confirmed(OTHER_POOL, LEGACY.factory)):
        result = _failed(_route(pool=CL_POOL))
    assert "Refusing alternate-pool substitution" in (result.error or "")


def test_cl_pin_rejects_pair_mismatch() -> None:
    with _classic(None), _cl(_cl_binding(token1=AERO)), _cl_factory(_confirmed(CL_POOL)) as factory:
        result = _failed(_route(pool=CL_POOL))
    assert "does not match the swap pair" in (result.error or "")
    factory.assert_not_called()


@pytest.mark.parametrize(
    "extra, needle",
    [
        ({"classic": True}, "classic=True"),
        ({"stable": False}, "stable was given"),
        ({"tick_spacing": 200}, "tick_spacing=200 was given"),
    ],
)
def test_cl_pin_rejects_conflicting_keys(extra: dict, needle: str) -> None:
    with _classic(None), _cl(_cl_binding()), _cl_factory(_confirmed(CL_POOL)) as factory:
        result = _failed(_route(pool=CL_POOL, **extra))
    assert needle in (result.error or "")
    factory.assert_not_called()


def test_cl_pin_accepts_consistent_tick_spacing_key() -> None:
    with _classic(None), _cl(_cl_binding()), _cl_factory(_confirmed(CL_POOL, LEGACY.factory)):
        route = _route(pool=CL_POOL, tick_spacing=100)
    assert isinstance(route, aerodrome_compiler._AerodromeRoute)
    assert route.tick_spacing == 100


def test_cl_pin_on_a_classic_only_chain_is_refused() -> None:
    compiler = _compiler()
    compiler.chain = "optimism"
    with _classic(None), _cl(_cl_binding()):
        result = _failed(_route(compiler, pool=CL_POOL))
    assert "CL routing is not available on optimism" in (result.error or "")


# Neither family / shape / offline
def test_pin_that_answers_neither_family_fails_closed() -> None:
    with _classic(None), _cl(None), _cl_factory(_confirmed(CL_POOL)) as factory:
        result = _failed(_route(pool=WETH))
    assert "answers neither" in (result.error or "")
    factory.assert_not_called()


def test_malformed_pin_is_rejected_at_intent_construction() -> None:
    with pytest.raises(ValueError, match="swap_params.pool must be a 0x-prefixed 20-byte hex address"):
        _intent(pool="0xdead")


def test_malformed_pin_fails_before_any_read_at_the_resolver() -> None:
    """Defense in depth: the resolver re-checks the shape even if a caller bypasses the intent validator."""
    with _classic() as read:
        result = _failed(
            aerodrome_compiler._resolve_aerodrome_pinned_route(
                _compiler(), "swap-1", WETH_T, USDC_T, {"pool": "0xdead"}
            )
        )
    assert result.error == "Invalid pinned pool address: 0xdead"
    read.assert_not_called()


def test_pin_is_never_downgraded_to_auto_routing() -> None:
    """A failed pin must not fall through to the CL/Classic probe ladder."""
    with (
        _classic(None),
        _cl(None),
        patch.object(aerodrome_compiler, "_aerodrome_cached_probe") as probe,
    ):
        _failed(_route(pool=WETH))
    probe.assert_not_called()


def test_permission_discovery_bypasses_the_pin_like_v3() -> None:
    """Swap permissions are router-scoped, so offline discovery needs no pool resolution."""
    with _classic() as read, _cl_factory(_confirmed(CL_POOL)):
        route = _route(_compiler(discovery=True), pool=CLASSIC_POOL)
    assert isinstance(route, aerodrome_compiler._AerodromeRoute)
    assert route.degraded is True
    assert route.pinned_pool is None
    read.assert_not_called()


def test_placeholder_pricing_does_not_waive_the_pin() -> None:
    """Placeholder prices are not a licence to route elsewhere: the pin is still resolved."""
    with _classic(), _classic_factory(_confirmed(CLASSIC_POOL)) as factory:
        route = _route(_compiler(placeholders=True), pool=CLASSIC_POOL)
    assert isinstance(route, aerodrome_compiler._AerodromeRoute)
    assert route.pinned_pool == CLASSIC_POOL
    factory.assert_called_once()


def test_placeholder_pricing_with_unreadable_pin_fails_closed() -> None:
    with _classic(None), _cl(None), patch.object(aerodrome_compiler, "_aerodrome_cached_probe") as probe:
        result = _failed(_route(_compiler(placeholders=True), pool=CLASSIC_POOL))
    assert "answers neither" in (result.error or "")
    probe.assert_not_called()


# Through compile_swap_aerodrome: the pin reaches the adapter and metadata
class _Compiler:
    chain = "base"
    wallet_address = "0x" + "11" * 20
    default_deadline_seconds = 300
    price_oracle = {"USDC": Decimal("1"), "WETH": Decimal("3000")}
    _gateway_client = None
    _config = SimpleNamespace(
        max_price_impact_pct=Decimal("0.30"), using_placeholders=False, permission_discovery=False
    )

    def _resolve_token(self, sym):
        token = MagicMock()
        token.symbol = sym
        token.address, token.decimals = {"WETH": (WETH, 18), "USDC": (USDC, 6)}[sym]
        token.is_native = False
        token.to_dict.return_value = {"symbol": sym, "address": token.address, "decimals": token.decimals}
        return token

    def _require_token_price(self, sym):
        return self.price_oracle[sym]

    def _get_chain_rpc_url(self):
        return "http://localhost:8545"

    def _validate_pool(self, result, intent_id):
        return None


@pytest.mark.parametrize("generation", [CURRENT, LEGACY], ids=["current", "legacy"])
def test_compiled_swap_carries_the_pin_into_adapter_call_and_metadata(generation: SlipstreamDeployment) -> None:
    tx = MagicMock(gas_estimate=120_000)
    tx.to_dict.return_value = {"tx_type": "swap"}
    quote = MagicMock(amount_out=3000 * 10**6, is_onchain=True)
    swap_result = MagicMock(success=True, transactions=[tx], quote=quote, error=None)
    with (
        _classic(None),
        _cl(_cl_binding(factory=generation.factory)),
        _cl_factory(_confirmed(CL_POOL, generation.factory)),
        patch("almanak.connectors.aerodrome.AerodromeConfig"),
        patch("almanak.connectors.aerodrome.AerodromeAdapter") as adapter_cls,
    ):
        adapter_cls.return_value.swap_exact_input.return_value = swap_result
        result = aerodrome_compiler.compile_swap_aerodrome(_Compiler(), _intent(pool=CL_POOL))
    assert result.status is CompilationStatus.SUCCESS, result.error
    kwargs = adapter_cls.return_value.swap_exact_input.call_args.kwargs
    assert (kwargs["use_classic"], kwargs["tick_spacing"]) == (False, 100)
    assert kwargs["deployment"] == generation
    meta = result.action_bundle.metadata
    assert meta["pinned_pool"] == CL_POOL
    assert (meta["routing"], meta["tick_spacing"], meta["routing_fallback"]) == ("cl", 100, False)
    assert meta["slipstream_deployment"] == generation.generation
    assert meta["swap_router"] == generation.swap_router
