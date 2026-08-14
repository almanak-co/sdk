"""Deployment-scoped Curve pool permission binding regressions (ALM-3287)."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from almanak.connectors.curve import compiler as curve_compiler
from almanak.connectors.curve import pair_resolver, pool_resolver
from almanak.connectors.curve import permission_hints as curve_hints
from almanak.connectors.curve.pool_binding import (
    CurvePoolPermissionBinding,
    permission_binding_from_intent,
    resolve_configured_pool_bindings,
)
from almanak.connectors.curve.pool_reader import POOL_READER_SPEC
from almanak.connectors.curve.pool_resolver import CurvePoolMetadata
from almanak.core.intent_types import IntentType
from almanak.framework.intents.compiler import ERC20_APPROVE_SELECTOR
from almanak.framework.permissions import synthetic_intents
from almanak.framework.permissions.generator import generate_manifest
from almanak.framework.permissions.hints import DiscoveryContext, PermissionBindingError
from tests.support.curve_adapter import (
    ADD_LIQUIDITY_2_SELECTOR,
    ADD_LIQUIDITY_DYN_SELECTOR,
    REMOVE_LIQUIDITY_2_SELECTOR,
    REMOVE_LIQUIDITY_DYN_SELECTOR,
    REMOVE_LIQUIDITY_IMBALANCE_DYN_SELECTOR,
    REMOVE_LIQUIDITY_IMBALANCE_SELECTORS,
    REMOVE_LIQUIDITY_ONE_SELECTOR,
    CurveAdapter,
    PoolInfo,
    PoolType,
)

POOL = "0x" + "aa" * 20
TOKEN0 = "0x" + "11" * 20
TOKEN1 = "0x" + "22" * 20
LP_TOKEN = "0x" + "33" * 20
DAI = "0x6b175474e89094c44da98b954eedeac495271d0f"
USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
USDT = "0xdac17f958d2ee523a2206206994597c13d831ec7"
NATIVE_ETH = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"


def _metadata(*, coins: tuple[str, ...] = (TOKEN0, TOKEN1)) -> CurvePoolMetadata:
    return CurvePoolMetadata(
        address=POOL,
        lp_token=LP_TOKEN,
        coin_addresses=list(coins),
        coin_decimals=[18] * len(coins),
        coin_symbols=[f"TOKEN{index}" for index in range(len(coins))],
        n_coins=len(coins),
        pool_type="stableswap",
        is_metapool=False,
        base_pool=None,
        base_pool_coin_addresses=None,
        base_pool_coins=None,
    )


def _config() -> dict[str, str]:
    return {"chain": "ethereum", "pool": POOL, "token0": TOKEN0, "token1": TOKEN1}


def test_exact_pool_requires_live_binding_transport() -> None:
    """An exact target may never degrade to the curated-only manifest."""
    with pytest.raises(PermissionBindingError, match="no gateway/RPC read transport"):
        generate_manifest(
            strategy_name="exact-curve",
            chain="ethereum",
            supported_protocols=["curve"],
            intent_types=["LP_OPEN"],
            config=_config(),
        )


def test_curve_permission_generation_requires_an_exact_pool_binding() -> None:
    """A protocol name and LP verb alone may not produce an empty/broad Curve role."""
    with pytest.raises(PermissionBindingError, match="requires an exact deployment pool binding"):
        generate_manifest(
            strategy_name="unbound-curve",
            chain="ethereum",
            supported_protocols=["curve"],
            intent_types=["LP_OPEN"],
            config={"protocol": "curve"},
        )


def test_ordered_coin_mismatch_fails_admission(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pool address alone is insufficient: amount indices bind to coin order."""
    monkeypatch.setattr(pool_resolver, "resolve_pool_metadata", lambda **_kwargs: _metadata(coins=(TOKEN1, TOKEN0)))
    monkeypatch.setattr(pool_resolver, "resolution_is_definitive", lambda *_args: True)

    with pytest.raises(PermissionBindingError, match="ordered coins"):
        resolve_configured_pool_bindings(chain="ethereum", config=_config(), rpc_url="http://binding.test")


def test_exact_pool_lifecycle_is_compiled_without_global_catalog(monkeypatch: pytest.MonkeyPatch) -> None:
    """The generated role contains the exact open/close target without SDK pool data."""
    metadata = _metadata()
    monkeypatch.setattr(pool_resolver, "resolve_pool_metadata", lambda **_kwargs: metadata)
    monkeypatch.setattr(pool_resolver, "resolution_is_definitive", lambda *_args: True)
    monkeypatch.setattr(curve_compiler, "_probe_lp_open_deployability", lambda *_args, **_kwargs: (True, "ok"))
    monkeypatch.setattr(curve_compiler, "_deployment_pool_catalog", lambda: {})
    monkeypatch.setattr(pair_resolver, "pool_provenance_suspect", lambda *_args, **_kwargs: False)

    def no_network(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("calldata discovery must stay offline after exact-pool admission")

    monkeypatch.setattr("requests.Session.request", no_network)

    def get_pool_info(self: CurveAdapter, pool_address: str, *, refresh: bool = True) -> PoolInfo | None:
        del refresh
        if pool_address.lower() == POOL.lower():
            return PoolInfo(
                address=POOL,
                lp_token=LP_TOKEN,
                coins=["TOKEN0", "TOKEN1"],
                coin_addresses=[TOKEN0, TOKEN1],
                coin_decimals=[18, 18],
                pool_type=PoolType.STABLESWAP,
                n_coins=2,
                name=f"dynamic:{POOL[:10]}",
                is_ng=self._force_is_ng if self._force_is_ng is not None else True,
            )
        return None

    monkeypatch.setattr(CurveAdapter, "get_pool_info", get_pool_info)
    manifest = generate_manifest(
        strategy_name="exact-curve",
        chain="ethereum",
        supported_protocols=["curve"],
        intent_types=["LP_OPEN"],
        config=_config(),
        rpc_url="http://binding.test",
    )

    targets = {permission.target: permission for permission in manifest.permissions}
    assert POOL.lower() in targets
    selectors = {entry.selector for entry in targets[POOL.lower()].function_selectors}
    # LP_OPEN is expanded to LP_CLOSE. StableSwap admission deliberately
    # includes both ABI families and every supported teardown shape; selector
    # values come from the adapter constants, never literals in this test.
    assert {
        ADD_LIQUIDITY_2_SELECTOR,
        ADD_LIQUIDITY_DYN_SELECTOR,
        REMOVE_LIQUIDITY_2_SELECTOR,
        REMOVE_LIQUIDITY_DYN_SELECTOR,
        REMOVE_LIQUIDITY_ONE_SELECTOR,
        REMOVE_LIQUIDITY_IMBALANCE_SELECTORS[2],
        REMOVE_LIQUIDITY_IMBALANCE_DYN_SELECTOR,
    } <= selectors
    curve_adapter = __import__("almanak.connectors.curve.adapter", fromlist=["CurveAdapter"])
    assert not hasattr(curve_adapter, "CURVE_" + "POOLS")


def test_binding_preserves_symbols_for_adapter_coin_indexing() -> None:
    """Address identity and adapter symbol lookup remain separate, aligned vectors."""
    binding = CurvePoolPermissionBinding.from_metadata("ethereum", _metadata())

    assert binding.pool_data()["coins"] == ["TOKEN0", "TOKEN1"]
    assert binding.pool_data()["coin_addresses"] == [TOKEN0, TOKEN1]
    assert (
        PoolInfo(
            address=POOL,
            lp_token=LP_TOKEN,
            coins=binding.pool_data()["coins"],
            coin_addresses=binding.pool_data()["coin_addresses"],
            coin_decimals=binding.pool_data()["coin_decimals"],
            pool_type=PoolType.STABLESWAP,
            n_coins=2,
            name="bound",
        ).get_coin_index("TOKEN1")
        == 1
    )


def test_binding_is_found_in_swap_params_when_protocol_params_are_nonempty() -> None:
    """An unrelated protocol-param entry cannot shadow the exact swap binding."""
    binding = CurvePoolPermissionBinding.from_metadata("ethereum", _metadata())
    intent = SimpleNamespace(protocol_params={"quote_source": "oracle"}, swap_params=binding.marker_params())

    assert permission_binding_from_intent(intent) == binding


def test_conflicting_parameter_bindings_fail_closed() -> None:
    """Two parameter carriers may not authorize different exact pools."""
    binding = CurvePoolPermissionBinding.from_metadata("ethereum", _metadata())
    conflicting_raw = binding.to_dict()
    conflicting_pool = "0x" + "bb" * 20
    conflicting_raw["pool_address"] = conflicting_pool
    conflicting_raw["required_targets"] = [conflicting_pool]
    conflicting = CurvePoolPermissionBinding.from_dict(conflicting_raw)
    intent = SimpleNamespace(
        protocol_params=binding.marker_params(),
        swap_params=conflicting.marker_params(),
    )

    with pytest.raises(ValueError, match="Conflicting Curve permission bindings"):
        permission_binding_from_intent(intent)


def test_identical_parameter_bindings_are_accepted() -> None:
    binding = CurvePoolPermissionBinding.from_metadata("ethereum", _metadata())
    intent = SimpleNamespace(protocol_params=binding.marker_params(), swap_params=binding.marker_params())

    assert permission_binding_from_intent(intent) == binding


def test_swap_vectors_cover_every_pool_coin_as_input(monkeypatch: pytest.MonkeyPatch) -> None:
    """Every ERC-20/native input gets its own authorization-producing compile."""
    metadata = _metadata(coins=(TOKEN0, TOKEN1, USDT))
    monkeypatch.setattr(pool_resolver, "resolve_pool_metadata", lambda **_kwargs: metadata)
    monkeypatch.setattr(pool_resolver, "resolution_is_definitive", lambda *_args: True)
    ctx = DiscoveryContext(
        usdc=USDC,
        weth="WETH",
        strategy_config={
            "permission_bindings": [
                {
                    "protocol": "curve",
                    "resource_type": "pool",
                    "chain": "ethereum",
                    "address": POOL,
                    "coin_addresses": [TOKEN0, TOKEN1, USDT],
                }
            ]
        },
        rpc_url="http://binding.test",
    )

    vectors = curve_hints.build_discovery_vectors("curve", IntentType.SWAP, "ethereum", ctx)

    assert vectors is not None
    assert len(vectors) == 3
    assert {vector.from_token for vector in vectors} == {TOKEN0, TOKEN1, USDT}
    assert all(vector.from_token != vector.to_token for vector in vectors)


def test_curve_override_runs_without_framework_default_token_pair(monkeypatch: pytest.MonkeyPatch) -> None:
    """Deployment-authenticated discovery must precede framework token defaults."""
    metadata = _metadata()
    monkeypatch.setattr(pool_resolver, "resolve_pool_metadata", lambda **_kwargs: metadata)
    monkeypatch.setattr(pool_resolver, "resolution_is_definitive", lambda *_args: True)
    monkeypatch.setattr(synthetic_intents, "_get_token_pair_or_none", lambda _chain: None)

    vectors = synthetic_intents.build_synthetic_intents(
        "curve",
        IntentType.SWAP,
        "ethereum",
        strategy_config={
            "permission_bindings": [
                {
                    "protocol": "curve",
                    "resource_type": "pool",
                    "chain": "ethereum",
                    "address": POOL,
                    "coin_addresses": [TOKEN0, TOKEN1],
                }
            ]
        },
        rpc_url="http://binding.test",
    )

    assert len(vectors) == 2
    assert {vector.swap_params["pool"] for vector in vectors} == {POOL}


def test_swap_only_manifest_approves_every_bound_input_coin(monkeypatch: pytest.MonkeyPatch) -> None:
    """Exact SWAP compilation works without a catalog and authorizes reverse inputs."""
    metadata = CurvePoolMetadata(
        address=POOL,
        lp_token=LP_TOKEN,
        coin_addresses=[DAI, USDC, USDT, NATIVE_ETH],
        coin_decimals=[18, 6, 6, 18],
        coin_symbols=["DAI", "USDC", "USDT", "ETH"],
        n_coins=4,
        pool_type="stableswap",
        is_metapool=False,
        base_pool=None,
        base_pool_coin_addresses=None,
        base_pool_coins=None,
    )
    monkeypatch.setattr(pool_resolver, "resolve_pool_metadata", lambda **_kwargs: metadata)
    monkeypatch.setattr(pool_resolver, "resolution_is_definitive", lambda *_args: True)
    monkeypatch.setattr(curve_compiler, "_deployment_pool_catalog", lambda: {})

    def no_network(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("swap calldata discovery must stay offline after exact-pool admission")

    monkeypatch.setattr("requests.Session.request", no_network)

    def get_pool_info(self: CurveAdapter, pool_address: str, *, refresh: bool = True) -> PoolInfo | None:
        del refresh
        if pool_address.lower() == POOL.lower():
            return PoolInfo(
                address=POOL,
                lp_token=LP_TOKEN,
                coins=["DAI", "USDC", "USDT", "ETH"],
                coin_addresses=[DAI, USDC, USDT, NATIVE_ETH],
                coin_decimals=[18, 6, 6, 18],
                pool_type=PoolType.STABLESWAP,
                n_coins=4,
                name="bound-4coin",
                is_ng=self._force_is_ng if self._force_is_ng is not None else True,
            )
        return None

    monkeypatch.setattr(CurveAdapter, "get_pool_info", get_pool_info)
    manifest = generate_manifest(
        strategy_name="exact-curve-swap",
        chain="ethereum",
        supported_protocols=["curve"],
        intent_types=["SWAP"],
        config={
            "permission_bindings": [
                {
                    "protocol": "curve",
                    "resource_type": "pool",
                    "chain": "ethereum",
                    "address": POOL,
                    "coin_addresses": [DAI, USDC, USDT, NATIVE_ETH],
                }
            ]
        },
        rpc_url="http://binding.test",
    )

    targets = {permission.target: permission for permission in manifest.permissions}
    for coin in (DAI, USDC, USDT):
        assert coin in targets
        assert ERC20_APPROVE_SELECTOR in {entry.selector for entry in targets[coin].function_selectors}
    assert targets[POOL].send_allowed is True


def test_production_curve_modules_publish_no_static_pool_catalog() -> None:
    """Address-bearing pool fixtures are test-only, not SDK configuration."""
    curve_package = __import__("almanak.connectors.curve", fromlist=["CurveAdapter"])
    curve_adapter = __import__("almanak.connectors.curve.adapter", fromlist=["CurveAdapter"])

    for catalog_name in ("CURVE_" + "POOLS", "CURVE_TEST_POOLS"):
        assert not hasattr(curve_package, catalog_name)
        assert not hasattr(curve_adapter, catalog_name)
    assert POOL_READER_SPEC.known_pools == {}
    assert POOL_READER_SPEC.factory_addresses == {}


def test_runtime_marker_revalidation_rejects_pool_shape_drift() -> None:
    """The serialised admission binding is reusable by runtime compilation."""
    binding = CurvePoolPermissionBinding.from_metadata("ethereum", _metadata())
    intent = SimpleNamespace(protocol_params=binding.marker_params(), swap_params=None)
    drifted = binding.pool_data()
    drifted["coin_addresses"] = [TOKEN1, TOKEN0]

    with pytest.raises(ValueError, match="binding revalidation failed.*ordered coins"):
        curve_compiler._validate_permission_pool_binding(intent, chain="ethereum", pool_data=drifted)


def test_bound_swap_adapter_fails_when_runtime_pool_cannot_be_resolved(monkeypatch: pytest.MonkeyPatch) -> None:
    """A serialised binding cannot compile if runtime identity cannot be read."""
    binding = CurvePoolPermissionBinding.from_metadata("ethereum", _metadata())
    intent = SimpleNamespace(swap_params={"pool": POOL, **binding.marker_params()})
    ctx = SimpleNamespace(
        chain="ethereum",
        wallet_address="0x" + "44" * 20,
        rpc_url="http://runtime.test",
        gateway_client=None,
        permission_discovery=False,
    )
    monkeypatch.setattr(CurveAdapter, "get_pool_info", lambda *_args, **_kwargs: None)

    with pytest.raises(ValueError, match="binding revalidation could not resolve"):
        curve_compiler._swap_adapter_for_pool(ctx, intent, pool_address=POOL, slippage_bps=50)


def test_binding_marker_cannot_drop_the_required_pool_target() -> None:
    """Connector identity evidence cannot opt out of the framework target gate."""
    raw = CurvePoolPermissionBinding.from_metadata("ethereum", _metadata()).to_dict()
    raw["required_targets"] = []

    with pytest.raises(ValueError, match="required_targets must contain only the exact pool address"):
        CurvePoolPermissionBinding.from_dict(raw)


def test_canonical_multi_pool_binding_shape_is_supported(monkeypatch: pytest.MonkeyPatch) -> None:
    """Control planes can supply bindings without relying on strategy-specific key names."""
    monkeypatch.setattr(pool_resolver, "resolve_pool_metadata", lambda **_kwargs: _metadata())
    config = {
        "permission_bindings": [
            {
                "protocol": "curve",
                "resource_type": "pool",
                "chain": "ethereum",
                "address": POOL,
                "coin_addresses": [TOKEN0, TOKEN1],
            }
        ]
    }

    bindings = resolve_configured_pool_bindings(chain="ethereum", config=config, rpc_url="http://binding.test")
    assert bindings == (CurvePoolPermissionBinding.from_metadata("ethereum", _metadata()),)


def test_explicit_binding_owns_matching_top_level_pool(monkeypatch: pytest.MonkeyPatch) -> None:
    """Operational config.pool need not duplicate coins beside its canonical binding."""
    monkeypatch.setattr(pool_resolver, "resolve_pool_metadata", lambda **_kwargs: _metadata())
    config = {
        "chain": "ethereum",
        "protocol": "curve",
        "pool": POOL,
        "permission_bindings": [
            {
                "protocol": "curve",
                "resource_type": "pool",
                "chain": "ethereum",
                "address": POOL,
                "coin_addresses": [TOKEN0, TOKEN1],
            }
        ],
    }

    bindings = resolve_configured_pool_bindings(chain="ethereum", config=config, rpc_url="http://binding.test")
    assert bindings == (CurvePoolPermissionBinding.from_metadata("ethereum", _metadata()),)


def test_edge_token_object_shape_preserves_canonical_order(monkeypatch: pytest.MonkeyPatch) -> None:
    """The Edge execution contract's tokens[].address form is a legacy admission input."""
    monkeypatch.setattr(pool_resolver, "resolve_pool_metadata", lambda **_kwargs: _metadata())
    config = {
        "chain": "ethereum",
        "protocol": "curve",
        "pool": POOL,
        "tokens": [
            {"symbol": "TOKEN0", "address": TOKEN0},
            {"symbol": "TOKEN1", "address": TOKEN1},
        ],
    }

    bindings = resolve_configured_pool_bindings(chain="ethereum", config=config, rpc_url="http://binding.test")
    assert bindings[0].coin_addresses == (TOKEN0, TOKEN1)


def test_legacy_pool_config_explicitly_owned_by_another_protocol_is_ignored() -> None:
    """Curve discovery must not reinterpret another connector's global pool field."""
    config = {**_config(), "protocol": "uniswap_v3"}
    assert resolve_configured_pool_bindings(chain="ethereum", config=config) == ()
