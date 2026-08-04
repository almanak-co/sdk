"""Integration manifest discovery and architecture boundary tests."""

from __future__ import annotations

import ast
import importlib
import pkgutil
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.core.chains import ChainRegistry, ExternalChainIds
from almanak.integrations._base import INTEGRATION_REGISTRY, ImportRef, Integration
from almanak.integrations.chains import (
    integration_chain_id,
    integration_market_symbol,
)

_REPO_ROOT = Path(__file__).resolve().parents[3]


def test_manifests_are_discoverable_and_named_after_their_packages() -> None:
    integrations = INTEGRATION_REGISTRY.all()
    assert integrations
    assert all(isinstance(integration, Integration) for integration in integrations)
    assert [integration.name for integration in integrations] == sorted(
        integration.name for integration in integrations
    )
    assert {"chainlink", "binance", "coingecko", "pyth", "dexscreener"} <= {
        integration.name for integration in integrations
    }


def test_metadata_imports_do_not_load_gateway_implementations(monkeypatch: pytest.MonkeyPatch) -> None:
    names = tuple(integration.name for integration in INTEGRATION_REGISTRY.all())
    for name in names:
        prefix = f"almanak.integrations.{name}.gateway"
        for module in [key for key in sys.modules if key.startswith(prefix)]:
            monkeypatch.delitem(sys.modules, module)
        importlib.reload(importlib.import_module(f"almanak.integrations.{name}.integration"))
        assert not any(key.startswith(prefix) for key in sys.modules)


def test_chain_metadata_and_market_metadata_resolve_through_their_owners() -> None:
    assert ChainRegistry.get("bsc").external_ids.dexscreener == "bsc"
    assert integration_chain_id("bnb", "dexscreener") == "bsc"
    assert integration_chain_id("solana", "okx") == "501"
    assert integration_market_symbol("binance", "WETH", "USDT") == "ETHUSDT"
    assert integration_market_symbol("coinbase", "WETH", "USDC") == "ETH-USD"


@pytest.mark.parametrize(
    ("integration", "base", "quote"),
    [
        ("unknown", "ETH", "USD"),
        ("", "ETH", "USD"),
        ("binance", "", "USDT"),
        ("binance", "ETH", ""),
    ],
)
def test_market_symbol_lookup_fails_closed_for_invalid_inputs(integration: str, base: str, quote: str) -> None:
    assert integration_market_symbol(integration, base, quote) is None


def test_registry_registration_and_lookup_share_canonical_key() -> None:
    from almanak.integrations._base.registry import IntegrationRegistry

    registry = IntegrationRegistry()
    manifest = Integration(name="example")
    registry.register(manifest)

    assert registry.get(" EXAMPLE ") is manifest


def test_legacy_core_chainlink_path_is_an_identity_preserving_shim() -> None:
    from almanak.core import chainlink as legacy
    from almanak.integrations.chainlink import catalog, codec

    expected = {
        "CHAINLINK_CHAIN_IDS": catalog.CHAINLINK_CHAIN_IDS,
        "CHAINLINK_DEVIATION_THRESHOLDS": catalog.CHAINLINK_DEVIATION_THRESHOLDS,
        "CHAINLINK_HEARTBEATS": catalog.CHAINLINK_HEARTBEATS,
        "CHAINLINK_PRICE_FEEDS": catalog.CHAINLINK_PRICE_FEEDS,
        "ETH_DENOMINATED_FEEDS": catalog.ETH_DENOMINATED_FEEDS,
        "TOKEN_TO_ETH_PAIR": catalog.TOKEN_TO_ETH_PAIR,
        "TOKEN_TO_PAIR": catalog.TOKEN_TO_PAIR,
        "DECIMALS_SELECTOR": codec.DECIMALS_SELECTOR,
        "GET_ROUND_DATA_SELECTOR": codec.GET_ROUND_DATA_SELECTOR,
        "LATEST_ROUND_DATA_SELECTOR": codec.LATEST_ROUND_DATA_SELECTOR,
    }
    assert set(legacy.__all__) == set(expected)
    assert all(getattr(legacy, name) is value for name, value in expected.items())


def test_manifest_mappings_are_immutable() -> None:
    integration = INTEGRATION_REGISTRY.get("pyth")
    assert integration.asset_ids is not None
    with pytest.raises(TypeError):
        integration.asset_ids["ETH"] = "changed"  # type: ignore[index]


def test_integration_normalizes_and_freezes_provider_metadata() -> None:
    owned_ref = ImportRef("almanak.integrations.example.gateway.factory", "Factory")
    integration = Integration(
        name="example",
        asset_ids={"ETH": "ethereum"},
        market_symbols={(" eth ", " usd "): "ETHUSD"},
        gateway_price_source=owned_ref,
        gateway_oracle_reader=owned_ref,
        gateway_api_client=owned_ref,
        gateway_portfolio_provider=owned_ref,
    )

    assert integration.asset_ids == {"ETH": "ethereum"}
    assert integration.market_symbols == {("ETH", "USD"): "ETHUSD"}
    with pytest.raises(TypeError):
        integration.market_symbols[("BTC", "USD")] = "BTCUSD"  # type: ignore[index]


@pytest.mark.parametrize("name", ["", " ", "Chainlink", " chainlink", "chainlink "])
def test_integration_rejects_noncanonical_names(name: str) -> None:
    with pytest.raises(ValueError, match="canonical lowercase slug"):
        Integration(name=name)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("asset_ids", {"": "ethereum"}, "asset_ids"),
        ("asset_ids", {"ETH": ""}, "asset_ids"),
        ("asset_ids", {"ETH": 1}, "asset_ids"),
        ("market_symbols", {("", "USD"): "ETHUSD"}, "market_symbols"),
        ("market_symbols", {("ETH", ""): "ETHUSD"}, "market_symbols"),
        ("market_symbols", {("ETH", "USD"): ""}, "market_symbols"),
        ("market_symbols", {("ETH", "USD"): 1}, "market_symbols"),
    ],
)
def test_integration_rejects_invalid_provider_metadata(field: str, value: object, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        Integration(name="example", **{field: value})


@pytest.mark.parametrize(
    "field",
    [
        "gateway_price_source",
        "gateway_oracle_reader",
        "gateway_api_client",
        "gateway_portfolio_provider",
    ],
)
def test_integration_rejects_capabilities_owned_by_another_provider(field: str) -> None:
    foreign_ref = ImportRef("almanak.integrations.other.gateway.factory", "Factory")
    with pytest.raises(ValueError, match="must own its"):
        Integration(name="example", **{field: foreign_ref})


def test_integration_rejects_gateway_prefix_without_dot_boundary() -> None:
    deceptive_ref = ImportRef("almanak.integrations.example.gatewayish.factory", "Factory")
    with pytest.raises(ValueError, match="must own its"):
        Integration(name="example", gateway_price_source=deceptive_ref)


def test_chain_ids_are_chain_owned_while_provider_behavior_is_integration_owned() -> None:
    descriptor_source = (_REPO_ROOT / "almanak/core/chains/_descriptor.py").read_text()
    tree = ast.parse(descriptor_source)
    declared = {
        node.target.id
        for node in ast.walk(tree)
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name)
    }
    assert {"external_ids", "coingecko_id", "wrapped_coingecko_id"} <= declared
    assert "chainlink" not in declared

    integration_source = (_REPO_ROOT / "almanak/integrations/_base/descriptor.py").read_text()
    integration_tree = ast.parse(integration_source)
    integration_fields = {
        node.target.id
        for node in ast.walk(integration_tree)
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name)
    }
    assert "chain_ids" not in integration_fields
    assert ExternalChainIds().as_mapping() == {}


def _runtime_import_modules(tree: ast.AST) -> set[str]:
    """Collect imports while treating ``if TYPE_CHECKING`` blocks as non-runtime."""
    modules: set[str] = set()

    class Visitor(ast.NodeVisitor):
        def visit_If(self, node: ast.If) -> None:  # noqa: N802 - ast visitor API
            if isinstance(node.test, ast.Name) and node.test.id == "TYPE_CHECKING":
                for child in node.orelse:
                    self.visit(child)
                return
            self.generic_visit(node)

        def visit_ImportFrom(self, node: ast.ImportFrom) -> None:  # noqa: N802 - ast visitor API
            if node.module is not None:
                modules.add(node.module)

        def visit_Import(self, node: ast.Import) -> None:  # noqa: N802 - ast visitor API
            modules.update(alias.name for alias in node.names)

    Visitor().visit(tree)
    return modules


@pytest.mark.parametrize(
    "service",
    sorted(path.name for path in (_REPO_ROOT / "almanak/gateway/services").glob("*.py")),
)
def test_generic_gateway_services_have_no_concrete_provider_runtime_imports(service: str) -> None:
    tree = ast.parse((_REPO_ROOT / "almanak/gateway/services" / service).read_text())
    modules = _runtime_import_modules(tree)
    assert not any(
        module.startswith("almanak.integrations.")
        and not module.startswith("almanak.integrations._base.gateway")
        and ".gateway" in module
        for module in modules
    )


def test_discovery_failure_does_not_publish_partial_registry(monkeypatch: pytest.MonkeyPatch) -> None:
    from almanak.integrations._base.registry import IntegrationRegistry

    registry = IntegrationRegistry()
    real_import = importlib.import_module

    def failing_import(name: str):
        if name == "almanak.integrations.coingecko.integration":
            raise RuntimeError("broken manifest")
        return real_import(name)

    monkeypatch.setattr(importlib, "import_module", failing_import)
    with pytest.raises(RuntimeError, match="broken manifest"):
        registry.discover()
    assert not registry._discovered
    assert registry._integrations == {}


def test_missing_provider_manifest_fails_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    from almanak.integrations._base.registry import IntegrationRegistry

    registry = IntegrationRegistry()
    real_import = importlib.import_module
    monkeypatch.setattr(
        pkgutil,
        "iter_modules",
        lambda _path: [pkgutil.ModuleInfo(module_finder=None, name="missing", ispkg=True)],
    )

    def missing_import(name: str):
        if name == "almanak.integrations.missing.integration":
            raise ModuleNotFoundError(name)
        return real_import(name)

    monkeypatch.setattr(importlib, "import_module", missing_import)
    with pytest.raises(ModuleNotFoundError, match="missing"):
        registry.discover()
    assert registry._integrations == {}
    assert registry._discovered is False


def test_concurrent_discovery_imports_each_manifest_once(monkeypatch: pytest.MonkeyPatch) -> None:
    from almanak.integrations._base.registry import IntegrationRegistry

    registry = IntegrationRegistry()
    manifest = Integration(name="example")
    imports = 0
    real_import = importlib.import_module
    monkeypatch.setattr(
        pkgutil,
        "iter_modules",
        lambda _path: [pkgutil.ModuleInfo(module_finder=None, name="example", ispkg=True)],
    )

    def counted_import(name: str):
        nonlocal imports
        if name == "almanak.integrations.example.integration":
            imports += 1
            return SimpleNamespace(INTEGRATION=manifest)
        return real_import(name)

    monkeypatch.setattr(importlib, "import_module", counted_import)
    with ThreadPoolExecutor(max_workers=8) as executor:
        tuple(executor.map(lambda _index: registry.discover(), range(32)))
    assert imports == 1
    assert registry.all() == (manifest,)


def test_price_factory_name_must_match_manifest(monkeypatch: pytest.MonkeyPatch) -> None:
    from almanak.integrations._base import PriceSourceScope
    from almanak.integrations._base.registry import IntegrationRegistry

    class MismatchedFactory:
        name = "other"
        scope = PriceSourceScope.SHARED
        order = 1

        def supports(self, chain: str | None) -> bool:
            return True

        def build(self, *, chain: str | None, settings: object) -> None:
            return None

    registry = IntegrationRegistry()
    registry.register(
        Integration(
            name="example",
            gateway_price_source=ImportRef("almanak.integrations.example.gateway.factory", "Factory"),
        )
    )
    registry._discovered = True
    monkeypatch.setattr(ImportRef, "instantiate", lambda _self: MismatchedFactory())
    with pytest.raises(ValueError, match="mismatched price source"):
        registry.gateway_price_source_factories()


def test_price_factory_order_must_match_manifest(monkeypatch: pytest.MonkeyPatch) -> None:
    from almanak.integrations._base import PriceSourceScope
    from almanak.integrations._base.registry import IntegrationRegistry

    class Factory:
        name = "example"
        scope = PriceSourceScope.SHARED
        order = 20

        def supports(self, chain: str | None) -> bool:
            return True

        def build(self, *, chain: str | None, settings: object) -> None:
            return None

    registry = IntegrationRegistry()
    registry.register(
        Integration(
            name="example",
            gateway_price_source=ImportRef(
                "almanak.integrations.example.gateway.factory",
                "Factory",
                order=10,
            ),
        )
    )
    registry._discovered = True
    monkeypatch.setattr(ImportRef, "instantiate", lambda _self: Factory())

    with pytest.raises(ValueError, match="order mismatch"):
        registry.gateway_price_source_factories()
