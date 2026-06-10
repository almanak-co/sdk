"""Unit tests for the VIB-4989 prediction/CLOB/stub ``_strategy_base`` seams.

PR A commit 1 landed the seam *infrastructure* with empty dispatch (no
connector opted in yet — Polymarket wired in later commits; dispatch is now
derived from connector manifests). These tests pin the
registry machinery in isolation: empty-registry fail-closed behaviour, keyed
dispatch + chains discovery (via a monkeypatched loader), broken-sibling
isolation, the gateway-stub ``service_name`` collision guard, spec validation, and
``reset_cache``. They are connector-agnostic — no Polymarket import.
"""

from __future__ import annotations

import pytest

from almanak.connectors._strategy_base.gateway_stub_base import GatewayStubSpec
from almanak.connectors._strategy_base.gateway_stub_registry import GatewayStubRegistry
from almanak.connectors._strategy_base.prediction_execute_base import PredictionExecuteSpec
from almanak.connectors._strategy_base.prediction_execute_registry import PredictionExecuteRegistry
from almanak.connectors._strategy_base.prediction_read_base import PredictionProvider, PredictionReadSpec
from almanak.connectors._strategy_base.prediction_read_registry import PredictionReadRegistry


@pytest.fixture(autouse=True)
def _reset_registries(monkeypatch):
    # Clear real connector registrations so these are pure *machinery* tests,
    # independent of which connectors opt in via manifest declarations (e.g.
    # polymarket). Pre-seeding ``_spec_loader_map`` with a fresh empty dict
    # bypasses manifest derivation; each test that needs a loader sets its own.
    for reg in (PredictionReadRegistry, PredictionExecuteRegistry, GatewayStubRegistry):
        reg.reset_cache()
        monkeypatch.setattr(reg, "_spec_loader_map", {})
    yield
    for reg in (PredictionReadRegistry, PredictionExecuteRegistry, GatewayStubRegistry):
        reg.reset_cache()


# ──────────────────────────────────────────────────────────────────────────────
# Empty-registry baseline (commit 1 ships empty loaders — everything fails closed)
# ──────────────────────────────────────────────────────────────────────────────


def test_read_registry_empty_baseline():
    assert PredictionReadRegistry.supported_protocols() == ()
    assert PredictionReadRegistry.has("polymarket") is False
    assert PredictionReadRegistry.canonical("polymarket") is None
    assert PredictionReadRegistry.supports_chain("polymarket", "polygon") is False
    assert PredictionReadRegistry.build_provider("polymarket", gateway_client=object(), wallet="0x") is None


def test_execute_registry_empty_baseline():
    assert PredictionExecuteRegistry.supported_protocols() == ()
    assert PredictionExecuteRegistry.has("polymarket") is False
    assert PredictionExecuteRegistry.protocols_for_chain("polygon") == ()
    assert PredictionExecuteRegistry.supports_chain("polymarket", "polygon") is False
    assert PredictionExecuteRegistry.build_handler("polymarket", gateway_client=object()) is None


def test_stub_registry_empty_baseline():
    assert GatewayStubRegistry.stub_names() == ()
    assert GatewayStubRegistry.build_stubs(object()) == {}


# ──────────────────────────────────────────────────────────────────────────────
# Totality: None / non-str protocol fails closed, never raises
# ──────────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("reg", [PredictionReadRegistry, PredictionExecuteRegistry])
def test_normalize_is_total(reg):
    assert reg.has(None) is False  # type: ignore[arg-type]
    assert reg.canonical(None) is None
    assert reg.canonical(123) is None  # type: ignore[arg-type]
    assert reg.supports_chain(None, None) is False  # type: ignore[arg-type]


# ──────────────────────────────────────────────────────────────────────────────
# Keyed dispatch + chains discovery (monkeypatched loader → cached spec)
# ──────────────────────────────────────────────────────────────────────────────


def test_read_registry_keyed_dispatch(monkeypatch):
    sentinel = object()
    spec = PredictionReadSpec(
        build_provider=lambda *, gateway_client, wallet: sentinel,
        chains=frozenset({"polygon"}),
    )
    # Canonical key carries an underscore; inputs with a dash / mixed case normalize onto it.
    monkeypatch.setitem(PredictionReadRegistry._spec_loader_map, "test_proto", ("unused.mod", "SPEC"))
    monkeypatch.setitem(PredictionReadRegistry._spec_cache, "test_proto", spec)

    assert PredictionReadRegistry.has("Test-Proto") is True  # normalized: case + '-'→'_'
    assert PredictionReadRegistry.supported_protocols() == ("test_proto",)
    assert PredictionReadRegistry.canonical("test-proto") == "test_proto"  # '-'→'_'
    assert PredictionReadRegistry.supports_chain("test_proto", "Polygon") is True  # case-insensitive
    assert PredictionReadRegistry.supports_chain("test_proto", "ethereum") is False
    assert PredictionReadRegistry.build_provider("Test-Proto", gateway_client=object(), wallet="0x") is sentinel


def test_execute_registry_keyed_dispatch_and_protocols_for_chain(monkeypatch):
    handler = object()
    spec = PredictionExecuteSpec(
        build_handler=lambda *, gateway_client, wallet: handler,
        chains=frozenset({"polygon"}),
    )
    monkeypatch.setitem(PredictionExecuteRegistry._spec_loader_map, "testproto", ("unused.mod", "SPEC"))
    monkeypatch.setitem(PredictionExecuteRegistry._spec_cache, "testproto", spec)

    assert PredictionExecuteRegistry.build_handler("testproto", gateway_client=object()) is handler
    assert PredictionExecuteRegistry.protocols_for_chain("polygon") == ("testproto",)
    assert PredictionExecuteRegistry.protocols_for_chain("ethereum") == ()


def test_stub_registry_build_and_names(monkeypatch):
    spec = GatewayStubSpec(service_name="poly", stub_factory=lambda ch: f"stub:{ch}")
    monkeypatch.setitem(GatewayStubRegistry._spec_loader_map, "polyconn", ("unused.mod", "SPEC"))
    monkeypatch.setitem(GatewayStubRegistry._spec_cache, "polyconn", spec)

    assert GatewayStubRegistry.stub_names() == ("poly",)
    assert GatewayStubRegistry.build_stubs("CHAN") == {"poly": "stub:CHAN"}


# ──────────────────────────────────────────────────────────────────────────────
# Broken-sibling isolation (a bad loader is skipped / fails closed, never raises)
# ──────────────────────────────────────────────────────────────────────────────


def test_read_registry_broken_sibling_fails_closed(monkeypatch):
    good = object()
    good_spec = PredictionReadSpec(build_provider=lambda *, gateway_client, wallet: good, chains=frozenset({"polygon"}))
    monkeypatch.setitem(PredictionReadRegistry._spec_loader_map, "good", ("unused.mod", "SPEC"))
    monkeypatch.setitem(PredictionReadRegistry._spec_cache, "good", good_spec)
    # 'broken' has NO cache entry → _load_spec attempts a real import that fails.
    monkeypatch.setitem(PredictionReadRegistry._spec_loader_map, "broken", ("almanak.does.not.exist", "SPEC"))

    assert PredictionReadRegistry.build_provider("broken", gateway_client=object()) is None  # fail closed, no raise
    assert PredictionReadRegistry.supports_chain("broken", "polygon") is False
    # The healthy sibling is unaffected.
    assert PredictionReadRegistry.build_provider("good", gateway_client=object()) is good


def test_execute_registry_broken_sibling_omitted_from_chain_scan(monkeypatch):
    good_spec = PredictionExecuteSpec(
        build_handler=lambda *, gateway_client, wallet: object(), chains=frozenset({"polygon"})
    )
    monkeypatch.setitem(PredictionExecuteRegistry._spec_loader_map, "good", ("unused.mod", "SPEC"))
    monkeypatch.setitem(PredictionExecuteRegistry._spec_cache, "good", good_spec)
    monkeypatch.setitem(PredictionExecuteRegistry._spec_loader_map, "broken", ("almanak.does.not.exist", "SPEC"))

    # Broken connector is simply absent from the chain scan; the good one shows.
    assert PredictionExecuteRegistry.protocols_for_chain("polygon") == ("good",)
    assert PredictionExecuteRegistry.build_handler("broken", gateway_client=object()) is None


def test_stub_registry_broken_sibling_skipped(monkeypatch):
    good_spec = GatewayStubSpec(service_name="good", stub_factory=lambda ch: "ok")
    monkeypatch.setitem(GatewayStubRegistry._spec_loader_map, "goodconn", ("unused.mod", "SPEC"))
    monkeypatch.setitem(GatewayStubRegistry._spec_cache, "goodconn", good_spec)
    monkeypatch.setitem(GatewayStubRegistry._spec_loader_map, "brokenconn", ("almanak.does.not.exist", "SPEC"))

    assert GatewayStubRegistry.build_stubs("CH") == {"good": "ok"}  # broken skipped, not raised


def test_stub_registry_raising_factory_isolated_at_runtime(monkeypatch):
    # ``_iter_specs`` only isolates import/load failures. A spec that loads fine but
    # whose ``stub_factory`` raises *when invoked* must also be isolated, or one
    # connector's runtime fault crashes ``GatewayClient.connect()`` for everyone.
    def _boom(_channel):
        raise RuntimeError("factory blew up at connect time")

    bad_spec = GatewayStubSpec(service_name="bad", stub_factory=_boom)
    good_spec = GatewayStubSpec(service_name="good", stub_factory=lambda ch: "ok")
    # 'badconn' loads before 'goodconn'; both have valid cached specs.
    monkeypatch.setitem(GatewayStubRegistry._spec_loader_map, "badconn", ("unused.mod", "SPEC"))
    monkeypatch.setitem(GatewayStubRegistry._spec_cache, "badconn", bad_spec)
    monkeypatch.setitem(GatewayStubRegistry._spec_loader_map, "goodconn", ("unused.mod", "SPEC"))
    monkeypatch.setitem(GatewayStubRegistry._spec_cache, "goodconn", good_spec)

    stubs = GatewayStubRegistry.build_stubs("CH")  # does not raise
    assert stubs == {"good": "ok"}  # raising factory's stub absent; healthy one present


# ──────────────────────────────────────────────────────────────────────────────
# GatewayStub service_name collision is a hard error (not swallowed)
# ──────────────────────────────────────────────────────────────────────────────


def test_stub_registry_service_name_collision_raises(monkeypatch):
    spec_a = GatewayStubSpec(service_name="dup", stub_factory=lambda ch: "A")
    spec_b = GatewayStubSpec(service_name="dup", stub_factory=lambda ch: "B")
    monkeypatch.setitem(GatewayStubRegistry._spec_loader_map, "connA", ("unused.mod", "SPEC"))
    monkeypatch.setitem(GatewayStubRegistry._spec_cache, "connA", spec_a)
    monkeypatch.setitem(GatewayStubRegistry._spec_loader_map, "connB", ("unused.mod", "SPEC"))
    monkeypatch.setitem(GatewayStubRegistry._spec_cache, "connB", spec_b)

    with pytest.raises(ValueError, match="service_name 'dup' claimed by both"):
        GatewayStubRegistry.build_stubs(object())


# ──────────────────────────────────────────────────────────────────────────────
# Spec validation (the bare-str / empty / non-callable guards)
# ──────────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("spec_cls", [PredictionReadSpec, PredictionExecuteSpec])
def test_spec_rejects_bare_str_chains(spec_cls):
    fn = lambda *, gateway_client, wallet: None
    field = "build_provider" if spec_cls is PredictionReadSpec else "build_handler"
    with pytest.raises(TypeError, match="bare str"):
        spec_cls(**{field: fn}, chains="polygon")  # type: ignore[arg-type]


@pytest.mark.parametrize("spec_cls", [PredictionReadSpec, PredictionExecuteSpec])
def test_spec_coerces_iterable_chains(spec_cls):
    fn = lambda *, gateway_client, wallet: None
    field = "build_provider" if spec_cls is PredictionReadSpec else "build_handler"
    spec = spec_cls(**{field: fn}, chains=["polygon", "polygon"])  # list → frozenset, dedup
    assert spec.chains == frozenset({"polygon"})


@pytest.mark.parametrize("spec_cls", [PredictionReadSpec, PredictionExecuteSpec])
def test_spec_rejects_non_callable_factory(spec_cls):
    # A non-callable factory is caught at construction (registration/import), not
    # late when the runner first builds the provider/handler — mirrors
    # ``GatewayStubSpec``'s ``stub_factory`` callable check. The error names the field.
    field = "build_provider" if spec_cls is PredictionReadSpec else "build_handler"
    with pytest.raises(TypeError, match=field):
        spec_cls(**{field: "not-callable"}, chains=frozenset({"polygon"}))  # type: ignore[arg-type]


def test_gateway_stub_spec_validation():
    with pytest.raises(TypeError, match="service_name"):
        GatewayStubSpec(service_name="", stub_factory=lambda ch: ch)
    with pytest.raises(TypeError, match="stub_factory"):
        GatewayStubSpec(service_name="x", stub_factory="not-callable")  # type: ignore[arg-type]


# ──────────────────────────────────────────────────────────────────────────────
# PredictionProvider Protocol is runtime-checkable against a structural match
# ──────────────────────────────────────────────────────────────────────────────


def test_prediction_provider_protocol_structural():
    class FakeProvider:
        def get_market(self, market_id_or_slug): ...
        def get_market_by_token_id(self, token_id): ...
        def get_price(self, market_id_or_slug, outcome): ...
        def get_positions(self, *a, **k): ...
        def get_open_orders(self, *a, **k): ...
        def get_price_history(self, *a, **k): ...
        def clear_cache(self): ...

    class NotAProvider:
        def get_market(self, m): ...

    assert isinstance(FakeProvider(), PredictionProvider)
    assert not isinstance(NotAProvider(), PredictionProvider)


# ──────────────────────────────────────────────────────────────────────────────
# reset_cache drops the resolved-spec cache
# ──────────────────────────────────────────────────────────────────────────────


def test_reset_cache_clears_resolved_specs(monkeypatch):
    spec = PredictionReadSpec(build_provider=lambda *, gateway_client, wallet: None, chains=frozenset({"polygon"}))
    monkeypatch.setitem(PredictionReadRegistry._spec_cache, "x", spec)
    assert PredictionReadRegistry._spec_cache.get("x") is spec
    PredictionReadRegistry.reset_cache()
    assert PredictionReadRegistry._spec_cache.get("x") is None
