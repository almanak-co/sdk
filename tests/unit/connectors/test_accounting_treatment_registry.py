"""Contract tests for the connector accounting-treatment seam (VIB-4931 PR-A commit 1).

These tests pin the accounting-treatment seam's contract: base-type validation,
descriptor-backed real Pendle registration, broken-connector isolation,
``treatment_key`` collision, first-claim-wins ordering, and cache reset. Synthetic
specs still replace the private loader table for narrow isolation tests; production
registration comes from connector manifests through
``almanak.connectors._strategy_accounting_treatment_registry``.
"""

from __future__ import annotations

import sys
import types

import pytest

from almanak.connectors._strategy_accounting_treatment_registry import (
    AccountingTreatmentRegistry,
)
from almanak.connectors._strategy_base.accounting_treatment_base import (
    AccountingCategoryDecision,
    AccountingTreatmentSpec,
)

# ``.category`` is opaque to the registry (it never inspects it), so a plain
# string stands in for an ``AccountingCategory`` member — these tests need no
# framework import.
_SENTINEL_CATEGORY = "LP"


@pytest.fixture(autouse=True)
def _reset_registry():
    """Each test starts and ends with a clean registry (no cross-test leakage)."""
    AccountingTreatmentRegistry.reset_cache()
    yield
    AccountingTreatmentRegistry.reset_cache()


def _decision(treatment_key: str) -> AccountingCategoryDecision:
    return AccountingCategoryDecision(category=_SENTINEL_CATEGORY, treatment_key=treatment_key)  # type: ignore[arg-type]


def _spec(*, claims, treatments=None, categorize=None, position_key=None) -> AccountingTreatmentSpec:
    return AccountingTreatmentSpec(
        categorize=categorize or (lambda _it, _proto, _tok, _tin="": None),
        treatments=treatments or {},
        claims_event_types=frozenset(claims),
        position_key=position_key,
    )


def _install(monkeypatch, loaders: dict[str, AccountingTreatmentSpec | None]) -> None:
    """Point the registry at synthetic in-``sys.modules`` spec modules.

    Insertion order of ``loaders`` is preserved as registration order. A ``None``
    spec models a broken connector: the module exists but its
    ``ACCOUNTING_TREATMENT_SPEC`` attribute is the wrong type.
    """
    spec_loaders: dict[str, tuple[str, str]] = {}
    for connector, spec in loaders.items():
        module_path = f"almanak._test_accounting_spec_{connector}"
        module = types.ModuleType(module_path)
        module.ACCOUNTING_TREATMENT_SPEC = spec if spec is not None else object()
        monkeypatch.setitem(sys.modules, module_path, module)
        spec_loaders[connector] = (module_path, "ACCOUNTING_TREATMENT_SPEC")
    monkeypatch.setattr(AccountingTreatmentRegistry, "_SPEC_LOADERS", spec_loaders)
    AccountingTreatmentRegistry.reset_cache()


# --- base-type validation ---------------------------------------------------


def test_decision_rejects_empty_treatment_key():
    with pytest.raises(TypeError):
        AccountingCategoryDecision(category=_SENTINEL_CATEGORY, treatment_key="")  # type: ignore[arg-type]


def test_spec_rejects_bare_string_claims():
    # A bare str would be iterated character-by-character (the PrimitiveDeclaration footgun).
    with pytest.raises(TypeError):
        AccountingTreatmentSpec(
            categorize=lambda *_: None,
            treatments={},
            claims_event_types="LP_OPEN",  # type: ignore[arg-type]
        )


def test_spec_coerces_claims_to_frozenset():
    spec = AccountingTreatmentSpec(
        categorize=lambda *_: None,
        treatments={},
        claims_event_types=["LP_OPEN", "LP_CLOSE"],  # type: ignore[arg-type]
    )
    assert spec.claims_event_types == frozenset({"LP_OPEN", "LP_CLOSE"})


# --- real registry state (Pendle wired in PR-A commit 2) --------------------


def test_pendle_connector_is_registered():
    # Commit 2 wires the Pendle connector as the first (and only) opt-in. The
    # Pendle spec's own behaviour is pinned in tests/unit/connectors/pendle/.
    assert AccountingTreatmentRegistry.supported_connectors() == ("pendle",)


def test_position_key_for_routes_pendle():
    # Commit 4: Pendle publishes a position_key. An intent with a market pool yields
    # the (pendle_lp key, market id) pair; a non-Pendle protocol falls through (None).
    intent = types.SimpleNamespace(pool="0xMarket")
    assert AccountingTreatmentRegistry.position_key_for(
        "pendle_v2", intent_type="LP_OPEN", chain="ethereum", wallet="0xWallet", intent=intent
    ) == ("pendle_lp:ethereum:0xwallet:0xmarket", "0xmarket")
    assert (
        AccountingTreatmentRegistry.position_key_for(
            "uniswap_v3", intent_type="LP_OPEN", chain="ethereum", wallet="0xWallet", intent=intent
        )
        is None
    )


# --- isolation / collision / ordering ---------------------------------------


def test_categorize_first_claiming_connector_wins(monkeypatch):
    a = _spec(claims={"SWAP"}, categorize=lambda _it, p, _t, _tin="": _decision("a") if p == "alpha" else None)
    b = _spec(claims={"SWAP"}, categorize=lambda _it, p, _t, _tin="": _decision("b") if p == "alpha" else None)
    _install(monkeypatch, {"a_conn": a, "b_conn": b})  # insertion order: a_conn first
    decision = AccountingTreatmentRegistry.categorize("SWAP", "alpha", "")
    assert decision is not None and decision.treatment_key == "a"


def test_categorize_calls_strict_3arg_connector(monkeypatch):
    """A connector publishing a strict 3-arg ``categorize`` (no ``token_in``) is
    still called and can claim — the CategorizeFn additive-arg contract. Passing
    ``token_in`` positionally to such a function would TypeError → isolate it →
    silently drop its events to the generic path (a money-path loss)."""
    # Exactly 3 params, no *args, no token_in — the legacy signature.
    legacy = _spec(claims={"SWAP"}, categorize=lambda _it, p, _t: _decision("legacy") if p == "alpha" else None)
    _install(monkeypatch, {"legacy_conn": legacy})
    decision = AccountingTreatmentRegistry.categorize("SWAP", "alpha", "TOK", "PT-x")
    assert decision is not None and decision.treatment_key == "legacy"


def test_categorize_passes_token_in_to_4arg_connector(monkeypatch):
    """A 4-arg connector receives ``token_in`` so it can claim a directional leg
    (e.g. a PT- ``token_in`` sell) without over-claiming every swap."""
    directional = _spec(
        claims={"SWAP"},
        categorize=lambda _it, _p, _t, tin="": _decision("pt_sell") if tin.startswith("PT-") else None,
    )
    _install(monkeypatch, {"dir_conn": directional})
    assert AccountingTreatmentRegistry.categorize("SWAP", "pendle", "WSTETH", "PT-x") is not None
    assert AccountingTreatmentRegistry.categorize("SWAP", "pendle", "PT-x", "WSTETH") is None


def test_broken_connector_is_skipped_not_fatal(monkeypatch):
    good = _spec(
        claims={"LP_OPEN"},
        categorize=lambda _it, p, _t, _tin="": _decision("good") if p == "good_proto" else None,
    )
    _install(monkeypatch, {"broken": None, "good": good})  # broken's attr is the wrong type
    decision = AccountingTreatmentRegistry.categorize("LP_OPEN", "good_proto", "")
    assert decision is not None and decision.treatment_key == "good"


def test_categorize_isolates_a_raising_connector(monkeypatch):
    def _boom(*_):
        raise RuntimeError("boom")

    raising = _spec(claims={"LP_OPEN"}, categorize=_boom)
    good = _spec(claims={"LP_OPEN"}, categorize=lambda _it, _p, _t, _tin="": _decision("good"))
    _install(monkeypatch, {"a_raising": raising, "z_good": good})  # raising iterated first
    decision = AccountingTreatmentRegistry.categorize("LP_OPEN", "anything", "")
    assert decision is not None and decision.treatment_key == "good"


def test_treatment_for_resolves_published_treatment(monkeypatch):
    def _fn(ctx):
        return ctx

    _install(monkeypatch, {"pendle": _spec(claims={"LP_OPEN"}, treatments={"pendle_lp": _fn})})
    assert AccountingTreatmentRegistry.treatment_for("pendle_lp") is _fn


def test_treatment_key_collision_is_fatal(monkeypatch):
    a = _spec(claims={"LP_OPEN"}, treatments={"dup": lambda c: c})
    b = _spec(claims={"LP_OPEN"}, treatments={"dup": lambda c: c})
    _install(monkeypatch, {"a_conn": a, "b_conn": b})
    with pytest.raises(ValueError, match="dup"):
        AccountingTreatmentRegistry.treatment_for("dup")


def test_position_key_for_only_the_owning_connector_resolves(monkeypatch):
    spec = _spec(
        claims={"LP_OPEN"},
        position_key=lambda **kw: (f"pendle_lp:{kw['protocol']}", "mkt") if kw["protocol"] == "pendle_v2" else None,
    )
    _install(monkeypatch, {"pendle": spec})
    key = AccountingTreatmentRegistry.position_key_for(
        "pendle_v2", intent_type="LP_OPEN", chain="ethereum", wallet="0x1", intent=object()
    )
    assert key == ("pendle_lp:pendle_v2", "mkt")
    # A protocol the connector does not own -> None (falls back to the generic key).
    assert (
        AccountingTreatmentRegistry.position_key_for(
            "uniswap_v3", intent_type="LP_OPEN", chain="ethereum", wallet="0x1", intent=object()
        )
        is None
    )


def test_reset_cache_rebuilds_after_loader_swap(monkeypatch):
    _install(monkeypatch, {"first": _spec(claims={"LP_OPEN"}, treatments={"one": lambda c: c})})
    assert AccountingTreatmentRegistry.treatment_for("one") is not None
    _install(monkeypatch, {"second": _spec(claims={"LP_OPEN"}, treatments={"two": lambda c: c})})  # resets cache
    assert AccountingTreatmentRegistry.treatment_for("one") is None
    assert AccountingTreatmentRegistry.treatment_for("two") is not None
