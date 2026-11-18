"""Unsolicited effects preserve admitted work and verify a fixed canonical page."""

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from almanak import external_effects
from almanak.caip import ChainId
from almanak.connectors import composition
from almanak.connectors.identifiers import ConnectorId
from almanak.external_effects import ExternalEffect, ExternalIdentity, ExternalScan
from almanak.gateway.external_effects import verified_commit
from almanak.models import Chain, Model
from almanak.work import ObservationScope, outcome_chain


class Effect(ExternalEffect):
    amount: int


class Page(ExternalScan):
    start: int
    end: int
    canonical: str
    outcome: Effect | None = None


class State(Model):
    config: str = "fixture"
    initialized: bool = True
    mode: str = "running"
    memory: tuple[str, ...] = ()
    schedule: int = 100
    delivered: int = 0
    decision: str | None = "original-admitted-decision"
    plan: str | None = "immutable-signed-plan-evidence"
    pending: str | None = "unknown-broadcast-identity"
    requests: tuple[str, ...] = ("accepted-protocol-request",)
    owned: int = 50
    outcomes: tuple[Effect, ...] = ()
    external_scans: tuple[Page, ...] = ()


@pytest.fixture
def scope():
    return ObservationScope(
        chain=Chain(chain_id=ChainId("eip155:42161"), environment="fork"),
        account="fund",
        allocation="position",
    )


@pytest.fixture
def page(scope):
    identity = ExternalIdentity(connector=ConnectorId("fixture"), scope=scope, reference="event:1")
    return Page(
        connector=identity.connector,
        scope=scope,
        start=10,
        end=20,
        canonical="original-page",
        outcome=Effect(
            identity=identity, effective_at=datetime(2026, 9, 9, tzinfo=UTC), amount=-30
        ),
    )


def ownership(state, outcome=None):
    return {"owned": state.owned if outcome is None else state.owned + outcome.amount}


@pytest.fixture
def binding(monkeypatch, page):
    def reconcile(backend, state, *, reference=None):
        assert reference == page
        assert reference.end < backend.latest
        if reference.canonical != backend.canonical:
            raise ValueError("page is no longer canonical")
        return page

    value = SimpleNamespace(reconcile=reconcile)
    connector = SimpleNamespace(
        external_effects=value, lifecycle=SimpleNamespace(ownership=ownership)
    )
    monkeypatch.setattr(composition, "connector_for", lambda config: connector)
    return value


def backend():
    return SimpleNamespace(latest=100, canonical="original-page", audit_history=lambda state: None)


def test_exact_page_verification_preserves_every_admitted_identity_and_delivery(binding, page):
    before = State()
    after = external_effects.commit(before, before, page)
    assert after.owned == 20 and after.outcomes == (page.outcome,)
    protected = {"owned", "outcomes", "external_scans"}
    assert after.model_dump(exclude=protected) == before.model_dump(exclude=protected)
    verifier = backend()
    assert verified_commit(verifier, before, after) == after
    verifier.latest = 200
    assert verified_commit(verifier, before, after) == after
    verifier.canonical = "replacement-history"
    with pytest.raises(ValueError, match="no longer canonical"):
        verified_commit(verifier, before, after)


@pytest.mark.parametrize(
    "field,value",
    [
        ("owned", 999),
        ("pending", None),
        ("plan", None),
        ("decision", None),
        ("requests", ()),
        ("memory", ("premature-delivery",)),
        ("delivered", 1),
        ("schedule", 0),
        ("mode", "closed"),
    ],
)
def test_external_recording_rejects_unverified_changes_to_finances_or_admitted_work(
    binding, page, field, value
):
    before = State()
    after = external_effects.commit(before, before, page).model_copy(update={field: value})
    with pytest.raises(ValueError, match="changed pending work or differs"):
        verified_commit(backend(), before, after)


def test_repeated_effect_cannot_duplicate_ownership_or_reopen_delivery(binding, page):
    before = State()
    after = external_effects.commit(before, before, page)
    with pytest.raises(ValueError, match="initialized, delivered, new evidence"):
        external_effects.commit(after, after, page)
    delivered = after.model_copy(update={"delivered": 1})
    another_page = page.model_copy(update={"start": 21, "end": 30})
    with pytest.raises(ValueError, match="already been committed"):
        external_effects.commit(delivered, delivered, another_page)
    with pytest.raises(RuntimeError, match="state changed"):
        external_effects.commit(after, before, page)


def test_empty_page_updates_scan_evidence_without_financial_or_author_effects(binding, page):
    before = State()
    empty = page.model_copy(update={"outcome": None})
    after = external_effects.commit(before, before, empty)
    assert after.model_dump(exclude={"external_scans"}) == before.model_dump(
        exclude={"external_scans"}
    )
    assert after.external_scans == (empty,)


def test_event_scope_namespaces_connector_chain_and_fund_allocation(binding, page):
    identity = page.outcome.identity
    assert outcome_chain(page.outcome) == page.scope.chain
    assert identity != identity.model_copy(update={"connector": ConnectorId("other")})
    foreign = page.scope.model_copy(update={"account": "other-fund"})
    assert identity != identity.model_copy(update={"scope": foreign})
    with pytest.raises(ValueError, match="another connector or scope"):
        Page.model_validate(page.model_dump() | {"scope": foreign})


def test_scoped_scan_merge_preserves_other_chain_evidence(binding, page):
    first = page.model_copy(update={"outcome": None})
    foreign_scope = page.scope.model_copy(
        update={"chain": Chain(chain_id=ChainId("eip155:43114"), environment="fork")}
    )
    second = first.model_copy(update={"scope": foreign_scope})
    state = State(external_scans=(first, second))
    local = State(external_scans=(first,))
    assert external_effects.scoped_fields(state, page.scope.chain) == {"external_scans": (first,)}
    assert external_effects.scoped_fields(state, page.scope.chain, local) == {
        "external_scans": (first, second)
    }
    with pytest.raises(ValueError, match="erase history or affect another chain"):
        external_effects.scoped_fields(state, page.scope.chain, State(external_scans=(second,)))
