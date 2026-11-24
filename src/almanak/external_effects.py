"""Independently verified economic events have scope, never invented execution authority."""

from typing import Annotated

from pydantic import Field, model_validator

from almanak.connectors.identifiers import ConnectorId
from almanak.models import Model
from almanak.triggers import UtcInstant
from almanak.work import ObservationScope


class ExternalIdentity(Model):
    connector: ConnectorId
    scope: ObservationScope
    reference: Annotated[str, Field(min_length=1, max_length=512, pattern=r"^\S+$")]


class ExternalEffect(Model):
    identity: ExternalIdentity
    effective_at: UtcInstant


class ExternalScan(Model):
    connector: ConnectorId
    scope: ObservationScope
    outcome: ExternalEffect | None = None

    @model_validator(mode="after")
    def effect_scope(self):
        if self.outcome is not None and (
            self.outcome.identity.connector,
            self.outcome.identity.scope,
        ) != (self.connector, self.scope):
            raise ValueError(
                "external scan cannot attribute an effect to another connector or scope"
            )
        return self


def binding(state):
    from almanak.connectors.composition import connector_for

    return connector_for(state.config).external_effects


def changed(before, after):
    return binding(before) is not None and before.external_scans != after.external_scans


def commit(current, before, page):
    from almanak.connectors.composition import connector_for

    if current != before:
        raise RuntimeError("external evidence state changed during reconciliation")
    if (
        binding(current) is None
        or not current.initialized
        or current.mode == "closed"
        or current.delivered != len(current.outcomes)
        or page in current.external_scans
    ):
        raise ValueError("external recording requires initialized, delivered, new evidence")
    outcomes = current.outcomes
    if page.outcome is not None:
        if any(
            isinstance(outcome, ExternalEffect) and outcome.identity == page.outcome.identity
            for outcome in outcomes
        ):
            raise ValueError("external economic identity has already been committed")
        outcomes = (*outcomes, page.outcome)
    # Ownership sees the page it is recorded with: a page can change what is owned
    # without an outcome, as a scan that proves a bridge transfer unfilled does.
    staged = current.model_copy(update={"external_scans": (*current.external_scans, page)})
    ownership = connector_for(current.config).lifecycle.ownership(staged, page.outcome)
    return staged.model_copy(update={**ownership, "outcomes": outcomes})


def progress(runtime, state):
    if binding(state) is None or not state.initialized:
        return False
    page = runtime.gateway.external_reconcile()
    if page is None:
        return False
    runtime.fault("external_evidence", page)
    runtime.journal.update(lambda current: commit(current, state, page))
    runtime.fault("external_commit", page)
    return True


def scoped_fields(state, chain, changed_state=None):
    if binding(state) is None:
        return {}
    if changed_state is None:
        return {"external_scans": tuple(p for p in state.external_scans if p.scope.chain == chain)}
    before = tuple(p for p in state.external_scans if p.scope.chain == chain)
    after = changed_state.external_scans
    if after[: len(before)] != before or any(p.scope.chain != chain for p in after):
        raise ValueError("external scan merge cannot erase history or affect another chain")
    return {"external_scans": (*state.external_scans, *after[len(before) :])}
