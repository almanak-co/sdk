"""Delegate protocol obligations while recording one shared financial transition."""

from uuid import uuid4

from almanak.connectors.composed.authority import selected
from almanak.connectors.composed.inventory import (
    initial_owned,
    ownership,
    project_cash,
    validate_observed,
)
from almanak.connectors.composition import connector_from_wire
from almanak.connectors.identifiers import ConnectorId
from almanak.connectors.target_routing import connector_for_target, target_connector
from almanak.triggers import DecisionSchedule
from almanak.work import initial_work


def local(items, identity):
    return tuple(item for item in items if target_connector(item.target) == identity)


def outcome_target(outcome):
    """The target an outcome settles: the one a transaction observed, or the one an
    external effect without a submitted transaction names itself."""
    after = getattr(outcome, "after", None)
    return outcome.target if after is None else after.target


def component_transfers(state, identity):
    return tuple(t for t in state.transfers if t.connector == identity)


def component_state(state, identity):
    """The checkpoint as one component sees it: its own configuration, records, scan
    pages, requests and transfers, and nothing of another connector's."""
    outcomes = tuple(o for o in state.outcomes if target_connector(outcome_target(o)) == identity)
    return state.model_copy(
        update={
            "config": state.config.component(identity),
            "capabilities": selected(state.capabilities, identity),
            "owned": local(state.owned, identity),
            "baseline": local(state.baseline, identity),
            "closure": local(state.closure, identity),
            "outcomes": outcomes,
            "requests": state.requests if identity == ConnectorId("gmx_v2") else (),
            "external_scans": tuple(p for p in state.external_scans if p.connector == identity),
            "transfers": component_transfers(state, identity),
            "delivered": sum(
                target_connector(outcome_target(o)) == identity
                for o in state.outcomes[: state.delivered]
            ),
        }
    )


def validate_components(state, observations, method):
    if tuple(o.target for o in observations) != tuple(o.target for o in state.owned):
        raise ValueError("observations must cover the composed ownership exactly once")
    validate_observed(state, observations)
    for identity in state.config.parameter_components:
        view = component_state(state, identity)
        # A chain a bridge only delivers to holds none of its records, yet its transfers
        # in flight are obligations there too: they hold that chain's closure.
        if not view.owned and not view.transfers:
            continue
        binding = connector_from_wire({"connector": str(identity)})
        validate_component(binding, view, local(observations, identity), method, identity)


def validate_component(binding, state, observations, method, identity):
    if method == "validate_closure" and identity == ConnectorId("uniswap_v4"):
        from almanak.connectors.uniswap_v4.lifecycle import (
            validate_protocol_closure as v4_closure,
        )

        v4_closure(state, observations)
        return
    if method == "validate_closure" and identity in (
        ConnectorId("uniswap_v3"),
        ConnectorId("pancakeswap_v3"),
        ConnectorId("aerodrome_slipstream"),
    ):
        from almanak.connectors.v3_pool_lifecycle import validate_protocol_closure

        # Cash belongs to the fund, including collateral returned by lending.
        # The exchange must close its NFT and authority, not liquidate that cash.
        validate_protocol_closure(state, observations)
        return
    if method == "validate_closure" and identity == ConnectorId("aerodrome"):
        from almanak.connectors.aerodrome.lifecycle import (
            validate_protocol_closure as aerodrome_closure,
        )

        aerodrome_closure(state, observations)
        return
    if method == "validate_closure" and identity == ConnectorId("curve"):
        from almanak.connectors.curve.lifecycle import validate_protocol_closure as curve_closure

        curve_closure(state, observations)
        return
    getattr(binding.lifecycle, method)(state, observations)


def validate_initial(state, observations):
    validate_components(state, observations, "validate_initial")


def validate_owned(state, observations):
    validate_components(state, observations, "validate_owned")


def validate_closure(state, observations):
    validate_components(state, observations, "validate_closure")


def commit_owned(owned, outcome):
    raise ValueError("composed ownership must be committed with its shared inventory")


def repayment_coverage(state, observed):
    """Lending coverage against the fund's wallet, which a full repay may spend; a
    partial repay stays within the retained inventory.

    The retained inventory alone would hide a top-up: a full repay also spends loan
    asset sent to the fund beyond it (`execution.validate_inventory`).
    """
    rows = []
    for identity in dict.fromkeys(target_connector(o.target) for o in observed):
        component = local(observed, identity)
        coverage = getattr(
            connector_for_target(component[0].target).lifecycle, "repayment_coverage", None
        )
        if coverage is not None:
            rows.extend(coverage(component_state(state, identity), component))
    return tuple(rows)


class ComposedLifecycle:
    repayment_coverage = staticmethod(repayment_coverage)
    ownership = staticmethod(ownership)
    commit_owned = staticmethod(commit_owned)
    validate_initial = staticmethod(validate_initial)
    validate_owned = staticmethod(validate_owned)
    validate_closure = staticmethod(validate_closure)

    @staticmethod
    def decision_owned(state):
        return project_cash(state.owned, state.inventory, state.transfers)

    @staticmethod
    def increases_exposure(intent):
        return connector_for_target(intent.target).lifecycle.increases_exposure(intent)

    @staticmethod
    def initial(config, strategy, capabilities, permissions):
        from almanak.connectors.composed.state import Checkpoint, Work

        return Checkpoint(
            schedule=DecisionSchedule(),
            work=initial_work(config, Work),
            deployment_id=uuid4().hex,
            config=config,
            strategy=strategy,
            capabilities=capabilities,
            permissions=permissions,
            mode="running",
            initialized=False,
            owned=initial_owned(config),
            inventory=config.inventory,
        )
