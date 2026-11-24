"""One fund ledger and payer total accompany connector-owned position evidence."""

from almanak.connectors.aave_v3.reporting import position_report as lending_position
from almanak.connectors.aerodrome.reporting import position_report as aerodrome_position
from almanak.connectors.aerodrome_slipstream.reporting import (
    position_report as slipstream_position,
)
from almanak.connectors.composed.aave_v4_reporting import position_report as aave_v4_position
from almanak.connectors.composed.lifecycle import component_state
from almanak.connectors.composed.v4_reporting import position_only
from almanak.connectors.compound_v3.reporting import position_report as compound_position
from almanak.connectors.curve.reporting import position_report as curve_position
from almanak.connectors.gmx_v2.state import LiquidationOutcome, RequestOutcome
from almanak.connectors.identifiers import ConnectorId
from almanak.connectors.morpho_blue.reporting import position_report as morpho_position
from almanak.connectors.pancakeswap_v3.reporting import position_report as pancake_position
from almanak.connectors.sparklend.reporting import position_report as spark_position
from almanak.connectors.target_routing import target_connector
from almanak.connectors.uniswap_v3.reporting import (
    accounting_evidence,
    execution_evidence,
    pending_evidence,
)
from almanak.connectors.uniswap_v3.reporting import (
    payer_reports as receipt_payer_reports,
)
from almanak.connectors.uniswap_v3.reporting import position_report as exchange_position
from almanak.connectors.uniswap_v4.reporting import allocation_report as v4_position
from almanak.external_effects import ExternalEffect


def position_report(state, owned):
    identity = target_connector(owned.target)
    view = component_state(state, identity)
    if identity == ConnectorId("gmx_v2"):
        return {
            **owned.model_dump(mode="json"),
            "inventory_scope": (
                "View of shared fund cash; do not sum these amounts across positions."
            ),
        }
    if identity == ConnectorId("across"):
        # A bridge holds no position. Its value in flight is listed under `transfers`.
        return {
            **owned.model_dump(mode="json"),
            "position_scope": "A bridge holds nothing in the venue; see transfers.",
        }
    project = {
        ConnectorId("aave_v3"): lending_position,
        ConnectorId("aave_v4"): aave_v4_position,
        ConnectorId("sparklend"): spark_position,
        ConnectorId("uniswap_v3"): exchange_position,
        ConnectorId("pancakeswap_v3"): pancake_position,
        ConnectorId("aerodrome"): aerodrome_position,
        ConnectorId("aerodrome_slipstream"): slipstream_position,
        ConnectorId("uniswap_v4"): v4_position,
        ConnectorId("morpho_blue"): morpho_position,
        ConnectorId("compound_v3"): compound_position,
        ConnectorId("curve"): curve_position,
    }[identity]
    result = project(view, owned)
    if identity == ConnectorId("uniswap_v4"):
        result = position_only(result)
    if identity in (
        ConnectorId("uniswap_v3"),
        ConnectorId("pancakeswap_v3"),
        ConnectorId("aerodrome"),
        ConnectorId("aerodrome_slipstream"),
        ConnectorId("uniswap_v4"),
        ConnectorId("curve"),
    ):
        result["inventory_scope"] = (
            "View of shared fund cash; do not sum these currency amounts across positions."
        )
    return result


def submitted_state(state):
    return state.model_copy(
        update={
            "outcomes": tuple(
                outcome
                for outcome in state.outcomes
                if not isinstance(outcome, (RequestOutcome, LiquidationOutcome, ExternalEffect))
            )
        }
    )


def transfer_evidence(state):
    """Value in flight between the deployment's chains, listed apart from `inventory`:
    an open transfer is in no wallet, and it has no received amount until one is
    measured. Absent for a deployment without a bridge."""
    identity = ConnectorId("across")
    if identity not in state.config.parameter_components:
        return {}
    from almanak.connectors.across.connector import AcrossProjection

    bridge = AcrossProjection.report(component_state(state, identity))
    return {
        "transfers": bridge["transfers"],
        "unattributed_refund_payments": bridge["unattributed_refund_payments"],
        "transfer_scope": (
            "Open transfers are owned value in flight. They are in no inventory balance "
            "and must not be added to one; received and returned amounts are absent "
            "until measured."
        ),
    }


def payer_reports(state):
    return receipt_payer_reports(submitted_state(state))


def report(state):
    return {
        "schema_version": state.schema_version,
        "connectors": [str(identity) for identity in state.config.parameter_components],
        "deployment_id": state.deployment_id,
        "strategy": {"filename": state.strategy.filename, "sha256": state.strategy.sha256},
        "mode": state.mode,
        **execution_evidence(state),
        **accounting_evidence(state),
        **pending_evidence(state),
        "memory": state.memory.model_dump(mode="json"),
        "inventory": [
            {
                **balance.model_dump(mode="json"),
                "amount": str(balance.amount),
                "unit": "base_units",
                "evidence": "retained" if state.initialized else "configured",
            }
            for balance in state.inventory
        ],
        # Loan asset sent to the fund that full repays absorbed; listed only once one did.
        **(
            {
                "repayment_top_ups": [
                    {
                        **balance.model_dump(mode="json"),
                        "amount": str(balance.amount),
                        "unit": "base_units",
                        "evidence": "receipt",
                    }
                    for balance in state.top_ups
                ]
            }
            if state.top_ups
            else {}
        ),
        "positions": [position_report(state, owned) for owned in state.owned],
        "funds": [
            {"account": b.name, "fund": b.profile.fund.model_dump(mode="json")}
            for b in state.config.accounts.bindings
        ],
        "gas_payers": payer_reports(state),
        "requests": [request.model_dump(mode="json") for request in state.requests],
        "unresolved_requests": sum(request.unresolved for request in state.requests),
        "external_scans": [page.model_dump(mode="json") for page in state.external_scans],
        **transfer_evidence(state),
        "closure": [o.model_dump(mode="json") for o in state.closure],
        "valuation": "Asset quantities only; no common-currency valuation or gas conversion.",
    }
