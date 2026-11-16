"""Read-only HyperCore reports separate the Safe's custody, its venue attempts and gas.

A custody transaction carries a hash and the gas its submitter paid; a venue action
carries neither, only its attempt and the answer or evidence that resolved it. Values
stay in explicit base units or the venue's decimal text: nothing is marked to a price.
"""

from almanak.connectors.dex_reporting import execution_evidence
from almanak.connectors.hyperliquid.models import Parameters
from almanak.connectors.hyperliquid.state import TransactionOutcome, VenueOutcome
from almanak.gateway.evm import native_asset


def payer_reports(state):
    result = []
    for payer in dict.fromkeys(b.profile.submitter for b in state.config.accounts.bindings):
        transactions = [
            o
            for o in state.outcomes
            if isinstance(o, TransactionOutcome) and o.execution.submitter == payer
        ]
        result.append(
            {
                "submitter": payer.model_dump(mode="json"),
                "gas_paid": {
                    "asset": str(native_asset(payer.chain.chain_id)),
                    "unit": "base_units",
                    "amount": str(sum(o.gas_paid.amount for o in transactions))
                    if state.initialized
                    else None,
                },
                "nonces": [o.nonce for o in transactions],
            }
        )
    return result


def source(outcome) -> dict:
    """Where an outcome's evidence came from: a transaction, a venue attempt or effects."""
    if isinstance(outcome, TransactionOutcome):
        return {"source": "submitter", "hash": outcome.tx_hash}
    if isinstance(outcome, VenueOutcome):
        return {"source": "venue_action", "attempt": outcome.attempt.id}
    return {"source": "hypercore_evidence"}


def report(state):
    issues = [
        {**source(o), "issues": list(o.accounting_issues)}
        for o in state.outcomes
        if o.accounting_issues
    ]
    return {
        "schema_version": state.schema_version,
        "connector": "hyperliquid",
        "deployment_id": state.deployment_id,
        "mode": state.mode,
        "strategy": {"filename": state.strategy.filename, "sha256": state.strategy.sha256},
        **execution_evidence(state),
        "transaction_accounting_complete": not issues,
        "accounting_issues": issues,
        "undelivered_outcomes": len(state.outcomes) - state.delivered,
        "pending": None if state.pending is None else state.pending.kind,
        "reserved_attempt": state.plan.attempt_id if state.plan else None,
        "memory": state.memory.model_dump(mode="json"),
        "positions": [o.model_dump(mode="json") for o in state.owned],
        "custody": state.custody.model_dump(mode="json"),
        "trading": state.trading.model_dump(mode="json"),
        "book": state.book.model_dump(mode="json"),
        "gas_payers": payer_reports(state),
        "outcomes": [
            {**source(o), "result": o.strategy_result().model_dump(mode="json")}
            for o in state.outcomes
        ],
        "closure": [o.model_dump(mode="json") for o in state.closure],
        "valuation": (
            "Explicit USDC base units on HyperEVM; HyperCore positions in coin units and "
            "balances in the venue's decimal USDC; no marked equity or gas conversion."
        ),
    }


LABELS = {
    "hyperliquid_approve": "Approve USDC for CoreDepositWallet",
    "hyperliquid_deposit": "Deposit USDC to HyperCore",
    "hyperliquid_standard_mode": "Set HyperCore standard mode",
    "hyperliquid_class_transfer": "Move USDC between spot and perp",
    "hyperliquid_withdrawal": "Withdraw USDC to the Safe",
    "hyperliquid_agent_approval": "Approve a HyperCore agent",
}


def label(state, outcome):
    operation = outcome.operation
    if operation.kind == "hyperliquid_approve" and not operation.amount.amount:
        return "Revoke USDC approval"
    return LABELS[operation.kind]


def project(state, *, captured_at=None):
    """The owned USDC inventory and the custody transactions; HyperCore figures are in the
    report until the gateway adapter observes them."""
    from almanak.dashboard.models import Context, ExchangePosition, Scope, Value
    from almanak.dashboard.projection import snapshot
    from almanak.dashboard.tables import symbol_of, transactions_dataset

    if state.integrity_error:
        raise ValueError("invalidated Hyperliquid evidence cannot produce financial projections")
    values: dict[str, Value] = {}
    positions = []
    for owned in state.owned:
        target = owned.target
        inventory = []
        for index, amount in enumerate(owned.inventory):
            metadata = state.config.asset_metadata.get(amount.asset)
            value = Value(
                id=f"{target.allocation}-inventory{index}",
                amount=str(amount.amount),
                unit="base_units",
                asset=amount.asset,
                decimals=metadata.decimals if metadata else None,
                symbol=symbol_of(state, amount.asset),
                scope=Scope(account=target.account, fund=target.fund, target=target),
                evidence="configured",
                reason="USDC the strategy holds in the Safe on HyperEVM; HyperCore "
                "balances and the perp position are reported separately.",
            )
            values[value.id] = value
            inventory.append(value)
        positions.append(
            ExchangePosition(
                allocation=target.allocation, target=target, inventory=tuple(inventory)
            )
        )
    context = Context(
        config=Parameters.model_validate(
            {name: getattr(state.config, name) for name in Parameters.model_fields}
        ),
        memory=state.memory,
        positions=tuple(positions),
        values=values,
        datasets={"transactions": transactions_dataset(state, values, label)},
    )
    return context, snapshot(state, values, captured_at)


class Projection:
    report = staticmethod(report)
    dashboard = staticmethod(project)
