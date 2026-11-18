"""Tables the lending connectors share: gas paid per fund and committed transactions named
in lending terms ("Supply USDC", "Repay WETH")."""

from almanak.dashboard.models import Column, Dataset, Ref, Scope, Value
from almanak.dashboard.tables import decimals_of, named_accounts, symbol_of, transactions_dataset
from almanak.gateway.evm import native_asset

LENDING_LABELS = {
    "approve": "Approve",
    "revoke": "Revoke approval",
    "supply": "Supply",
    "withdraw": "Withdraw",
    "borrow": "Borrow",
    "repay": "Repay",
}


def operation_asset(operation):
    amount = getattr(operation, "amount", None)
    if amount is not None:
        return amount.asset
    target = getattr(operation, "collateral", None) or operation.target
    return getattr(target, "asset", None)


def lending_label(state, operation):
    """What a lending operation did, with its token: "Supply USDC", "Use WETH as collateral"."""
    asset = operation_asset(operation)
    token = symbol_of(state, asset) if asset is not None else None
    if operation.kind == "collateral":
        if not token:
            return "Enable collateral" if operation.enabled else "Disable collateral"
        if operation.enabled:
            return f"Use {token} as collateral"
        return f"Stop using {token} as collateral"
    label = LENDING_LABELS.get(operation.kind, operation.kind.replace("_", " ").capitalize())
    return f"{label} {token}" if token else label


def lending_transactions(state, values, extra=()):
    return transactions_dataset(
        state, values, lambda s, o: lending_label(s, o.operation), extra=extra
    )


def fund_costs(state, values, *, ledger_required=True):
    """Native receipt gas per fund; the fund account is named only when there are several.
    With `ledger_required`, gas before the first observation is unavailable, not zero."""
    rows = []
    for binding in state.config.accounts.bindings:
        profile = binding.profile
        outcomes = [o for o in state.outcomes if o.operation.target.account == binding.name]
        key = f"{binding.name}-gas"
        asset = native_asset(profile.fund.chain.chain_id)
        values[key] = Value(
            id=key,
            amount=str(sum(o.gas_paid.amount for o in outcomes))
            if state.initialized or not ledger_required
            else None,
            unit="base_units",
            asset=asset,
            decimals=decimals_of(state, asset),
            symbol=symbol_of(state, asset),
            scope=Scope(account=binding.name, fund=profile.fund, payer=profile.submitter),
            evidence="recorded",
            block=outcomes[-1].after.block if outcomes else None,
            reason="Gas paid in retained receipts for this fund's operations."
            if state.initialized or not ledger_required
            else "No initialized receipt ledger.",
        )
        rows.append(
            {
                "account": binding.name,
                "fund": profile.fund.address,
                "payer": profile.submitter.address,
                "gas": Ref(ref=key),
            }
        )
    columns, rows = named_accounts(
        (
            Column(key="account", label="Fund account"),
            Column(key="fund", label="Fund address"),
            Column(key="payer", label="Gas payer"),
            Column(key="gas", label="Receipt gas", kind="value"),
        ),
        rows,
    )
    return Dataset(
        columns=columns,
        rows=rows,
        coverage="Native receipt costs attributed to each fund's operations and its submitter. "
        "External payer transfers and fiat valuation are excluded.",
    )
