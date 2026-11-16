"""What each bound account and gas payer must hold before a strategy starts.

The `funding` block of `almanak check --json` is computed from resolved parameters
only. It states requirements, never observations: a consumer compares it with the
balances it measures. Amounts are base-unit strings and every asset is CAIP-19.
"""

from almanak.connectors.composition import connector_for
from almanak.connectors.funding import (
    native_entry_minimum,
    native_threshold,
    payer_accounts,
    payer_entry_transactions,
)
from almanak.connectors.support import RunMode
from almanak.gateway.evm import native_metadata

SCHEMA = "almanak-funding-v1"

# Backtest bindings carry generated addresses and simulated inventory, so a
# funding statement for them would describe accounts that do not exist.
FUNDED_MODES = (RunMode.FORK, RunMode.PRODUCTION)


def funding_view(parameters, mode, parameters_digest) -> dict:
    if mode not in FUNDED_MODES:
        return {}
    return {"funding": funding_block(parameters, mode, parameters_digest)}


def funding_block(parameters, mode, parameters_digest) -> dict:
    requirements = connector_for(parameters).execution.requirements
    metadata = getattr(parameters, "asset_metadata", {})
    return {
        "schema": SCHEMA,
        "mode": mode.value,
        "parameters_digest": parameters_digest,
        "accounts": [
            account_entry(binding, requirements(parameters, binding.name), metadata)
            for binding in parameters.accounts.bindings
        ],
        "payers": payer_entries(parameters),
    }


def account_entry(binding, requirements, metadata) -> dict:
    fund = binding.profile.fund
    return {
        "account": binding.name,
        "chain": str(fund.chain.chain_id),
        "fund": str(fund.address),
        "payer": str(binding.profile.submitter.address),
        "assets": [asset_entry(requirement, metadata) for requirement in requirements],
    }


def asset_entry(requirement, metadata) -> dict:
    known = metadata.get(requirement.asset)
    return {
        "asset": str(requirement.asset),
        "amount_base_units": str(requirement.amount),
        "decimals": None if known is None else known.decimals,
        "symbol": None if known is None else known.symbol,
        "basis": requirement.basis,
    }


def payer_entries(parameters) -> list[dict]:
    """One entry per gas payer; accounts sharing a payer share its balance and policy."""
    payers = dict.fromkeys(binding.profile.submitter for binding in parameters.accounts.bindings)
    return [payer_entry(parameters, payer) for payer in payers]


def payer_entry(parameters, payer) -> dict:
    names = payer_accounts(parameters, payer)
    policy = next((p for p in parameters.gas if set(p.accounts) & set(names)), None)
    asset, native = native_metadata(payer.chain.chain_id)
    entries = payer_entry_transactions(parameters, names)
    return {
        "payer": str(payer.address),
        "chain": str(payer.chain.chain_id),
        "accounts": list(names),
        "asset": str(asset),
        "decimals": native.decimals,
        "minimum_native_balance_base_units": str(native_threshold(parameters, names)),
        "entry_transactions": entries,
        "entry_minimum_base_units": str(native_entry_minimum(policy, parameters, names, entries)),
        "gas": None if policy is None else gas_entry(policy),
    }


def gas_entry(policy) -> dict:
    return {
        "policy": policy.name,
        "max_transaction_fee_base_units": str(policy.max_transaction_fee.amount),
        "exit_reserve_base_units": str(policy.exit_reserve.amount),
    } | (
        {}
        if policy.total_budget is None
        else {"total_budget_base_units": str(policy.total_budget.amount)}
    )
