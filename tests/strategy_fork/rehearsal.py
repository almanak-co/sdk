"""Production rehearsal inputs for converted references.

A reference with a production author file rehearses through it; every other
converted reference gets the fixture payer policy injected as author gas. The
generated operator file binds each account to a Foundry test account whose key
is public, so it can never be mistaken for a production operator file.
"""

import json
import os
import shutil
from pathlib import Path

import yaml
from eth_account import Account as Signer

from almanak.configuration.unresolved import read_inputs
from almanak.configuration.yaml import ConfigurationError, load
from almanak.connectors.composition import connector_from_wire
from almanak.connectors.support import RunMode
from almanak.gateway.credentials import write_private
from almanak.gateway.evm import chain_number
from almanak.gateway.fixture_preparation import EOA_INDEX_START
from almanak.gateway.fork_fixture import fixture_signer
from almanak.gateway.operator_config import read_operator_config
from scripts.ci.check_strategies import CONVERTED

POLICY = Path(__file__).with_name("rehearsal-policy.json")
ROOT = Path(__file__).parents[2]
# Authoring references whose production rehearsal has not run yet; each joins the
# selection with its own fork qualification, never by its chain's declaration alone.
UNREHEARSED: frozenset[str] = frozenset(
    {
        "aave_v3_base_sma_borrow",
        # Their connector declares no production support. Both passed the rehearsal
        # locally with the declaration set by hand, which retains nothing: they stay
        # here until a hosted Fork Qualification run has rehearsed them.
        "across_usdc_arbitrum_base_bridge",
        "arbitrum_base_bridge_and_lend",
        "uniswap_v3_base_atr_breakout",
        "uniswap_v3_base_rsi",
        "uniswap_v3_base_rsi_lp",
    }
)
# Comma-separated UNREHEARSED names whose fork qualification a rehearsal run is;
# the hosted fork qualification workflow sets it, the default selection ignores it.
QUALIFY_ENV = "ALMANAK_REHEARSAL_QUALIFY"
PASSWORD = "public-test-password"
AMOUNTS = ("max_transaction_fee", "total_budget", "exit_reserve")
LIMITS = (
    "source",
    "base_fee_basis",
    "reward_percentile",
    "base_fee_multiplier_bps",
    "max_gas_price_wei",
    "max_gas_limit",
    "gas_limit_buffer_bps",
)


def account_chains(inputs):
    return tuple(dict.fromkeys(account.chain for account in inputs.accounts.values()))


def rehearsal_author(reference: Path, directory: Path) -> Path:
    """The author file a rehearsal reads: the production file, or gas injected into a copy."""
    target = directory / "config.yaml"
    production = reference / "config.production.yaml"
    if production.is_file():
        shutil.copyfile(production, target)
    else:
        raw = load(reference / "config.yaml").value
        inputs = read_inputs(reference / "config.yaml").inputs
        if not inputs.strategy.gas:
            raw["strategy"]["gas"] = author_gas(inputs)
        target.write_text(yaml.safe_dump(raw, sort_keys=False))
    read_inputs(target)
    return target


def rehearsal_fork_operator(name: str) -> Path:
    """Select an explicit fixture when a production author uses different account names."""
    fixture = ROOT / "tests" / "fixtures" / "author" / name / "production-rehearsal.operator.yaml"
    if fixture.is_file():
        return fixture
    return ROOT / "examples" / "operators" / f"{name}.fork.yaml"


def qualifying_references() -> frozenset[str]:
    """Unrehearsed references this run qualifies, from the environment; empty by default."""
    names = frozenset(filter(None, (n.strip() for n in os.environ.get(QUALIFY_ENV, "").split(","))))
    unknown = names - UNREHEARSED
    if unknown:
        raise ValueError(f"{QUALIFY_ENV} names references outside UNREHEARSED: {sorted(unknown)}")
    return names


def production_references(qualifying: frozenset[str] = frozenset()) -> list[str]:
    """Converted references whose complete market scope declares production support.

    `qualifying` names UNREHEARSED references joining this selection for their own
    fork qualification; a name outside that set, or one whose connectors do not
    declare production EOA support on its chains, is refused rather than dropped.
    """
    if qualifying - UNREHEARSED:
        raise ValueError(f"not unrehearsed: {sorted(qualifying - UNREHEARSED)}")
    selected = [
        name for name in sorted((CONVERTED - UNREHEARSED) | qualifying) if declares_production(name)
    ]
    missing = qualifying - set(selected)
    if missing:
        raise ValueError(f"no production EOA support declared for {sorted(missing)}")
    return selected


def declares_production(name: str) -> bool:
    """Whether every market of a converted reference declares production EOA support."""
    try:
        inputs = read_inputs(ROOT / "strategies" / name / "config.yaml").inputs
    except ConfigurationError:
        return False
    support = [
        connector_from_wire({"connector": str(market.connector)}).support_for(market.chain)
        for market in inputs.markets.values()
    ]
    return all(
        RunMode.PRODUCTION in item.modes and "eoa" in item.account_profiles for item in support
    )


def author_gas(inputs):
    """One author gas policy per account chain, from the fixture payer policy."""
    policies = json.loads(POLICY.read_text())
    gas = {}
    for chain in account_chains(inputs):
        policy = policies[str(chain)]
        gas[f"payer_{chain_number(chain)}"] = {
            "accounts": [name for name, a in inputs.accounts.items() if a.chain == chain],
            "chain": str(chain),
            **{key: policy[key] for key in LIMITS},
            **{key: {"base_units": policy[key]["amount"]} for key in AMOUNTS},
        }
    return gas


def rehearsal_signers(inputs):
    """One public Foundry test key per account, from the code-free EOA proof range."""
    return {
        name: fixture_signer(EOA_INDEX_START + index)
        for index, name in enumerate(sorted(inputs.accounts))
    }


def rehearsal_operator(author_path: Path, directory: Path) -> Path:
    """A production operator file bound to public test keys, with ceilings equal to the policy."""
    inputs = read_inputs(author_path).inputs
    signers = rehearsal_signers(inputs)
    directory = directory.resolve()
    chains = account_chains(inputs)
    operator = {
        "schema_version": "operator-gateway-config-v1",
        "modes": ["production"],
        "networks": [network(chain) for chain in chains],
        "accounts": {
            "bindings": [
                binding(name, account.chain, signers[name].address)
                for name, account in inputs.accounts.items()
            ]
        },
        "production": {
            "networks": [execution_network(chain) for chain in chains],
            "payers": ceilings(inputs),
            "signing": {
                "key_file": str(directory / "keystores.json"),
                "password_file": str(directory / "password"),
            },
        },
    }
    path = directory / "operator.production.yaml"
    path.write_text(yaml.safe_dump(operator, sort_keys=False))
    write_keystores(directory, signers)
    read_operator_config(path)
    return path


def execution_network(chain):
    return {"chain": str(chain), "max_observation_age_seconds": 180}


def network(chain):
    provider = {"kind": "rpc_url", "url_env": f"REHEARSAL_RPC_{chain_number(chain)}"}
    return {"chain": str(chain), "provider": provider}


def binding(name, chain, address):
    fund = {"chain": {"chain_id": str(chain), "environment": "mainnet"}, "address": address.lower()}
    return {"name": name, "profile": {"kind": "eoa", "fund": fund}}


def ceilings(inputs):
    """One ceiling per account: every rehearsal account is its own EOA payer."""
    return [
        ceiling(policy.resolve(name), account)
        for name, policy in inputs.strategy.gas.items()
        for account in policy.accounts
    ]


def ceiling(policy, account):
    return {
        "accounts": [account],
        "max_gas_price_wei": policy.max_gas_price_wei,
        "max_gas_limit": policy.max_gas_limit,
        "max_transaction_fee": policy.max_transaction_fee.model_dump(mode="json"),
        "total_budget": policy.total_budget.model_dump(mode="json"),
        "allow_replacement": policy.replacement is not None,
    }


def write_keystores(directory, signers):
    stores = {
        name: Signer.encrypt(signer.key, PASSWORD, kdf="pbkdf2", iterations=1000)
        for name, signer in signers.items()
    }
    write_private(directory / "keystores.json", json.dumps(stores))
    write_private(directory / "password", PASSWORD)
