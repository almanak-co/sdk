"""Operator entry points for live configuration, read-only checks and provisioning."""

import json
from pathlib import Path

from pydantic import TypeAdapter

from almanak.connectors.composition import configuration_from_json, connector_for
from almanak.connectors.funding import (
    admission_reserve,
    native_entry_minimum,
    payer_accounts,
    payer_entry_transactions,
)
from almanak.connectors.identifiers import Name
from almanak.gateway.credentials import create_wallet
from almanak.gateway.production import ProductionGateway
from almanak.gateway.providers import provider_endpoint
from almanak.gateway.routing import ChainRouter
from almanak.sandbox import worker_command


def production_backend(args, config):
    validate_launch(args, config)
    for path in (args.env_file, args.key_file, args.password_file):
        if path is not None:
            worker_command(key_file=path)
    password = read_password() if getattr(args, "password_stdin", False) else None
    return production_routes(
        config, args.env_file, args.key_file, args.password_file, password=password
    )


def read_password():
    import sys

    password = json.loads(sys.stdin.readline(65537))
    if not isinstance(password, str) or not password or len(password) > 65536:
        raise ValueError("invalid ephemeral password input")
    return password


def production_routes(
    config, env_file, key_file=None, password_file=None, *, password=None, signer_file=None
):
    from functools import partial

    # A connector whose venue is not only a chain builds its own production gateway.
    gateway = getattr(connector_for(config).execution, "production_gateway", ProductionGateway)
    factories = {
        network.chain: partial(
            gateway,
            config,
            provider_endpoint(network.provider, env_file),
            key_file,
            password_file,
            chain=network.chain,
            password=password,
            **({"signer_file": signer_file} if signer_file is not None else {}),
        )
        for network in config.production.networks
    }
    if len(factories) == 1:
        return next(iter(factories.values()))()
    return ChainRouter(config, factories)


def validate_launch(args, config):
    if config.production is None or args.env_file is None or args.key_file is None:
        raise ValueError("production gateway requires production config, --env-file and --key-file")
    if args.fork_key_file is not None:
        raise ValueError("production execution cannot load fork credentials")


def command(args):
    if args.product == "wallet-create":
        name = TypeAdapter(Name).validate_python(args.account)
        result = create_wallet(args.output, name)
    else:
        result = preflight(args.config, args.env_file)
    print(json.dumps(result, indent=2))
    return 0


def preflight(path: Path, env_file: Path):
    config = configuration_from_json(path.read_text())
    return preflight_config(config, env_file)


def preflight_config(config, env_file: Path):
    if config.production is None:
        raise ValueError("production preflight requires explicit production configuration")
    from almanak.gateway.ethereum import DataError
    from almanak.gateway.routing import backend_for

    backend = production_routes(config, env_file)
    chains = []
    try:
        for network in config.production.networks:
            try:
                selected = backend_for(backend, network.chain)
                observed = selected.observe()
                selected.require_fresh(observed[0].block)
                balances = [
                    fund_preflight(config, b, observed)
                    for b in config.accounts.bindings
                    if b.profile.fund.chain == network.chain
                ]
                chains.append(
                    {
                        "network": network.chain.model_dump(mode="json"),
                        "block": observed[0].block.model_dump(mode="json"),
                        "funds": balances,
                        "funded": all(item["funded"] for item in balances),
                        "observations": [item.model_dump(mode="json") for item in observed],
                    }
                )
            except DataError as exc:
                chains.append(
                    {
                        "network": network.chain.model_dump(mode="json"),
                        "funded": False,
                        "error": exc.kind,
                    }
                )
        result = {
            "execution_environment": "production",
            "read_only": True,
            "receipt_confirmation": "canonical_mined",
            "chains": chains,
            "funded": all(item["funded"] for item in chains),
            "transaction_simulation": "performed during preparation for each actual operation",
        }
        if len(chains) == 1:
            result.update(chains[0])
        return result
    finally:
        backend.close()


def fund_preflight(config, binding, observations):
    selected = [item for item in observations if item.target.account == binding.name]
    policy = config.production.policy(binding.name)
    accounts = payer_accounts(config, binding.profile.submitter)
    entries = payer_entry_transactions(config, accounts)
    native_required = native_entry_minimum(policy, config, accounts, entries)
    tokens, pristine = connector_for(config).execution.preflight(config, binding.name, selected)
    return {
        "account": binding.name,
        "fund": binding.profile.fund.model_dump(mode="json"),
        "gas_payer": binding.profile.submitter.model_dump(mode="json"),
        "tokens": tokens,
        "native_balance": selected[0].native_balance.model_dump(mode="json"),
        "native_admission_reserve_base_units": str(admission_reserve(policy)),
        "native_entry_transactions": entries,
        "native_entry_minimum_base_units": str(native_required),
        "pristine": pristine,
        "funded": pristine
        and all(t["funded"] for t in tokens)
        and selected[0].native_balance.amount >= native_required,
        "permissions": "requires_generated_and_verified_roles"
        if binding.profile.kind == "safe_roles"
        else "economic_capabilities",
    }
