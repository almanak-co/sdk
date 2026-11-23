"""Identified Anvil execution with explicitly bound EOA or Safe operator test keys."""

import json
from pathlib import Path
from urllib.parse import urlsplit

from eth_account import Account as Signer

from almanak.gateway.environments import fork_binding
from almanak.gateway.ethereum import DataError
from almanak.gateway.evm import chain_number
from almanak.gateway.execution import EvmGateway


class ForkGateway(EvmGateway):
    def __init__(self, config, endpoint: str, key_file: Path, *, chain=None):
        validate_endpoint(endpoint)
        chain = chain or config.accounts.bindings[0].profile.fund.chain
        validate_fork_scope(config, chain)
        super().__init__(
            config,
            endpoint,
            load_signers(config, chain, key_file),
            chain=chain,
            local_transport=True,
        )

    def guard(self):
        metadata = self.reader._rpc("anvil_metadata", [])
        binding = fork_binding(self.config, self.reader.chain)
        origin = binding.origin
        network = metadata.get("forkedNetwork") if isinstance(metadata, dict) else None
        if (
            not isinstance(network, dict)
            or not isinstance(metadata.get("clientVersion"), str)
            or not metadata["clientVersion"].startswith("anvil/")
            or metadata.get("chainId") != 31337
            or int(self.reader._rpc("eth_chainId", []), 16) != 31337
            or metadata.get("instanceId") != binding.instance
            or network.get("forkBlockHash") != origin.hash
            or network.get("forkBlockNumber") != origin.number
        ):
            raise DataError("fork_mismatch", "fork instance or origin changed; execution refused")


def validate_endpoint(endpoint):
    url = urlsplit(endpoint)
    if (
        (url.scheme, url.hostname, url.query, url.fragment, url.username)
        != ("http", "127.0.0.1", "", "", None)
        or not url.port
        or url.path not in ("", "/")
    ):
        raise ValueError(
            "fork execution requires a plain HTTP loopback endpoint with explicit port"
        )


def validate_fork_scope(config, chain):
    if config.production is not None:
        raise ValueError("fork execution requires fork-only configuration")
    binding = fork_binding(config, chain)
    chain_number(binding.origin.chain.chain_id)


def load_signers(config, chain, key_file):
    keys = json.loads(key_file.read_text())
    validate_signer_keys(config, keys)
    signers = {}
    for allocation in config.allocations:
        if allocation.chain != chain:
            continue
        signer = allocation_signer(config, chain, allocation, keys[allocation.account])
        signers[allocation.account] = signer
    return signers


def validate_signer_keys(config, keys):
    # An account that only receives a bridge is allocated too; it signs nothing.
    allocated = {a.account for a in config.allocations} | {
        a.destination_account
        for a in config.allocations
        if getattr(a, "destination_account", None) is not None
    }
    if not isinstance(keys, dict) or set(keys) != allocated:
        raise ValueError("fork key file must match the explicitly allocated account names")


def allocation_signer(config, chain, allocation, key):
    if (
        allocation.native_asset.chain != chain.chain_id
        or allocation.native_asset.namespace != "slip44"
        or allocation.native_asset
        != next(a.native_asset for a in config.allocations if a.chain == chain)
    ):
        raise ValueError("configured gas asset differs from the identified fork native asset")
    profile = config.accounts.binding(allocation.account).profile
    if profile.fund.chain != chain:
        raise ValueError("this fork executor supports one identified chain per deployment")
    return bound_signer(key, profile.submitter.address)


def bound_signer(key, address):
    try:
        signer = Signer.from_key(key)
    except (ValueError, TypeError):
        raise ValueError("invalid fork signing key") from None
    if signer.address.lower() != address:
        raise ValueError("fork signing key does not match its bound transaction submitter")
    return signer
