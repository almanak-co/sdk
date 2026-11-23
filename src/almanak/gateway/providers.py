"""Explicit RPC provider selection, with credentials resolved only in trusted code."""

from pathlib import Path
from typing import Annotated, Literal
from urllib.parse import quote, urlsplit

from dotenv import dotenv_values
from pydantic import Field

from almanak.caip import ChainId
from almanak.models import Model

EnvName = Annotated[str, Field(pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")]

# These routes match the explicitly selected rollout providers. Availability and
# archive coverage are checked on each read path, not implied by this mapping.
ALCHEMY_NETWORKS = {
    ChainId("eip155:1"): "eth-mainnet",
    ChainId("eip155:43114"): "avax-mainnet",
    ChainId("eip155:8453"): "base-mainnet",
    ChainId("eip155:42161"): "arb-mainnet",
    ChainId("eip155:56"): "bnb-mainnet",
    ChainId("eip155:10"): "opt-mainnet",
    ChainId("eip155:137"): "polygon-mainnet",
    ChainId("eip155:4663"): "robinhood-mainnet",
    ChainId("eip155:999"): "hyperliquid-mainnet",
}


# Public endpoints that need no key, by the name a command selects them with. One named
# endpoint serves a command; nothing falls back to another. They serve `almanak ax`
# reads and direct actions only: none promises availability, archive state or wide
# log ranges, and each operator sees the addresses a command reads and the
# transactions it sends. Nobody answers for what one reports, and a direct action
# signs what its reads resolve: that risk is the caller's when they name one.
#
# A pair is listed only after it passed every check of `scripts/qualify_public_rpc.py`
# in repeated full runs: the pinned genesis, state at `finalized`, `latest` and a
# numbered block, and the fee quote and receipt reads of a direct action. A pass is a
# point-in-time result, and a transient refusal fails a run as it would fail a command,
# so a listed endpoint can still refuse a read. A shared provider is absent from a chain
# where its keyless tier refused one of those reads in those runs, and a chain
# operator's own endpoint is listed under the operator's name. HyperEVM has no entry.
# BNB Chain's own endpoint is not listed: it keeps less than a minute of state, and a
# direct action reads at blocks older than that when it reconciles a mined step.
# Removing a pair strands no kept action record: `ax recover` takes a named transport
# when the record's own endpoint is no longer listed.
PUBLIC_RPC: dict[str, dict[ChainId, str]] = {
    "publicnode": {
        ChainId("eip155:1"): "https://ethereum-rpc.publicnode.com",
        ChainId("eip155:43114"): "https://avalanche-c-chain-rpc.publicnode.com",
    },
    "drpc": {
        ChainId("eip155:1"): "https://eth.drpc.org",
        ChainId("eip155:8453"): "https://base.drpc.org",
        ChainId("eip155:10"): "https://optimism.drpc.org",
        ChainId("eip155:137"): "https://polygon.drpc.org",
        ChainId("eip155:4663"): "https://robinhood.drpc.org",
    },
    "blastapi": {ChainId("eip155:56"): "https://bsc-mainnet.public.blastapi.io"},
    "base": {ChainId("eip155:8453"): "https://mainnet.base.org"},
    "arbitrum": {ChainId("eip155:42161"): "https://arb1.arbitrum.io/rpc"},
    "optimism": {ChainId("eip155:10"): "https://mainnet.optimism.io"},
}


class PublicProvider(Model):
    kind: Literal["public_rpc"]
    name: Annotated[str, Field(pattern=r"^[a-z0-9]+(?:-[a-z0-9]+)*$")]


def public_endpoint(name: str, chain: ChainId) -> str:
    try:
        return PUBLIC_RPC[name][chain]
    except KeyError:
        names = sorted(known for known, chains in PUBLIC_RPC.items() if chain in chains)
        choice = f"choose from {', '.join(names)}" if names else "use a provider key instead"
        raise ValueError(f"no public endpoint named {name} serves {chain}; {choice}") from None


class AlchemyProvider(Model):
    kind: Literal["alchemy"]
    network: Annotated[str, Field(pattern=r"^[a-z0-9]+(?:-[a-z0-9]+)*$")]
    api_key_env: EnvName


class UrlProvider(Model):
    kind: Literal["rpc_url"]
    url_env: EnvName


Provider = Annotated[AlchemyProvider | UrlProvider | PublicProvider, Field(discriminator="kind")]


def provider_endpoint(provider: Provider, env_file: Path) -> str:
    if isinstance(provider, PublicProvider):
        # Everything that resolves an endpoint here executes, forks or collects history.
        raise ValueError(
            "a public endpoint serves only `almanak ax` reads and direct actions; "
            "this needs a provider key or an operator RPC URL"
        )
    values = dotenv_values(env_file, interpolate=False)
    name = provider.api_key_env if isinstance(provider, AlchemyProvider) else provider.url_env
    value = values.get(name)
    if not value:
        raise ValueError(f"{name} is required in the selected env file")
    if isinstance(provider, AlchemyProvider):
        return f"https://{provider.network}.g.alchemy.com/v2/{quote(value, safe='')}"
    validate_provider_url(value)
    return value


def validate_provider_url(value):
    try:
        parsed = urlsplit(value)
        valid = parsed.scheme in ("http", "https") and parsed.hostname and not parsed.fragment
        _ = parsed.port
    except ValueError:
        valid = False
    if not valid:
        raise ValueError("configured RPC URL must be an HTTP(S) endpoint") from None
