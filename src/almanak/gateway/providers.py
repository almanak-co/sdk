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


class AlchemyProvider(Model):
    kind: Literal["alchemy"]
    network: Annotated[str, Field(pattern=r"^[a-z0-9]+(?:-[a-z0-9]+)*$")]
    api_key_env: EnvName


class UrlProvider(Model):
    kind: Literal["rpc_url"]
    url_env: EnvName


Provider = Annotated[AlchemyProvider | UrlProvider, Field(discriminator="kind")]


def provider_endpoint(provider: Provider, env_file: Path) -> str:
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
