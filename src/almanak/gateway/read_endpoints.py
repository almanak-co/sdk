"""Chain-aware read transport resolution for protocol observation commands."""

import time
from pathlib import Path

from dotenv import dotenv_values

from almanak.caip import ChainId
from almanak.gateway.ethereum import BUSY_WAIT_LIMIT, DataError, EthereumReader
from almanak.gateway.providers import (
    ALCHEMY_NETWORKS,
    AlchemyProvider,
    provider_endpoint,
    public_endpoint,
)
from almanak.models import Chain


def alchemy_key(env_file: Path) -> str:
    values = dotenv_values(env_file, interpolate=False)
    key = values.get("ALCHEMY_API_KEY")
    if not key:
        raise ValueError("ALCHEMY_API_KEY is required in the selected env file")
    return key


def read_endpoint(chain: ChainId, env_file: Path) -> str:
    try:
        network = ALCHEMY_NETWORKS[chain]
    except KeyError:
        raise ValueError(f"no Alchemy read endpoint is configured for {chain}") from None
    provider = AlchemyProvider(kind="alchemy", network=network, api_key_env="ALCHEMY_API_KEY")
    return provider_endpoint(provider, env_file)


def mainnet_reader(chain: ChainId, endpoint: str, *, observation: bool = False) -> EthereumReader:
    # Only observation reads re-send a transient single read and pin tags behind the head;
    # see `EthereumReader`.
    return EthereumReader(
        Chain(chain_id=chain, environment="mainnet"),
        endpoint,
        retry_budget=BUSY_WAIT_LIMIT if observation else 0.0,
        observation=observation,
    )


def verified_reader(chain: ChainId, env_file: Path) -> EthereumReader:
    """Resolve the chain's Alchemy endpoint and verify its identity before any read.

    Archive coverage is not probed here; it is a caller concern. Historical
    reads fail on the read path when the endpoint lacks the requested data.
    The caller owns the returned reader's context-manager lifecycle.
    """
    reader = mainnet_reader(chain, read_endpoint(chain, env_file), observation=True)
    try:
        reader.verify_chain()
    except Exception:
        # The caller never receives this reader, so its owned client closes here.
        reader.__exit__()
        raise
    return reader


# A public endpoint whose head is older than this is lagging or stalled.
PUBLIC_HEAD_AGE_SECONDS = 120


def public_reader(chain: ChainId, name: str) -> EthereumReader:
    """The named public endpoint for the chain, verified before any read.

    Nobody answers for a public endpoint, so it has to show more than its chain
    number: the pinned genesis block, which a fork or another network sharing the
    number does not have, and a recent head. The caller owns the reader's lifecycle.
    """
    reader = mainnet_reader(chain, public_endpoint(name, chain), observation=True)
    try:
        reader.verify_chain()
        verify_public(reader)
    except Exception:
        reader.__exit__()
        raise
    return reader


def verify_public(reader: EthereumReader, clock=time.time) -> None:
    from almanak.chains import evm_network

    with reader.retained_pin():
        if reader.pin_block(0).hash != evm_network(reader.chain.chain_id).genesis:
            raise DataError("chain_mismatch", "public endpoint has a different genesis")
        head = reader.pin_block("latest")
    # Measured after the answer arrives, so a slow read counts against the head.
    age = clock() - head.timestamp
    if age > PUBLIC_HEAD_AGE_SECONDS:
        raise DataError("stale", f"public endpoint head is {int(age)} seconds old")
