"""Chain-aware read transport resolution for protocol observation commands."""

from pathlib import Path

from dotenv import dotenv_values

from almanak.caip import ChainId
from almanak.gateway.ethereum import BUSY_WAIT_LIMIT, EthereumReader
from almanak.gateway.providers import ALCHEMY_NETWORKS, AlchemyProvider, provider_endpoint
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
