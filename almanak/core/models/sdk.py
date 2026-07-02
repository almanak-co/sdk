from abc import ABC, abstractmethod

from almanak.core.enums import Network


class ISDK(ABC):
    """Interface for protocol SDKs."""

    @abstractmethod
    def __init__(self, network: Network, chain: str, **kwargs):
        """
        Initialize the SDK with the provided parameters.

        Args:
            network: Network enum value (required)
            chain: Canonical lowercase chain name (required)
            **kwargs: Additional parameters needed for initialization, which may include:
                - api_key: API key if required
                - web3_provider_uri: Web3 provider URI if required
                - Any other protocol-specific configuration values
        """
        pass
