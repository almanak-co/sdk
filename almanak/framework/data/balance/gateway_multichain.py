"""Multi-Chain Gateway Balance Provider.

Provides balance queries across multiple chains via the gateway's
MarketService, replacing direct Web3 calls in multi-chain strategies.

Example:
    from almanak.framework.data.balance.gateway_multichain import MultiChainGatewayBalanceProvider

    provider = MultiChainGatewayBalanceProvider(
        client=gateway_client,
        wallet_address="0x1234...",
        chains=["arbitrum", "base"],
    )
    balance = provider("USDC", "base")
"""

import logging
from decimal import Decimal

from almanak.framework.gateway_client import GatewayClient
from almanak.framework.strategies.intent_strategy import TokenBalance
from almanak.gateway.proto import gateway_pb2

logger = logging.getLogger(__name__)


class MultiChainGatewayBalanceProvider:
    """Balance provider using gateway for multi-chain balance queries.

    Implements the MultiChainBalanceProvider callable interface:
        Callable[[str, str], TokenBalance]

    All balance queries are routed through the gateway's MarketService,
    so the CLI process never makes direct RPC calls.
    """

    def __init__(
        self,
        client: GatewayClient,
        wallet_address: str,
        chains: list[str],
    ) -> None:
        """Initialize the multi-chain gateway balance provider.

        Args:
            client: Connected GatewayClient instance
            wallet_address: Wallet address to query balances for
            chains: List of supported chain names
        """
        self._client = client
        self._wallet_address = wallet_address
        self._chains = [c.lower() for c in chains]

        logger.info(f"MultiChainGatewayBalanceProvider initialized for chains: {self._chains}")

    def get_balance(self, token: str, chain: str) -> TokenBalance:
        """Get balance for a token on a specific chain via gateway.

        Args:
            token: Token symbol (e.g., "USDC", "WETH")
            chain: Chain name (e.g., "arbitrum", "base")

        Returns:
            TokenBalance with balance and USD value
        """
        chain_lower = chain.lower()
        if chain_lower not in self._chains:
            logger.warning(f"Chain '{chain}' not in configured chains: {self._chains}")
            return TokenBalance(symbol=token, balance=Decimal("0"), balance_usd=Decimal("0"))

        try:
            response = self._client.market.GetBalance(
                gateway_pb2.BalanceRequest(
                    token=token,
                    chain=chain_lower,
                    wallet_address=self._wallet_address,
                ),
                timeout=15.0,
            )

            balance = Decimal(response.balance) if response.balance else Decimal("0")
            balance_usd = Decimal(response.balance_usd) if response.balance_usd else Decimal("0")

            return TokenBalance(
                symbol=token,
                balance=balance,
                balance_usd=balance_usd,
            )

        except Exception as e:
            logger.warning(f"Failed to get {token} balance on {chain} via gateway: {e}")
            return TokenBalance(symbol=token, balance=Decimal("0"), balance_usd=Decimal("0"))

    def __call__(self, token: str, chain: str) -> TokenBalance:
        """Callable interface matching MultiChainBalanceProvider type."""
        return self.get_balance(token, chain)

    @property
    def chains(self) -> list[str]:
        """Get list of configured chains."""
        return self._chains
