"""On-chain GMX V2 perpetual position reader.

Queries open GMX V2 positions for a wallet using the GMXV2SDK
(on-chain Reader contract with REST API fallback).

Unlike LP/lending readers which use the gateway's generic RPC call,
GMX V2 queries need the full SDK (contract ABIs, multicall decoding).
This reader wraps GMXV2SDK and returns typed dataclasses.
"""

import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PerpsPositionOnChain:
    """On-chain state of a GMX V2 perpetual position.

    Values are raw (not human-readable) to preserve precision.
    The perps_valuer converts to human-readable.
    """

    account: str
    market: str  # Market contract address
    collateral_token: str  # Collateral token address
    size_in_usd: int  # 30 decimals
    size_in_tokens: int  # Index token decimals
    collateral_amount: int  # Collateral token decimals
    is_long: bool
    borrowing_factor: int
    funding_fee_amount_per_size: int
    increased_at_time: int  # Unix timestamp
    decreased_at_time: int  # Unix timestamp

    @property
    def is_active(self) -> bool:
        """Position has non-zero size."""
        return self.size_in_usd > 0

    @property
    def position_key(self) -> str:
        """Unique identifier matching strategy-reported position IDs."""
        side = "long" if self.is_long else "short"
        return f"gmx-{self.market.lower()}-{self.collateral_token.lower()}-{side}"


class PerpsPositionReader:
    """Reads GMX V2 positions via GMXV2SDK.

    Currently supports Arbitrum only (GMXV2SDK limitation).
    Uses on-chain Reader contract with REST API fallback (PR #1086).
    """

    # Chains where the GMXV2SDK Reader contract is deployed and tested.
    # Avalanche has GMX V2 markets but the SDK only supports arbitrum currently.
    SUPPORTED_CHAINS = {"arbitrum"}

    def __init__(self, rpc_url: str | None = None) -> None:
        """Initialize with an optional RPC URL.

        Args:
            rpc_url: JSON-RPC endpoint for position queries.
                If None, queries return empty (graceful degradation).
        """
        self._rpc_url = rpc_url

    def read_positions(
        self,
        chain: str,
        wallet_address: str,
    ) -> list[PerpsPositionOnChain]:
        """Query all open GMX V2 positions for a wallet.

        Args:
            chain: Chain identifier (must be "arbitrum" or "avalanche").
            wallet_address: Wallet address to query.

        Returns:
            List of active positions, empty on failure.
        """
        if not self._rpc_url:
            return []

        if chain not in self.SUPPORTED_CHAINS:
            logger.debug("GMX V2 not supported on %s", chain)
            return []

        try:
            from almanak.framework.connectors.gmx_v2.sdk import GMXV2SDK

            sdk = GMXV2SDK(self._rpc_url, chain=chain)
            raw_positions = sdk.get_account_positions(wallet_address)

            positions = []
            for raw in raw_positions:
                pos = _parse_position_dict(raw, wallet_address)
                if pos and pos.is_active:
                    positions.append(pos)

            if positions:
                logger.debug(
                    "Found %d active GMX V2 positions for %s on %s",
                    len(positions),
                    wallet_address[:10],
                    chain,
                )

            return positions

        except ImportError:
            logger.debug("GMXV2SDK not available (missing web3 dependency)")
            return []
        except Exception as e:
            logger.warning("GMX V2 position query failed for %s on %s: %s", wallet_address[:10], chain, e)
            return []

    @staticmethod
    def from_gateway_client(gateway_client: object | None, chain: str = "") -> "PerpsPositionReader":
        """Create a reader from a gateway client or DirectRpcAdapter.

        Extraction order:
        1. DirectRpcAdapter: reads URL from _rpc_stub._rpc_url (paper trading)
        2. Environment: builds URL from ALCHEMY_API_KEY (live strategy runs)

        Args:
            gateway_client: Gateway client or DirectRpcAdapter instance.
            chain: Chain hint for RPC URL construction (e.g. "arbitrum").

        Returns:
            PerpsPositionReader (possibly with no URL if extraction fails).
        """
        if gateway_client is None:
            return PerpsPositionReader()

        # DirectRpcAdapter: has _rpc_stub._rpc_url (paper trading path)
        rpc_stub = getattr(gateway_client, "_rpc_stub", None)
        if rpc_stub is not None:
            rpc_url = getattr(rpc_stub, "_rpc_url", None)
            if rpc_url:
                return PerpsPositionReader(rpc_url=rpc_url)

        # Live gateway: try to build RPC URL from environment
        rpc_url = _get_rpc_url_from_env(chain or "arbitrum")
        if rpc_url:
            return PerpsPositionReader(rpc_url=rpc_url)

        return PerpsPositionReader()


def _get_rpc_url_from_env(chain: str) -> str | None:
    """Build an RPC URL from environment variables.

    Uses the same ALCHEMY_API_KEY that the gateway uses for RPC access.
    """
    import os

    api_key = os.environ.get("ALCHEMY_API_KEY")
    if not api_key:
        return None

    alchemy_chain_slugs = {
        "arbitrum": "arb-mainnet",
    }
    slug = alchemy_chain_slugs.get(chain)
    if not slug:
        return None

    return f"https://{slug}.g.alchemy.com/v2/{api_key}"


def _parse_position_dict(raw: dict, account: str) -> PerpsPositionOnChain | None:
    """Convert SDK position dict to typed dataclass.

    Args:
        raw: Position dict from GMXV2SDK.get_account_positions().
        account: Wallet address (fallback if not in raw).

    Returns:
        PerpsPositionOnChain or None if missing required fields.
    """
    try:
        return PerpsPositionOnChain(
            account=raw.get("account", account),
            market=raw.get("market", ""),
            collateral_token=raw.get("collateral_token", ""),
            size_in_usd=int(raw.get("size_in_usd", 0)),
            size_in_tokens=int(raw.get("size_in_tokens", 0)),
            collateral_amount=int(raw.get("collateral_amount", 0)),
            is_long=bool(raw.get("is_long", False)),
            borrowing_factor=int(raw.get("borrowing_factor", 0)),
            funding_fee_amount_per_size=int(raw.get("funding_fee_amount_per_size", 0)),
            increased_at_time=int(raw.get("increased_at_time", 0)),
            decreased_at_time=int(raw.get("decreased_at_time", 0)),
        )
    except (ValueError, TypeError) as e:
        logger.debug("Failed to parse GMX V2 position: %s", e)
        return None
