"""On-chain ERC-4626 vault position reader via the vault adapter registry.

Reads share balance + converts to underlying asset amount for any vault
protocol registered with :mod:`almanak.connectors._strategy_base.vaults`. Used by
:class:`PortfolioValuer` to value VAULT positions from live on-chain state
instead of relying on stale strategy-reported values.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class VaultPositionOnChain:
    """On-chain state of an ERC-4626 vault position."""

    vault_address: str
    asset_address: str
    shares_wei: int
    asset_amount_wei: int
    asset_decimals: int

    @property
    def is_active(self) -> bool:
        return self.shares_wei > 0


class VaultPositionReader:
    """Reads ERC-4626 vault positions via the vault adapter registry.

    The reader is protocol-agnostic: any SDK exposing the standard ERC-4626
    methods (``get_vault_asset``, ``get_balance_of``, ``convert_to_assets``,
    ``get_decimals``) plugged into the registry works.
    """

    def __init__(self, gateway_client: object | None = None) -> None:
        self._gateway = gateway_client

    def set_gateway_client(self, gateway_client: object | None) -> None:
        self._gateway = gateway_client

    def read_position(
        self,
        *,
        protocol: str,
        chain: str,
        vault_address: str,
        wallet_address: str,
        token_resolver: object | None = None,
    ) -> VaultPositionOnChain | None:
        """Query share balance + convert to underlying assets.

        Returns None on any failure so the caller can fall back to a stale
        strategy-reported value rather than crashing the snapshot.
        """
        if self._gateway is None:
            return None

        try:
            from almanak.connectors._strategy_base.vaults import build_vault_adapter

            adapter = build_vault_adapter(
                protocol,
                chain=chain,
                wallet_address=wallet_address,
                gateway_client=self._gateway,
                token_resolver=token_resolver,  # type: ignore[arg-type]
            )
            sdk = adapter.sdk

            shares_wei = sdk.get_balance_of(vault_address, wallet_address)
            if shares_wei <= 0:
                return VaultPositionOnChain(
                    vault_address=vault_address,
                    asset_address="",
                    shares_wei=0,
                    asset_amount_wei=0,
                    asset_decimals=0,
                )

            asset_address = sdk.get_vault_asset(vault_address)
            asset_amount_wei = sdk.convert_to_assets(vault_address, shares_wei)
            asset_decimals = sdk.get_decimals(asset_address)

            return VaultPositionOnChain(
                vault_address=vault_address,
                asset_address=asset_address,
                shares_wei=shares_wei,
                asset_amount_wei=asset_amount_wei,
                asset_decimals=asset_decimals,
            )
        except ValueError:
            logger.warning(
                "Vault on-chain read skipped — unknown or misconfigured protocol=%s vault=%s",
                protocol,
                vault_address,
            )
            return None
        except Exception:
            logger.debug(
                "Vault on-chain read failed for protocol=%s vault=%s wallet=%s",
                protocol,
                vault_address,
                wallet_address,
                exc_info=True,
            )
            return None
