"""Lagoon Vault Adapter - Builds ActionBundles for vault operations.

This adapter converts vault operation params into ActionBundles that the
ExecutionOrchestrator can execute. It delegates transaction building to
the LagoonVaultSDK and wraps results in ActionBundle containers.

Example:
    from almanak.framework.connectors.lagoon import LagoonVaultSDK, LagoonVaultAdapter
    from almanak.core.models.params import UpdateTotalAssetsParams

    sdk = LagoonVaultSDK(gateway_client, chain="ethereum")
    adapter = LagoonVaultAdapter(sdk)

    params = UpdateTotalAssetsParams(
        vault_address="0x...",
        valuator_address="0x...",
        new_total_assets=1000000,
        pending_deposits=0,
    )
    bundle = adapter.build_propose_valuation_bundle(params)
"""

import logging

from almanak.core.enums import ActionType
from almanak.core.models.params import (
    SettleDepositParams,
    SettleRedeemParams,
    UpdateTotalAssetsParams,
)
from almanak.framework.connectors.lagoon.sdk import LagoonVaultSDK
from almanak.framework.models.reproduction_bundle import ActionBundle

logger = logging.getLogger(__name__)


class LagoonVaultAdapter:
    """Adapter that converts vault params into ActionBundles.

    The adapter builds ActionBundles from vault operation params using the
    LagoonVaultSDK to construct unsigned transactions. It does NOT execute
    transactions -- the VaultLifecycleManager passes bundles to the
    ExecutionOrchestrator for execution.

    Args:
        sdk: A LagoonVaultSDK instance for building transactions.
    """

    def __init__(self, sdk: LagoonVaultSDK, token_resolver=None):
        self._sdk = sdk
        self._token_resolver = token_resolver

    def build_propose_valuation_bundle(self, params: UpdateTotalAssetsParams) -> ActionBundle:
        """Build an ActionBundle for proposing a new vault valuation.

        Args:
            params: Parameters containing vault address, valuator address,
                and the proposed total assets value.

        Returns:
            ActionBundle with a single propose transaction.
        """
        tx = self._sdk.build_update_total_assets_tx(
            vault_address=params.vault_address,
            valuator_address=params.valuator_address,
            new_total_assets=params.new_total_assets,
        )
        return ActionBundle(
            intent_type=ActionType.PROPOSE_VAULT_VALUATION.value,
            transactions=[tx],
            metadata={
                "vault_address": params.vault_address,
                "new_total_assets": params.new_total_assets,
            },
        )

    def build_settle_deposit_bundle(self, params: SettleDepositParams) -> ActionBundle:
        """Build an ActionBundle for settling pending deposits.

        Args:
            params: Parameters containing vault address, safe address,
                and the total assets value for settlement.

        Returns:
            ActionBundle with a single settle deposit transaction.
        """
        tx = self._sdk.build_settle_deposit_tx(
            vault_address=params.vault_address,
            safe_address=params.safe_address,
            total_assets=params.total_assets,
        )
        return ActionBundle(
            intent_type=ActionType.SETTLE_VAULT_DEPOSIT.value,
            transactions=[tx],
            metadata={
                "vault_address": params.vault_address,
                "total_assets": params.total_assets,
            },
        )

    def build_settle_redeem_bundle(self, params: SettleRedeemParams) -> ActionBundle:
        """Build an ActionBundle for settling pending redemptions.

        Args:
            params: Parameters containing vault address, safe address,
                and the total assets value for settlement.

        Returns:
            ActionBundle with a single settle redeem transaction.
        """
        tx = self._sdk.build_settle_redeem_tx(
            vault_address=params.vault_address,
            safe_address=params.safe_address,
            total_assets=params.total_assets,
        )
        return ActionBundle(
            intent_type=ActionType.SETTLE_VAULT_REDEEM.value,
            transactions=[tx],
            metadata={
                "vault_address": params.vault_address,
                "total_assets": params.total_assets,
            },
        )
