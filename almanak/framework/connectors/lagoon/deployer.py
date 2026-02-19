"""Lagoon Vault Deployer - Deploy new Lagoon vaults via factory contracts.

Builds unsigned transactions that create new Lagoon vault proxies on-chain.
Uses the Lagoon OptinProxyFactory interface with proper ABI encoding.

Prerequisites:
- A Safe wallet must already exist (deployed via safe.global)
- The deployer EOA must have ETH for gas
- The factory's registry must have a default vault logic address

Example:
    from almanak.framework.connectors.lagoon.deployer import LagoonVaultDeployer, VaultDeployParams

    deployer = LagoonVaultDeployer(gateway_client)
    params = VaultDeployParams(
        chain="base",
        underlying_token_address="0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
        name="My Vault",
        symbol="mVLT",
        safe_address="0x...",
        admin_address="0x...",
        fee_receiver_address="0x...",
        deployer_address="0x...",
    )
    tx = deployer.build_deploy_vault_tx(params)
"""

import json
import logging
import os
from dataclasses import dataclass
from typing import Any

from eth_abi import encode as abi_encode

from almanak.framework.connectors.lagoon.sdk import _encode_address, _encode_uint256
from almanak.framework.models.reproduction_bundle import ActionBundle

logger = logging.getLogger(__name__)

# --- Constants ---

# createVaultProxy(address,address,uint256,(address,string,string,address,address,address,address,address,uint16,uint16,bool,uint256),bytes32)
CREATE_VAULT_PROXY_SELECTOR = "0xff592eb5"

# Function selectors for reading factory state
REGISTRY_SELECTOR = "0x7b103999"  # registry()
DEFAULT_LOGIC_SELECTOR = "0x39a51be5"  # defaultLogic()

# approve(address,uint256)
ERC20_APPROVE_SELECTOR = "0x095ea7b3"

MAX_UINT256 = (1 << 256) - 1

# Minimum timelock delay required by the factory (1 day)
MIN_DEPLOY_DELAY = 86400

# Event topics for vault deployment
# ProxyDeployed(address proxy, address deployer) -- non-indexed in data
PROXY_DEPLOYED_TOPIC = "0x3d2489efb661e8b1c3679865db649ca1de61d76a71184a1234de2e55786a6aad"

# Gas estimates
DEPLOY_GAS_ESTIMATE = 2_000_000
APPROVE_GAS_ESTIMATE = 60_000

# ABI type string for the InitStruct tuple
INIT_STRUCT_ABI_TYPE = "(address,string,string,address,address,address,address,address,uint16,uint16,bool,uint256)"

# Factory addresses sourced from Lagoon documentation (https://docs.lagoon.finance/resources/networks-and-addresses)
FACTORY_ADDRESSES: dict[str, str] = {
    "ethereum": "0x8D6f5479B14348186faE9BC7E636e947c260f9B1",
    "arbitrum": "0x9De724B0efEe0FbA07FE21a16B9Bf9bBb5204Fb4",
    "base": "0x6FC0F2320483fa03FBFdF626DDbAE2CC4B112b51",
    "avalanche": "0xC094C224ce0406BC338E00837B96aD2e265F7287",
    "sonic": "0x6FC0F2320483fa03FBFdF626DDbAE2CC4B112b51",
    "plasma": "0xF838E8Bd649fc6fBC48D44E9D87273c0519C45c9",
    "polygon": "0x0C0E287f6e4de685f4b44A5282A3ad4A29D05a91",
    "optimism": "0xA8E0684887b9475f8942DF6a89bEBa5B25219632",
}


# --- Data Models ---


@dataclass
class VaultDeployParams:
    """Parameters for deploying a new Lagoon vault."""

    chain: str
    underlying_token_address: str
    name: str
    symbol: str
    safe_address: str  # Pre-existing Safe (vault owner)
    admin_address: str  # Usually same as safe_address
    fee_receiver_address: str
    deployer_address: str  # EOA signing the factory call
    logic_address: str | None = None  # Vault implementation; read from registry if None
    valuation_manager_address: str | None = None  # Defaults to admin_address
    whitelist_manager_address: str | None = None  # Defaults to admin_address
    management_rate_bps: int = 200  # 2% (uint16, max 10000)
    performance_rate_bps: int = 2000  # 20% (uint16, max 10000)
    enable_whitelist: bool = False
    rate_update_cooldown: int = 86400  # 1 day in seconds
    deploy_delay: int = 86400  # Timelock delay (min 86400 = 1 day)
    salt: bytes | None = None  # Random 32 bytes if None


@dataclass
class VaultDeployResult:
    """Result of parsing a vault deployment receipt."""

    success: bool
    vault_address: str | None = None
    transaction_hash: str | None = None
    error: str | None = None


# --- Main Deployer Class ---


class LagoonVaultDeployer:
    """Deploys new Lagoon vaults via factory contracts.

    Uses the Lagoon OptinProxyFactory with proper ABI-encoded calldata.
    The factory function signature is:
        createVaultProxy(address logic, address owner, uint256 delay, InitStruct init, bytes32 salt)

    Where InitStruct is:
        (address underlying, string name, string symbol, address admin,
         address safe, address feeReceiver, address valuationManager,
         address whitelistManager, uint16 managementRate, uint16 performanceRate,
         bool enableWhitelist, uint256 rateUpdateCooldown)

    Args:
        gateway_client: Gateway client for RPC calls (needed to read vault logic from registry).
    """

    def __init__(self, gateway_client=None):
        self._gateway_client = gateway_client

    @staticmethod
    def get_factory_address(chain: str) -> str:
        """Look up the factory address for a chain.

        Returns:
            Factory contract address.

        Raises:
            ValueError: If chain has no known factory.
        """
        chain_lower = chain.lower()
        address = FACTORY_ADDRESSES.get(chain_lower)
        if address is None:
            supported = ", ".join(sorted(FACTORY_ADDRESSES.keys()))
            raise ValueError(f"No Lagoon factory configured for chain '{chain}'. Supported: {supported}")
        return address

    def build_deploy_vault_tx(self, params: VaultDeployParams) -> dict[str, Any]:
        """Build an unsigned transaction to deploy a new vault proxy.

        Uses eth_abi for proper ABI encoding of the createVaultProxy call.

        Args:
            params: Vault deployment parameters.

        Returns:
            Unsigned transaction dict with keys: to, from, data, value, gas_estimate.

        Raises:
            ValueError: If chain is unsupported or parameters are invalid.
        """
        factory_address = self.get_factory_address(params.chain)

        if params.deploy_delay < MIN_DEPLOY_DELAY:
            raise ValueError(f"deploy_delay must be >= {MIN_DEPLOY_DELAY} seconds (1 day), got {params.deploy_delay}")

        # Use explicit logic address, or address(0) to let the factory use its default.
        # Passing address(0) is more reliable than reading defaultLogic() from the
        # registry, which can be out of sync with the factory's allowed-logic set.
        logic_address = params.logic_address or "0x0000000000000000000000000000000000000000"

        calldata = self._encode_calldata(params, logic_address)

        return {
            "to": factory_address,
            "from": params.deployer_address,
            "data": calldata,
            "value": "0",
            "gas_estimate": DEPLOY_GAS_ESTIMATE,
        }

    def build_deploy_vault_bundle(self, params: VaultDeployParams) -> ActionBundle:
        """Build an ActionBundle for vault deployment.

        Args:
            params: Vault deployment parameters.

        Returns:
            ActionBundle wrapping the deploy transaction.
        """
        tx = self.build_deploy_vault_tx(params)
        return ActionBundle(
            intent_type="DEPLOY_LAGOON_VAULT",
            transactions=[tx],
            metadata={
                "chain": params.chain,
                "vault_name": params.name,
                "vault_symbol": params.symbol,
                "safe_address": params.safe_address,
                "underlying_token": params.underlying_token_address,
            },
        )

    @staticmethod
    def parse_deploy_receipt(receipt: dict[str, Any]) -> VaultDeployResult:
        """Parse a deployment transaction receipt to extract the vault address.

        Looks for ProxyDeployed event in logs. The vault address is in the
        event data (first 32 bytes), not in topics.

        Args:
            receipt: Transaction receipt dict with 'logs', 'transactionHash', 'status'.

        Returns:
            VaultDeployResult with extracted vault address.
        """
        tx_hash = receipt.get("transactionHash") or receipt.get("transaction_hash")
        status = receipt.get("status")

        # Check tx status (handle both int and hex)
        if isinstance(status, str):
            status_int = int(status, 16) if status.startswith("0x") else int(status)
        else:
            status_int = int(status) if status is not None else 0

        if status_int != 1:
            return VaultDeployResult(
                success=False,
                transaction_hash=tx_hash,
                error="Transaction reverted",
            )

        for log in receipt.get("logs", []):
            topics = log.get("topics", [])
            if not topics:
                continue
            topic0 = topics[0]
            if isinstance(topic0, bytes):
                topic0 = "0x" + topic0.hex()
            topic0 = str(topic0).lower()
            if topic0 == PROXY_DEPLOYED_TOPIC.lower():
                # Vault address is in data (first address in event data)
                data = log.get("data", "0x")
                if isinstance(data, bytes):
                    data = "0x" + data.hex()
                if len(data) >= 66:  # 0x + 64 hex chars
                    vault_address = "0x" + data[26:66]  # extract address from first 32-byte word
                    return VaultDeployResult(
                        success=True,
                        vault_address=vault_address,
                        transaction_hash=tx_hash,
                    )

        return VaultDeployResult(
            success=False,
            transaction_hash=tx_hash,
            error="No vault deployment event found in receipt logs",
        )

    @staticmethod
    def build_approve_underlying_tx(
        underlying_token_address: str,
        vault_address: str,
        safe_address: str,
        *,
        approval_amount: int | None = None,
    ) -> dict[str, Any]:
        """Build an ERC20 approve transaction for the Safe to allow vault redemptions.

        Post-deployment: the Safe calls ``underlying.approve(vault, amount)`` so the vault
        can pull tokens during redemption settlement.

        Security rationale for MAX_UINT256 default: This is the standard ERC-4626 vault
        approval pattern. The vault contract is the trust boundary -- if it is
        compromised, the approval amount is moot because the vault already has
        custody of deposited funds. A scoped approval would require re-approving
        before every settlement cycle, adding gas cost and operational complexity
        with no meaningful security improvement. Callers who want a tighter
        bound can pass ``approval_amount`` to cap the approval.

        Args:
            underlying_token_address: The ERC20 token the vault manages.
            vault_address: The newly deployed vault address (spender).
            safe_address: The Safe wallet address (token holder / tx sender).
            approval_amount: Optional cap on the approval amount. Defaults to
                MAX_UINT256 (standard ERC-4626 pattern).

        Returns:
            Unsigned transaction dict.
        """
        amount = approval_amount if approval_amount is not None else MAX_UINT256
        calldata = ERC20_APPROVE_SELECTOR + _encode_address(vault_address) + _encode_uint256(amount)
        return {
            "to": underlying_token_address,
            "from": safe_address,
            "data": calldata,
            "value": "0",
            "gas_estimate": APPROVE_GAS_ESTIMATE,
        }

    @staticmethod
    def build_post_deploy_bundle(
        underlying_token_address: str,
        vault_address: str,
        safe_address: str,
    ) -> ActionBundle:
        """Build an ActionBundle for post-deployment approval.

        Args:
            underlying_token_address: The ERC20 token the vault manages.
            vault_address: The newly deployed vault address.
            safe_address: The Safe wallet address.

        Returns:
            ActionBundle wrapping the approve transaction.
        """
        tx = LagoonVaultDeployer.build_approve_underlying_tx(underlying_token_address, vault_address, safe_address)
        return ActionBundle(
            intent_type="APPROVE_VAULT_UNDERLYING",
            transactions=[tx],
            metadata={
                "underlying_token": underlying_token_address,
                "vault_address": vault_address,
                "safe_address": safe_address,
            },
        )

    def get_default_logic(self, chain: str, factory_address: str | None = None) -> str:
        """Read the default vault logic address from the factory's registry.

        Calls registry() on the factory, then defaultLogic() on the registry.

        Args:
            chain: Chain identifier.
            factory_address: Override factory address (uses chain default if None).

        Returns:
            Default vault implementation address.

        Raises:
            RuntimeError: If gateway client is not configured or RPC call fails.
        """
        if self._gateway_client is None:
            raise RuntimeError("Gateway client required for get_default_logic()")

        if factory_address is None:
            factory_address = self.get_factory_address(chain)

        # Step 1: Read registry address from factory
        registry_address = self._rpc_call(chain, factory_address, REGISTRY_SELECTOR)

        # Step 2: Read defaultLogic from registry
        logic_address = self._rpc_call(chain, registry_address, DEFAULT_LOGIC_SELECTOR)

        logger.info("Resolved vault logic: registry=%s, logic=%s", registry_address, logic_address)
        return logic_address

    def _rpc_call(self, chain: str, to: str, selector: str) -> str:
        """Make an eth_call and decode the result as an address."""
        from almanak.gateway.proto import gateway_pb2

        request = gateway_pb2.RpcRequest(
            chain=chain.lower(),
            method="eth_call",
            params=json.dumps([{"to": to, "data": selector}, "latest"]),
            id=f"lagoon_{selector}",
        )
        response = self._gateway_client.rpc.Call(request, timeout=30.0)
        if not response.success:
            error_msg = response.error if response.error else "Unknown RPC error"
            raise RuntimeError(f"RPC call failed (to={to}, selector={selector}): {error_msg}")

        result = json.loads(response.result)
        raw = result.removeprefix("0x")
        return "0x" + raw[-40:]

    # --- Private Calldata Encoding ---

    @staticmethod
    def _encode_calldata(params: VaultDeployParams, logic_address: str) -> str:
        """Encode calldata for createVaultProxy using eth_abi.

        Function: createVaultProxy(
            address logic,
            address owner,
            uint256 delay,
            InitStruct init,
            bytes32 salt
        )

        Returns:
            Full calldata hex string with 0x prefix.
        """
        valuation_mgr = params.valuation_manager_address or params.admin_address
        whitelist_mgr = params.whitelist_manager_address or params.admin_address

        # Build the InitStruct tuple -- field order must match Lagoon v0.5.0 Vault.sol:
        # struct InitStruct { IERC20 underlying, string name, string symbol,
        #   address safe, address whitelistManager, address valuationManager,
        #   address admin, address feeReceiver,
        #   uint16 managementRate, uint16 performanceRate,
        #   bool enableWhitelist, uint256 rateUpdateCooldown }
        init_struct = (
            params.underlying_token_address,  # IERC20 underlying
            params.name,  # string name
            params.symbol,  # string symbol
            params.safe_address,  # address safe
            whitelist_mgr,  # address whitelistManager
            valuation_mgr,  # address valuationManager
            params.admin_address,  # address admin
            params.fee_receiver_address,  # address feeReceiver
            params.management_rate_bps,  # uint16 managementRate
            params.performance_rate_bps,  # uint16 performanceRate
            params.enable_whitelist,  # bool enableWhitelist
            params.rate_update_cooldown,  # uint256 rateUpdateCooldown
        )

        # Generate salt
        if params.salt is not None:
            if len(params.salt) != 32:
                raise ValueError(f"Salt must be exactly 32 bytes, got {len(params.salt)}")
            salt = params.salt
        else:
            salt = os.urandom(32)

        # ABI-encode all parameters
        encoded = abi_encode(
            ["address", "address", "uint256", INIT_STRUCT_ABI_TYPE, "bytes32"],
            [logic_address, params.safe_address, params.deploy_delay, init_struct, salt],
        )

        return CREATE_VAULT_PROXY_SELECTOR + encoded.hex()
