"""Fluid vault (NFT-CDP) SDK — VaultResolver reads + ``operate()`` calldata.

Phase 3 (VIB-5031). Kept separate from ``sdk.py`` (DEX/fToken-scoped) so the
vault surface stays self-contained. Every constant below is pinned from the
on-chain verification report
``docs/internal/qa/fluid-vault-verification-2026-06-12.md`` (full bytes,
byte-verified against live arbitrum + base forks AND
``Instadapp/fluid-contracts-public`` @ main):

- VaultResolver ``0xA5C3E16523eeeDDcC34706b0E6bE88b4c6EA95cC`` (both chains).
- ``positionsByUser(address)`` selector ``0x347ca8bb`` →
  ``(UserPosition[], VaultEntireData[])`` — index-aligned arrays, NO
  pagination (cost bounded by the one-NFT-per-(wallet,vault) invariant).
- ``positionByNftId(uint256)`` selector ``0x144128e8`` (12 + 97 = 109 words).
- ``getVaultEntireData(address)`` selector ``0x09c062e2`` (97 static words).
- ``getVaultAddress(uint256)`` selector ``0xe6bd26a2``.
- ``operate(uint256,int256,int256,address)`` selector ``0x032d2276``.

Decoding is TYPED-ABI ONLY (V3.3 lesson — never word-offset arithmetic):
the gateway-routed path decodes through web3's contract ABI; the pure
strategy-side reducer (``vault_lending_read``) decodes the same blobs via
``eth_abi`` against the flattened type strings exported here. A unit test
pins the web3-derived selectors against the verified bytes, so any ABI
drift fails loudly.

Transport mirrors Phase 1/2 (``FluidSDK``): gateway client preferred
(production path, ``GatewayWeb3Provider``); direct ``rpc_url`` only for
ad-hoc scripts and tests.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from web3 import Web3
from web3.providers import HTTPProvider

from almanak.connectors.fluid.sdk import FLUID_ADDRESSES, FluidSDKError

if TYPE_CHECKING:
    from almanak.framework.gateway_client import GatewayClient

logger = logging.getLogger(__name__)

#: ``type(int256).min`` — the protocol's "max" sentinel for full withdraw /
#: full repay legs of ``operate()``. The vault resolves the exact amount
#: AT EXECUTION TIME, so interest accrued between compile and execute is
#: structurally covered (ADR §2.3). The ONLY compile paths to this value
#: are ``repay_full=True`` and ``withdraw_all=True``.
INT256_MIN = -(2**255)

#: Phase-0/verification measured ``operate()`` gas: open+borrow = 410,538.
#: Conservative ceiling — the execution pipeline re-estimates pre-submission.
DEFAULT_OPERATE_GAS = 900_000

# =============================================================================
# Typed ABI (verification report D1 — byte-verified flattened signatures)
# =============================================================================

#: ``UserPosition`` — 12 static words. Fields 9/10 (``supply``/``borrow``)
#: are already exchange-price-scaled TOKEN amounts (not raw big-numbers);
#: ``dustBorrow`` is already netted out of ``borrow`` (report note 4).
USER_POSITION_TYPE = "(uint256,address,bool,bool,int256,uint256,uint256,uint256,uint256,uint256,uint256,uint256)"

#: ``VaultEntireData`` — 97 static words (3 + 18 + 13 + 14 + 6 + 8 + 13 + 11 + 11).
VAULT_ENTIRE_DATA_TYPE = (
    "(address,bool,bool,"
    # constantVariables (18 words)
    "(address,address,address,address,address,address,address,address,"
    "(address,address),(address,address),uint256,uint256,bytes32,bytes32,bytes32,bytes32),"
    # configs (13 words)
    "(uint16,uint16,uint16,uint16,uint16,uint16,uint16,uint16,address,uint256,uint256,address,uint256),"
    # exchangePricesAndRates (14 words)
    "(uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256,int256,int256,int256,int256),"
    # totalSupplyAndBorrow (6 words)
    "(uint256,uint256,uint256,uint256,uint256,uint256),"
    # limitsAndAvailability (8 words)
    "(uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256),"
    # vaultState (13 words)
    "(uint256,int256,uint256,uint256,uint256,uint256,(uint256,int256,uint256,uint256,uint256,uint256,int256)),"
    # liquidityUserSupplyData (11 words)
    "(bool,uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256),"
    # liquidityUserBorrowData (11 words)
    "(bool,uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256))"
)


def _components_from_type(type_str: str) -> list[dict[str, Any]]:
    """Build web3 ABI ``components`` from a flattened canonical tuple type.

    Splitting the SINGLE pinned type string into web3 components (instead of
    hand-writing a parallel components tree) guarantees the web3 path and the
    pure ``eth_abi`` path decode the exact same shape — the selector pin test
    then byte-verifies both at once.
    """
    assert type_str.startswith("(") and type_str.endswith(")")
    inner = type_str[1:-1]
    parts: list[str] = []
    depth = 0
    current = ""
    for ch in inner:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append(current)
            current = ""
        else:
            current += ch
    if current:
        parts.append(current)
    components: list[dict[str, Any]] = []
    for i, part in enumerate(parts):
        if part.startswith("("):
            components.append({"name": f"f{i}", "type": "tuple", "components": _components_from_type(part)})
        else:
            components.append({"name": f"f{i}", "type": part})
    return components


_USER_POSITION_COMPONENTS = _components_from_type(USER_POSITION_TYPE)
_VAULT_ENTIRE_DATA_COMPONENTS = _components_from_type(VAULT_ENTIRE_DATA_TYPE)

VAULT_RESOLVER_ABI = [
    {
        "inputs": [{"name": "vault_", "type": "address"}],
        "name": "getVaultEntireData",
        "outputs": [{"name": "vaultData_", "type": "tuple", "components": _VAULT_ENTIRE_DATA_COMPONENTS}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"name": "nftId_", "type": "uint256"}],
        "name": "positionByNftId",
        "outputs": [
            {"name": "userPosition_", "type": "tuple", "components": _USER_POSITION_COMPONENTS},
            {"name": "vaultData_", "type": "tuple", "components": _VAULT_ENTIRE_DATA_COMPONENTS},
        ],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"name": "user_", "type": "address"}],
        "name": "positionsByUser",
        "outputs": [
            {"name": "userPositions_", "type": "tuple[]", "components": _USER_POSITION_COMPONENTS},
            {"name": "vaultsData_", "type": "tuple[]", "components": _VAULT_ENTIRE_DATA_COMPONENTS},
        ],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"name": "vaultId_", "type": "uint256"}],
        "name": "getVaultAddress",
        "outputs": [{"name": "vault_", "type": "address"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"name": "vault_", "type": "address"}],
        "name": "getVaultType",
        "outputs": [{"name": "vaultType_", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
]

#: VaultT1 single lifecycle entrypoint (selector ``0x032d2276``). ``payable``
#: because native-collateral vaults take raw ETH as ``msg.value``.
VAULT_T1_OPERATE_ABI = [
    {
        "inputs": [
            {"name": "nftId_", "type": "uint256"},
            {"name": "newCol_", "type": "int256"},
            {"name": "newDebt_", "type": "int256"},
            {"name": "to_", "type": "address"},
        ],
        "name": "operate",
        "outputs": [
            {"name": "nftId_", "type": "uint256"},
            {"name": "newCol_", "type": "int256"},
            {"name": "newDebt_", "type": "int256"},
        ],
        "stateMutability": "payable",
        "type": "function",
    },
]

#: ``operate(uint256,int256,int256,address)`` — pinned, byte-verified.
OPERATE_SELECTOR = "0x032d2276"


# =============================================================================
# Decoded shapes
# =============================================================================


@dataclass(frozen=True)
class FluidVaultPosition:
    """One decoded ``UserPosition`` (+ the vault it belongs to, when paired).

    ``supply`` / ``borrow`` are exchange-price-scaled TOKEN amounts in the
    vault pair's native base units (verification report note 4); ``borrow``
    already has ``dust_borrow`` netted out.
    """

    nft_id: int
    owner: str
    is_liquidated: bool
    is_supply_position: bool
    tick: int
    tick_id: int
    supply: int
    borrow: int
    dust_borrow: int
    #: Lowercased vault address from the index-aligned ``VaultEntireData``
    #: (``""`` when the position was decoded without its paired vault data).
    vault: str = ""


@dataclass(frozen=True)
class FluidVaultData:
    """The ``VaultEntireData`` fields the compiler / lending read consume."""

    vault: str  # lowercased
    is_smart_col: bool
    is_smart_debt: bool
    supply_token: str  # token0 of the supply pair (native sentinel for raw-ETH vaults)
    borrow_token: str  # token0 of the borrow pair
    vault_id: int
    vault_type: int  # 10000 == VAULT_T1_TYPE
    collateral_factor: int  # bps
    liquidation_threshold: int  # bps — the ratio liquidation actually keys on
    liquidation_max_limit: int  # bps
    liquidation_penalty: int  # bps
    oracle: str
    oracle_price_operate: int  # 1e27-scaled collateral->debt exchange rate
    oracle_price_liquidate: int
    withdrawable: int  # limitsAndAvailability.withdrawable (token base units)
    borrowable: int  # limitsAndAvailability.borrowable (token base units)
    total_supply_vault: int
    total_borrow_vault: int


def position_from_tuple(raw: Any, vault: str = "") -> FluidVaultPosition:
    """Map a decoded 12-field ``UserPosition`` tuple onto the dataclass."""
    return FluidVaultPosition(
        nft_id=int(raw[0]),
        owner=str(raw[1]),
        is_liquidated=bool(raw[2]),
        is_supply_position=bool(raw[3]),
        tick=int(raw[4]),
        tick_id=int(raw[5]),
        supply=int(raw[9]),
        borrow=int(raw[10]),
        dust_borrow=int(raw[11]),
        vault=vault.lower(),
    )


def vault_data_from_tuple(raw: Any) -> FluidVaultData:
    """Map a decoded 97-word ``VaultEntireData`` tuple onto the dataclass."""
    constants = raw[3]
    configs = raw[4]
    totals = raw[6]
    limits = raw[7]
    return FluidVaultData(
        vault=str(raw[0]).lower(),
        is_smart_col=bool(raw[1]),
        is_smart_debt=bool(raw[2]),
        supply_token=str(constants[8][0]),
        borrow_token=str(constants[9][0]),
        vault_id=int(constants[10]),
        vault_type=int(constants[11]),
        collateral_factor=int(configs[2]),
        liquidation_threshold=int(configs[3]),
        liquidation_max_limit=int(configs[4]),
        liquidation_penalty=int(configs[6]),
        oracle=str(configs[8]),
        oracle_price_operate=int(configs[9]),
        oracle_price_liquidate=int(configs[10]),
        withdrawable=int(limits[2]),
        borrowable=int(limits[5]),
        total_supply_vault=int(totals[0]),
        total_borrow_vault=int(totals[1]),
    )


# =============================================================================
# FluidVaultSDK
# =============================================================================


class FluidVaultSDK:
    """Low-level Fluid vault (NFT-CDP) protocol SDK.

    All reads go through the VaultResolver with typed-ABI decoding; writes
    are offline calldata builders for the vault's ``operate()`` entrypoint.

    Args:
        chain: Chain name (one of ``FLUID_ADDRESSES``).
        rpc_url: DEPRECATED — direct RPC URL for ad-hoc scripts/tests only.
        gateway_client: Gateway client routing all eth_call traffic through
            the gateway's RpcService. Preferred for production code paths.
    """

    def __init__(
        self,
        chain: str,
        rpc_url: str | None = None,
        gateway_client: GatewayClient | None = None,
    ) -> None:
        chain_lower = chain.lower()
        if chain_lower not in FLUID_ADDRESSES:
            raise FluidSDKError(
                f"Fluid vaults not supported on chain: {chain}. Supported: {list(FLUID_ADDRESSES.keys())}"
            )
        if rpc_url is None and gateway_client is None:
            raise FluidSDKError("FluidVaultSDK requires either rpc_url (deprecated) or gateway_client")
        if gateway_client is not None and not getattr(gateway_client, "is_connected", False):
            # Fail fast here rather than building a GatewayWeb3Provider that
            # errors opaquely on the first eth_call (polymarket
            # GatewayPolymarketClient precedent).
            raise FluidSDKError(
                "FluidVaultSDK was given a gateway_client that is not connected — "
                "connect the gateway client before constructing the SDK"
            )

        self.chain = chain_lower
        self.rpc_url = rpc_url
        self._gateway_client = gateway_client
        if gateway_client is not None:
            from almanak.framework.web3.gateway_provider import GatewayWeb3Provider

            self.w3 = Web3(GatewayWeb3Provider(gateway_client, chain=chain_lower))
        else:
            self.w3 = Web3(HTTPProvider(rpc_url))  # vib-2986-exempt: gateway-internal fallback
        self._addresses = FLUID_ADDRESSES[chain_lower]
        self._resolver = self.w3.eth.contract(
            address=Web3.to_checksum_address(self._addresses["vault_resolver"]),
            abi=VAULT_RESOLVER_ABI,
        )

    # =========================================================================
    # Resolver reads (typed-ABI decode only)
    # =========================================================================

    def get_vault_entire_data(self, vault: str) -> FluidVaultData:
        """``VaultResolver.getVaultEntireData(vault)`` — 97-word typed decode."""
        try:
            raw = self._resolver.functions.getVaultEntireData(Web3.to_checksum_address(vault)).call()
        except Exception as e:
            raise FluidSDKError(f"Failed to read Fluid vault data for {vault}: {e}") from e
        return vault_data_from_tuple(raw)

    def position_by_nft_id(self, nft_id: int) -> tuple[FluidVaultPosition, FluidVaultData]:
        """``VaultResolver.positionByNftId(nftId)`` — (12 + 97)-word typed decode."""
        try:
            raw_position, raw_vault = self._resolver.functions.positionByNftId(int(nft_id)).call()
        except Exception as e:
            raise FluidSDKError(f"Failed to read Fluid vault position for nftId={nft_id}: {e}") from e
        vault_data = vault_data_from_tuple(raw_vault)
        return position_from_tuple(raw_position, vault=vault_data.vault), vault_data

    def positions_by_user(self, wallet: str) -> list[FluidVaultPosition]:
        """``VaultResolver.positionsByUser(wallet)`` — every position with its vault.

        The two returned arrays are index-aligned (position i ↔ its vault's
        data at i — verification report D1), so each position carries its
        lowercased vault address. No pagination exists; cost is bounded by
        the one-NFT-per-(wallet,vault) invariant.
        """
        try:
            raw_positions, raw_vaults = self._resolver.functions.positionsByUser(
                Web3.to_checksum_address(wallet)
            ).call()
        except Exception as e:
            raise FluidSDKError(f"Failed to enumerate Fluid vault positions for {wallet}: {e}") from e
        if len(raw_positions) != len(raw_vaults):
            # The resolver contract guarantees alignment; a mismatch means a
            # truncated/foreign response — fail closed, never zip-and-drop.
            raise FluidSDKError(
                f"positionsByUser returned misaligned arrays ({len(raw_positions)} positions, "
                f"{len(raw_vaults)} vaults) for {wallet} — refusing to decode"
            )
        return [
            position_from_tuple(raw_position, vault=str(raw_vault[0]))
            for raw_position, raw_vault in zip(raw_positions, raw_vaults, strict=True)
        ]

    def resolve_user_nft_for_vault(self, wallet: str, vault: str) -> int | None:
        """Resolve the wallet's nftId on ``vault`` fresh from chain state.

        The VIB-5010 answer: chain is the source of truth — persisted nftIds
        are never load-bearing. Returns ``None`` when the wallet holds no
        position NFT on the vault (the measured "no position" answer —
        distinct from a read failure, which RAISES ``FluidSDKError`` so the
        compiler fails closed instead of minting a duplicate position).

        If the wallet somehow holds MULTIPLE NFTs on one vault (user acted
        outside the SDK), selection is deterministic: lowest nftId wins,
        with a warning — the others stay invisible by design (ADR §1.1).
        """
        vault_lower = vault.lower()
        matching = sorted(
            position.nft_id for position in self.positions_by_user(wallet) if position.vault == vault_lower
        )
        if not matching:
            return None
        if len(matching) > 1:
            logger.warning(
                "Wallet %s holds %d Fluid NFTs on vault %s (%s) — selecting lowest nftId %d; "
                "the others are invisible to accounting by design (one-NFT-per-vault invariant)",
                wallet,
                len(matching),
                vault_lower,
                matching,
                matching[0],
            )
        return matching[0]

    # =========================================================================
    # operate() calldata (offline — no RPC interaction)
    # =========================================================================

    def encode_operate_calldata(self, nft_id: int, col_delta: int, debt_delta: int, to: str) -> str:
        """ABI-encode ``operate(nftId, newCol, newDebt, to)`` calldata.

        Signed deltas: positive = deposit/borrow, negative = withdraw/repay,
        ``INT256_MIN`` = the protocol max sentinel (full withdraw/repay).
        """
        contract = Web3().eth.contract(abi=VAULT_T1_OPERATE_ABI)
        return contract.encode_abi(
            "operate",
            args=[int(nft_id), int(col_delta), int(debt_delta), Web3.to_checksum_address(to)],
        )

    def build_operate_tx(
        self,
        vault: str,
        nft_id: int,
        col_delta: int,
        debt_delta: int,
        to: str,
        value: int = 0,
    ) -> dict[str, Any]:
        """Build an ``operate()`` transaction for a Fluid type-1 vault.

        Args:
            vault: Vault contract address (the per-market operate target).
            nft_id: Position NFT id (0 mints a new position).
            col_delta: Signed collateral delta (raw units; INT256_MIN = all).
            debt_delta: Signed debt delta (raw units; INT256_MIN = full repay).
            to: Recipient of withdrawn collateral / borrowed debt.
            value: msg.value — MUST equal ``col_delta`` for native-collateral
                deposits; 0 for every ERC-20 leg.
        """
        return {
            "to": Web3.to_checksum_address(vault),
            "data": self.encode_operate_calldata(nft_id, col_delta, debt_delta, to),
            "value": value,
            "gas": DEFAULT_OPERATE_GAS,
        }


__all__ = [
    "DEFAULT_OPERATE_GAS",
    "INT256_MIN",
    "OPERATE_SELECTOR",
    "USER_POSITION_TYPE",
    "VAULT_ENTIRE_DATA_TYPE",
    "VAULT_RESOLVER_ABI",
    "VAULT_T1_OPERATE_ABI",
    "FluidVaultData",
    "FluidVaultPosition",
    "FluidVaultSDK",
    "position_from_tuple",
    "vault_data_from_tuple",
]
