"""Polymarket CTF (Conditional Token Framework) On-Chain SDK.

Provides on-chain interaction with the Gnosis Conditional Token Framework
for position management, token approvals, and redemption operations.

Polymarket uses a hybrid architecture:
- Off-chain CLOB for order matching (see clob_client.py)
- On-chain CTF for token ownership and settlement (this module)

Key Contract Addresses (Polygon Mainnet — V2):
- CTF Exchange V2:    0xE111180000d2663C0091e4f400237545B87B996B
- NegRisk Exchange V2: 0xe2222d279d744050d28e00520010520000310F59
- NegRisk Adapter:    0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296
- Conditional Tokens: 0x4D97DCd97eC945f40cF65F87097ACe5EA0476045
- pUSD (V2 collateral): 0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB
- CollateralOnramp:   0x93070a847efEf7F70739046A929D47a521F5B8ee
- CollateralOfframp:  0x2957922Eb93258b93368531d39fAcCA3B4dC5854
- USDC.e (source):    0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174

Example:
    from almanak.connectors.polymarket import CtfSDK
    from almanak.framework.gateway_client import GatewayClient
    from almanak.framework.web3.gateway_provider import GatewayWeb3Provider
    from web3 import Web3

    gateway_client = GatewayClient()
    gateway_client.connect()
    web3 = Web3(GatewayWeb3Provider(gateway_client, chain="polygon"))
    sdk = CtfSDK()

    # Idempotent V2 5-tx approval set (source→Onramp, pUSD→exchanges, CTF→exchanges)
    for tx in sdk.ensure_allowances(wallet_address, web3):
        ...  # sign + submit

    # Wrap source asset (USDC.e) to pUSD before trading
    wrap_tx = sdk.build_wrap_to_pusd_tx(wallet_address, amount)

    # Redeem winnings
    resolution = sdk.get_condition_resolution(condition_id, web3)
    if resolution.is_resolved:
        tx = sdk.build_redeem_tx(condition_id, [1, 2], wallet_address)
"""

import json
import logging
import os
from dataclasses import dataclass
from typing import Any

from eth_abi import encode as abi_encode
from hexbytes import HexBytes
from web3 import Web3

from .models import (
    COLLATERAL_OFFRAMP,
    COLLATERAL_ONRAMP,
    CONDITIONAL_TOKENS,
    CTF_EXCHANGE_V2,
    NEG_RISK_ADAPTER,
    NEG_RISK_EXCHANGE_V2,
    POLYGON_CHAIN_ID,
    PUSD,
    USDC_NATIVE_POLYGON,
    USDCE_POLYGON,
)

logger = logging.getLogger(__name__)


# =============================================================================
# Constants
# =============================================================================

# Maximum uint256 for unlimited approvals
MAX_UINT256 = 2**256 - 1

# Threshold above which an ERC-20 allowance is treated as "infinite" (i.e.
# the wallet has applied a MAX_UINT256 approval and we don't need to re-submit).
# Picked as MAX_UINT256 // 2: realistic order flow can never drag a full MAX
# approval below this point, but a dust allowance (e.g. a leftover 1-wei from a
# pre-existing partial approval) is below it — so ``ensure_allowances`` will
# correctly re-issue the MAX approval rather than skip it. Without this
# guard, ``allowance > 0`` would mark a dust allowance as "ready" and the
# next wrap or order would revert as soon as it spent past that dust amount.
SUFFICIENT_ALLOWANCE_THRESHOLD = MAX_UINT256 // 2

# Zero bytes32 (root parent collection)
ZERO_BYTES32 = b"\x00" * 32

# Index sets for binary markets
# YES = 1 (0b01), NO = 2 (0b10)
INDEX_SET_YES = 1
INDEX_SET_NO = 2
BINARY_PARTITION = [INDEX_SET_YES, INDEX_SET_NO]

# Gas estimates for CTF operations
# Note: Polygon USDC.e is a proxy contract that requires ~58k gas for approve
# We use 80k as a safe margin for ERC20/ERC1155 approvals on proxy contracts
GAS_ESTIMATES = {
    "approve_erc20": 80_000,
    "approve_erc1155": 80_000,
    "split_position": 150_000,
    "merge_positions": 150_000,
    "redeem_positions": 200_000,
    # V2 collateral ramp ops
    "wrap": 150_000,
    "unwrap": 150_000,
}


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class TransactionData:
    """Transaction data for on-chain operations.

    Attributes:
        to: Contract address to call
        data: Encoded function call data
        value: ETH value to send (usually 0)
        gas_estimate: Estimated gas for the transaction
        description: Human-readable description
    """

    to: str
    data: str
    value: int = 0
    gas_estimate: int = 100_000
    description: str = ""

    def to_tx_params(self, sender: str) -> dict[str, Any]:
        """Convert to web3 transaction parameters.

        Args:
            sender: Transaction sender address

        Returns:
            Dict with transaction parameters for web3
        """
        return {
            "from": sender,
            "to": Web3.to_checksum_address(self.to),
            "data": HexBytes(self.data),
            "value": self.value,
            "gas": self.gas_estimate,
        }


@dataclass
class AllowanceStatus:
    """Status of token allowances for Polymarket V2 trading.

    V2 collateral pivot: spending collateral is pUSD (minted via the
    CollateralOnramp from a source asset — USDC.e or native USDC). The
    approval set is:

      Source assets (only the wallet's funded source assets are approved
      lazily — we don't burn gas approving an asset the wallet has never
      held):
        - configured ``source_asset`` (default USDC.e) → CollateralOnramp
        - native USDC → CollateralOnramp (when wallet holds native USDC)

      pUSD spend (the BUY-side leg — Polymarket V2 pulls pUSD from the maker
      via these contracts on order fill):
        - pUSD → CTF Exchange V2           (binary YES/NO market BUYs)
        - pUSD → NegRisk Exchange V2       (neg-risk order matching)
        - pUSD → NegRisk Adapter           (neg-risk split/merge — the adapter
                                             is the actual spender on fills)

      CTF (ERC-1155) operator (the SELL-side leg — V2 exchanges pull shares):
        - CTF.setApprovalForAll(CTF Exchange V2)
        - CTF.setApprovalForAll(NegRisk Adapter)

    VIB-3770: ``native_usdc_balance`` / ``native_usdc_allowance_onramp`` were
    added so a wallet funded with native Circle USDC instead of USDC.e (the
    Polymarket UI now accepts both as deposit) can wrap to pUSD without the
    user manually swapping first.
    """

    source_asset_balance: int
    pusd_balance: int
    source_asset_allowance_onramp: int
    pusd_allowance_ctf_exchange: int
    pusd_allowance_neg_risk_exchange: int
    pusd_allowance_neg_risk_adapter: int
    ctf_approved_for_ctf_exchange: bool
    ctf_approved_for_neg_risk_adapter: bool
    # VIB-3770: native USDC tracked alongside the bridged USDC.e source asset.
    native_usdc_balance: int = 0
    native_usdc_allowance_onramp: int = 0

    @property
    def source_asset_approved_onramp(self) -> bool:
        """Check if the source asset (USDC.e / USDC) is approved for the Onramp.

        Sufficiency rather than non-zero — see ``SUFFICIENT_ALLOWANCE_THRESHOLD``.
        """
        return self.source_asset_allowance_onramp >= SUFFICIENT_ALLOWANCE_THRESHOLD

    @property
    def native_usdc_approved_onramp(self) -> bool:
        """Whether native Circle USDC is approved for the Onramp (sufficiency)."""
        return self.native_usdc_allowance_onramp >= SUFFICIENT_ALLOWANCE_THRESHOLD

    @property
    def pusd_approved_ctf_exchange(self) -> bool:
        """Check if pUSD is approved for CTF Exchange V2 (sufficiency, not >0)."""
        return self.pusd_allowance_ctf_exchange >= SUFFICIENT_ALLOWANCE_THRESHOLD

    @property
    def pusd_approved_neg_risk_exchange(self) -> bool:
        """Check if pUSD is approved for NegRisk Exchange V2 (sufficiency, not >0)."""
        return self.pusd_allowance_neg_risk_exchange >= SUFFICIENT_ALLOWANCE_THRESHOLD

    @property
    def pusd_approved_neg_risk_adapter(self) -> bool:
        """Check if pUSD is approved for the NegRisk Adapter (sufficiency, not >0)."""
        return self.pusd_allowance_neg_risk_adapter >= SUFFICIENT_ALLOWANCE_THRESHOLD

    @property
    def fully_approved(self) -> bool:
        """Check if all V2 approvals are in place (6-tx set fully applied)."""
        return (
            self.source_asset_approved_onramp
            and self.pusd_approved_ctf_exchange
            and self.pusd_approved_neg_risk_exchange
            and self.pusd_approved_neg_risk_adapter
            and self.ctf_approved_for_ctf_exchange
            and self.ctf_approved_for_neg_risk_adapter
        )


@dataclass(frozen=True)
class CollateralBreakdown:
    """Per-asset on-chain balance breakdown for Polymarket V2 collateral.

    VIB-3770: surfaces every source the gateway will consider for wrap →
    pUSD plus the spendable pUSD. Used by:

    - ``CtfSDK.select_source_for_wrap`` to pick a wrap input that actually
      has the on-chain balance to cover a BUY's pUSD deficit.
    - The ``almanak ax balance --protocol polymarket`` UX: shows pUSD,
      USDC.e, native USDC, and the total spendable collateral side-by-side
      so users see exactly what's usable.

    Attributes:
        pusd: pUSD balance (the spendable trading collateral) in 6-dp units.
        usdce: Bridged USDC.e balance (legacy source asset) in 6-dp units.
        usdc_native: Native Circle USDC balance in 6-dp units.
        pusd_address: pUSD contract address (Polygon).
        usdce_address: USDC.e contract address (Polygon).
        usdc_native_address: Native USDC contract address (Polygon).
    """

    pusd: int
    usdce: int
    usdc_native: int
    pusd_address: str
    usdce_address: str
    usdc_native_address: str

    @property
    def total_spendable(self) -> int:
        """pUSD + every wrappable source asset, in 6-dp units.

        This is the amount the wallet can ultimately bring to bear on a
        Polymarket BUY (pUSD spends directly; USDC.e / native USDC become
        pUSD via a single Onramp wrap). Treats the three at parity because
        the Onramp enforces 1:1.
        """
        return self.pusd + self.usdce + self.usdc_native


@dataclass
class ResolutionStatus:
    """Resolution status of a condition.

    Attributes:
        condition_id: The condition ID (bytes32 hex string)
        is_resolved: Whether the condition has been resolved
        payout_denominator: Denominator for payout calculation
        payout_numerators: List of payout numerators for each outcome
        winning_outcome: Index of winning outcome (0=YES, 1=NO) or None if not resolved
    """

    condition_id: str
    is_resolved: bool
    payout_denominator: int
    payout_numerators: list[int]
    winning_outcome: int | None = None


# =============================================================================
# CTF SDK
# =============================================================================


class CtfSDK:
    """Low-level SDK for Polymarket CTF on-chain operations.

    This SDK provides methods to:
    - Check and set token approvals
    - Query token balances
    - Build split, merge, and redeem transactions
    - Check condition resolution status

    All transaction building methods return TransactionData objects that
    can be signed and submitted using a signer.

    Example:
        from almanak.framework.gateway_client import GatewayClient
        gateway_client = GatewayClient()
        gateway_client.connect()

        sdk = CtfSDK()
        # Production: use GatewayWeb3Provider so RPC calls go through
        # the gateway sidecar, not directly to the chain.
        web3 = Web3(GatewayWeb3Provider(gateway_client, chain="polygon"))

        # Check if wallet needs approvals
        status = sdk.check_allowances("0x...", web3)

        if not status.pusd_approved_ctf_exchange:
            tx = sdk.build_approve_collateral_tx(PUSD, CTF_EXCHANGE_V2, "0x...")
            # Sign and submit tx...

        # Build redeem transaction for resolved market
        tx = sdk.build_redeem_tx(
            condition_id="0x...",
            index_sets=[1, 2],
            sender="0x...",
        )
    """

    def __init__(
        self,
        chain_id: int = POLYGON_CHAIN_ID,
        ctf_exchange: str = CTF_EXCHANGE_V2,
        neg_risk_exchange: str = NEG_RISK_EXCHANGE_V2,
        conditional_tokens: str = CONDITIONAL_TOKENS,
        neg_risk_adapter: str = NEG_RISK_ADAPTER,
        pusd: str = PUSD,
        collateral_onramp: str = COLLATERAL_ONRAMP,
        collateral_offramp: str = COLLATERAL_OFFRAMP,
        source_asset: str = USDCE_POLYGON,
        native_usdc: str = USDC_NATIVE_POLYGON,
    ) -> None:
        """Initialize the CTF SDK.

        Args:
            chain_id: Chain ID (default: Polygon 137)
            ctf_exchange: CTF Exchange V2 contract address
            neg_risk_exchange: NegRisk CTF Exchange V2 contract address
            conditional_tokens: Conditional Tokens contract address
            neg_risk_adapter: NegRisk Adapter contract address
            pusd: pUSD collateral token (V2). Approved to V2 exchanges.
            collateral_onramp: CollateralOnramp contract for wrapping source asset → pUSD.
            collateral_offramp: CollateralOfframp contract for unwrapping pUSD → source asset.
            source_asset: The user's primary source-of-funds token (USDC.e by default).
                Approved to the Onramp eagerly during ``ensure_allowances``.
            native_usdc: Secondary source-of-funds token (native Circle USDC). VIB-3770:
                tracked so a wallet funded with native USDC instead of USDC.e can still
                wrap to pUSD. The native-USDC → Onramp approval is emitted *only* when
                the wallet actually holds native USDC, so we don't burn gas approving
                an asset the user has never funded.
        """
        self.chain_id = chain_id
        self.ctf_exchange = Web3.to_checksum_address(ctf_exchange)
        self.neg_risk_exchange = Web3.to_checksum_address(neg_risk_exchange)
        self.conditional_tokens = Web3.to_checksum_address(conditional_tokens)
        self.neg_risk_adapter = Web3.to_checksum_address(neg_risk_adapter)
        self.pusd = Web3.to_checksum_address(pusd)
        self.collateral_onramp = Web3.to_checksum_address(collateral_onramp)
        self.collateral_offramp = Web3.to_checksum_address(collateral_offramp)
        self.source_asset = Web3.to_checksum_address(source_asset)
        self.native_usdc = Web3.to_checksum_address(native_usdc)
        # Load ABIs
        self._abi_dir = os.path.join(os.path.dirname(__file__), "abis")
        self._erc20_abi = self._load_abi("erc20")
        self._erc1155_abi = self._load_abi("erc1155")
        self._conditional_tokens_abi = self._load_abi("conditional_tokens")
        self._collateral_onramp_abi = self._load_abi("collateral_onramp")
        self._collateral_offramp_abi = self._load_abi("collateral_offramp")

        logger.info(
            "CtfSDK initialized for chain_id=%d, ctf_exchange=%s, conditional_tokens=%s",
            chain_id,
            ctf_exchange,
            conditional_tokens,
        )

    def _load_abi(self, name: str) -> list[dict]:
        """Load ABI from file."""
        abi_path = os.path.join(self._abi_dir, f"{name}.json")
        try:
            with open(abi_path) as f:
                return json.load(f)
        except FileNotFoundError:
            logger.warning(f"ABI file not found: {abi_path}")
            return []

    # =========================================================================
    # Token Approvals
    # =========================================================================

    def build_approve_collateral_tx(
        self,
        asset: str,
        spender: str,
        sender: str,  # noqa: ARG002  # kept for caller API symmetry
        amount: int = MAX_UINT256,
    ) -> TransactionData:
        """Build a generic ERC-20 approve transaction.

        Used in V2 for: source-asset → Onramp, pUSD → CTF Exchange V2,
        pUSD → NegRisk Exchange V2.

        Args:
            asset: ERC-20 asset address (e.g., pUSD, USDC.e, native USDC).
            spender: Address to approve.
            sender: Transaction sender address (informational; tx is built
                without a from-address since callers may rebroadcast under
                a different signer / via Zodiac wrapper).
            amount: Approval amount (defaults to MAX_UINT256).

        Returns:
            TransactionData for the approval.
        """
        asset = Web3.to_checksum_address(asset)
        spender = Web3.to_checksum_address(spender)

        selector = bytes(Web3.keccak(text="approve(address,uint256)")[:4])
        data = selector + abi_encode(["address", "uint256"], [spender, amount])

        return TransactionData(
            to=asset,
            data="0x" + data.hex(),
            gas_estimate=GAS_ESTIMATES["approve_erc20"],
            description=f"Approve {asset[:10]}... spending for {spender[:10]}...",
        )

    def build_wrap_to_pusd_tx(
        self,
        wallet: str,
        amount: int,
        source_asset: str | None = None,
    ) -> TransactionData:
        """Build a CollateralOnramp.wrap call to mint pUSD from a source asset.

        Args:
            wallet: Address that will receive the minted pUSD.
            amount: Amount of source asset to wrap (token units; same decimals as pUSD).
            source_asset: Source asset address (defaults to ``self.source_asset``,
                typically USDC.e). After Polymarket flips the Onramp pause to allow
                native USDC, callers may pass ``USDC_NATIVE_POLYGON`` instead.

        Returns:
            TransactionData targeting CollateralOnramp.wrap(asset, to, amount).
        """
        wallet = Web3.to_checksum_address(wallet)
        asset = Web3.to_checksum_address(source_asset or self.source_asset)

        selector = bytes(Web3.keccak(text="wrap(address,address,uint256)")[:4])
        data = selector + abi_encode(["address", "address", "uint256"], [asset, wallet, amount])

        return TransactionData(
            to=self.collateral_onramp,
            data="0x" + data.hex(),
            gas_estimate=GAS_ESTIMATES.get("wrap", 150_000),
            description=f"Wrap {asset[:10]}... → pUSD for {wallet[:10]}...",
        )

    def build_unwrap_from_pusd_tx(
        self,
        wallet: str,
        amount: int,
        target_asset: str | None = None,
    ) -> TransactionData:
        """Build a CollateralOfframp.unwrap call to redeem pUSD back to a source asset."""
        wallet = Web3.to_checksum_address(wallet)
        asset = Web3.to_checksum_address(target_asset or self.source_asset)

        selector = bytes(Web3.keccak(text="unwrap(address,address,uint256)")[:4])
        data = selector + abi_encode(["address", "address", "uint256"], [asset, wallet, amount])

        return TransactionData(
            to=self.collateral_offramp,
            data="0x" + data.hex(),
            gas_estimate=GAS_ESTIMATES.get("unwrap", 150_000),
            description=f"Unwrap pUSD → {asset[:10]}... for {wallet[:10]}...",
        )

    def build_approve_conditional_tokens_tx(
        self,
        operator: str,
        approved: bool,
        sender: str,
    ) -> TransactionData:
        """Build ERC-1155 setApprovalForAll transaction.

        Approves the operator (typically CTF Exchange or Neg Risk Adapter)
        to transfer conditional tokens on behalf of the sender.

        Args:
            operator: Address to approve (e.g., CTF_EXCHANGE_V2 or NEG_RISK_ADAPTER)
            approved: True to approve, False to revoke
            sender: Transaction sender address

        Returns:
            TransactionData for the approval
        """
        operator = Web3.to_checksum_address(operator)

        # Encode setApprovalForAll(address,bool)
        selector = bytes(Web3.keccak(text="setApprovalForAll(address,bool)")[:4])
        data = selector + abi_encode(["address", "bool"], [operator, approved])

        return TransactionData(
            to=self.conditional_tokens,
            data="0x" + data.hex(),
            gas_estimate=GAS_ESTIMATES["approve_erc1155"],
            description=f"{'Approve' if approved else 'Revoke'} CTF operator {operator[:10]}...",
        )

    def check_allowances(self, wallet: str, web3: Any) -> AllowanceStatus:
        """Check all relevant V2 token allowances.

        Queries the source-asset → Onramp leg (both USDC.e and native USDC),
        the pUSD → exchange legs, and the CTF (ERC-1155) operator approvals
        needed to trade on Polymarket V2.

        Args:
            wallet: Wallet address to check.
            web3: Web3 instance.

        Returns:
            AllowanceStatus with V2 allowance information, including the
            secondary native-USDC source asset (VIB-3770).
        """
        wallet = Web3.to_checksum_address(wallet)

        source_contract = web3.eth.contract(address=self.source_asset, abi=self._erc20_abi)
        pusd_contract = web3.eth.contract(address=self.pusd, abi=self._erc20_abi)
        ctf_contract = web3.eth.contract(address=self.conditional_tokens, abi=self._conditional_tokens_abi)

        # Source asset (e.g. USDC.e) — balance + allowance to Onramp
        source_balance = source_contract.functions.balanceOf(wallet).call()
        source_allowance_onramp = source_contract.functions.allowance(wallet, self.collateral_onramp).call()

        # pUSD — balance + allowance to both V2 exchanges and the NegRisk Adapter.
        # The Adapter is the actual spender on neg-risk fills (it splits/merges
        # the multi-outcome conditional tokens). Without this approval, neg-risk
        # BUYs are rejected with "the allowance is not enough -> spender: 0xd91E80...".
        pusd_balance = pusd_contract.functions.balanceOf(wallet).call()
        pusd_allowance_ctf = pusd_contract.functions.allowance(wallet, self.ctf_exchange).call()
        pusd_allowance_neg_risk = pusd_contract.functions.allowance(wallet, self.neg_risk_exchange).call()
        pusd_allowance_neg_risk_adapter = pusd_contract.functions.allowance(wallet, self.neg_risk_adapter).call()

        # CTF (ERC-1155) operator approvals
        ctf_approved_exchange = ctf_contract.functions.isApprovedForAll(wallet, self.ctf_exchange).call()
        ctf_approved_adapter = ctf_contract.functions.isApprovedForAll(wallet, self.neg_risk_adapter).call()

        # VIB-3770: native USDC tracked as a secondary source asset.
        # Skipped when the SDK is configured with native USDC AS the primary
        # source_asset (would just re-read the same contract). Failures here
        # are non-fatal — fall back to zeros so a transient Polygon RPC blip
        # doesn't break the whole approval read for a USDC.e-only wallet.
        native_balance = 0
        native_allowance_onramp = 0
        if self.native_usdc.lower() != self.source_asset.lower():
            try:
                native_contract = web3.eth.contract(address=self.native_usdc, abi=self._erc20_abi)
                native_balance = native_contract.functions.balanceOf(wallet).call()
                native_allowance_onramp = native_contract.functions.allowance(wallet, self.collateral_onramp).call()
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Native USDC allowance read failed for %s; treating as 0: %s",
                    wallet,
                    exc,
                )

        return AllowanceStatus(
            source_asset_balance=source_balance,
            pusd_balance=pusd_balance,
            source_asset_allowance_onramp=source_allowance_onramp,
            pusd_allowance_ctf_exchange=pusd_allowance_ctf,
            pusd_allowance_neg_risk_exchange=pusd_allowance_neg_risk,
            pusd_allowance_neg_risk_adapter=pusd_allowance_neg_risk_adapter,
            ctf_approved_for_ctf_exchange=ctf_approved_exchange,
            ctf_approved_for_neg_risk_adapter=ctf_approved_adapter,
            native_usdc_balance=native_balance,
            native_usdc_allowance_onramp=native_allowance_onramp,
        )

    def ensure_allowances(self, wallet: str, web3: Any) -> list[TransactionData]:
        """Build the idempotent V2 approval set.

        Emits only the approvals the wallet doesn't already have. Order:
            1. source_asset → CollateralOnramp     (so user can wrap to pUSD)
            2. pUSD → CTF Exchange V2              (binary BUYs)
            3. pUSD → NegRisk Exchange V2          (neg-risk order matching)
            4. pUSD → NegRisk Adapter              (neg-risk split/merge — the
                                                    adapter is the actual spender
                                                    on fill, not the exchange)
            5. CTF.setApprovalForAll(CTF Exchange V2)  (binary SELLs pull shares)
            6. CTF.setApprovalForAll(NegRisk Adapter)  (neg-risk SELLs / merge)
            7. native USDC → CollateralOnramp     (VIB-3770: ONLY when the wallet
                                                    actually holds native USDC —
                                                    otherwise we burn ~58k gas
                                                    approving an asset the user
                                                    has never funded)

        Returns:
            List of TransactionData for any approvals that aren't already in place.
        """
        status = self.check_allowances(wallet, web3)
        transactions: list[TransactionData] = []

        if not status.source_asset_approved_onramp:
            transactions.append(self.build_approve_collateral_tx(self.source_asset, self.collateral_onramp, wallet))

        if not status.pusd_approved_ctf_exchange:
            transactions.append(self.build_approve_collateral_tx(self.pusd, self.ctf_exchange, wallet))

        if not status.pusd_approved_neg_risk_exchange:
            transactions.append(self.build_approve_collateral_tx(self.pusd, self.neg_risk_exchange, wallet))

        if not status.pusd_approved_neg_risk_adapter:
            transactions.append(self.build_approve_collateral_tx(self.pusd, self.neg_risk_adapter, wallet))

        if not status.ctf_approved_for_ctf_exchange:
            transactions.append(self.build_approve_conditional_tokens_tx(self.ctf_exchange, True, wallet))

        if not status.ctf_approved_for_neg_risk_adapter:
            transactions.append(self.build_approve_conditional_tokens_tx(self.neg_risk_adapter, True, wallet))

        # VIB-3770: native USDC approval only if the wallet holds any.
        # Demand-driven so a USDC.e-only wallet keeps the same 6-tx footprint
        # it had pre-VIB-3770; a native-USDC-only wallet adds exactly one tx.
        # ``self.native_usdc != self.source_asset`` guards the edge case where
        # a future deploy reconfigures native USDC AS the primary source_asset
        # (then leg #1 already covered it).
        if (
            self.native_usdc.lower() != self.source_asset.lower()
            and status.native_usdc_balance > 0
            and not status.native_usdc_approved_onramp
        ):
            transactions.append(self.build_approve_collateral_tx(self.native_usdc, self.collateral_onramp, wallet))

        return transactions

    def get_collateral_breakdown(self, wallet: str, web3: Any) -> CollateralBreakdown:
        """Read the wallet's full collateral breakdown (VIB-3770).

        Three ``balanceOf`` calls — one each for pUSD, the canonical
        bridged USDC.e bucket, and native Circle USDC — keyed off the
        canonical token addresses (not ``self.source_asset``). Pinning the
        ``usdce`` slot to ``USDCE_POLYGON`` keeps the breakdown
        self-consistent if a future deploy ever reconfigures
        ``source_asset`` to the native USDC address: the CLI/UX
        ("how much USDC.e do I have? how much native USDC?") still maps
        each on-chain pile to the right semantic bucket, instead of
        silently lumping native USDC under ``usdce`` and zeroing
        ``usdc_native``.

        Used by the gateway to decide whether a wrap is needed (and from
        which source) and by ``almanak ax balance --protocol polymarket``
        for the user-facing breakdown.
        """
        wallet = Web3.to_checksum_address(wallet)
        usdce_address = Web3.to_checksum_address(USDCE_POLYGON)
        pusd_contract = web3.eth.contract(address=self.pusd, abi=self._erc20_abi)
        usdce_contract = web3.eth.contract(address=usdce_address, abi=self._erc20_abi)
        pusd_bal = pusd_contract.functions.balanceOf(wallet).call()
        usdce_bal = usdce_contract.functions.balanceOf(wallet).call()
        # Read native USDC unless it collides with the canonical USDC.e
        # address (would just re-read the same contract). Failures fall
        # back to 0 so a transient Polygon RPC blip doesn't break the
        # whole breakdown for a USDC.e-only wallet.
        usdc_native_bal = 0
        if self.native_usdc.lower() != usdce_address.lower():
            try:
                native_contract = web3.eth.contract(address=self.native_usdc, abi=self._erc20_abi)
                usdc_native_bal = native_contract.functions.balanceOf(wallet).call()
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Native USDC balance read failed for %s; reporting 0: %s",
                    wallet,
                    exc,
                )
        return CollateralBreakdown(
            pusd=pusd_bal,
            usdce=usdce_bal,
            usdc_native=usdc_native_bal,
            pusd_address=self.pusd,
            usdce_address=usdce_address,
            usdc_native_address=self.native_usdc,
        )

    def select_source_for_wrap(self, deficit: int, status: AllowanceStatus) -> str:
        """Pick the source asset to wrap into pUSD given a deficit.

        VIB-3770: when a BUY needs more pUSD than the wallet holds, the
        gateway must wrap from one of the user's source assets. Earlier
        builds hardcoded USDC.e; users funding native USDC instead saw the
        wrap fail with "Insufficient source asset for wrap" even though
        their wallet was funded.

        Selection rule:

        1. Prefer the configured ``source_asset`` (USDC.e by default) when
           it covers the deficit on its own. This is the path Polymarket's
           Onramp has been live on the longest.
        2. Otherwise pick native USDC if it covers the deficit.
        3. Otherwise pick whichever has the larger balance — the gateway's
           higher-level deficit check will surface the "insufficient" error
           with the more accurate per-asset numbers.

        Returns:
            The chosen source asset address (checksummed).
        """
        usdce_balance = status.source_asset_balance
        native_balance = status.native_usdc_balance

        if usdce_balance >= deficit:
            return self.source_asset
        if native_balance >= deficit and self.native_usdc.lower() != self.source_asset.lower():
            return self.native_usdc
        # Neither covers solo — pick the larger pile so the wrap at least
        # consumes the most-funded asset before bubbling the shortfall.
        if native_balance > usdce_balance and self.native_usdc.lower() != self.source_asset.lower():
            return self.native_usdc
        return self.source_asset

    # =========================================================================
    # Token Balances
    # =========================================================================

    def get_token_balance(self, wallet: str, token_id: int, web3: Any) -> int:
        """Get ERC-1155 token balance.

        Args:
            wallet: Wallet address
            token_id: Conditional token ID (position ID)
            web3: Web3 instance

        Returns:
            Token balance in base units
        """
        wallet = Web3.to_checksum_address(wallet)
        ctf_contract = web3.eth.contract(address=self.conditional_tokens, abi=self._conditional_tokens_abi)
        return ctf_contract.functions.balanceOf(wallet, token_id).call()

    def get_token_balances(self, wallet: str, token_ids: list[int], web3: Any) -> list[int]:
        """Get multiple ERC-1155 token balances in a single call.

        Args:
            wallet: Wallet address
            token_ids: List of conditional token IDs
            web3: Web3 instance

        Returns:
            List of token balances in base units
        """
        wallet = Web3.to_checksum_address(wallet)
        ctf_contract = web3.eth.contract(address=self.conditional_tokens, abi=self._conditional_tokens_abi)

        # Create list of wallet addresses (same wallet for all)
        wallets = [wallet] * len(token_ids)
        return ctf_contract.functions.balanceOfBatch(wallets, token_ids).call()

    def get_pusd_balance(self, wallet: str, web3: Any) -> int:
        """Get pUSD balance — the spendable trading collateral in V2.

        Args:
            wallet: Wallet address
            web3: Web3 instance

        Returns:
            pUSD balance in base units (6 decimals).
        """
        wallet = Web3.to_checksum_address(wallet)
        pusd_contract = web3.eth.contract(address=self.pusd, abi=self._erc20_abi)
        return pusd_contract.functions.balanceOf(wallet).call()

    def get_source_asset_balance(self, wallet: str, web3: Any) -> int:
        """Get the source-asset (USDC.e or native USDC) balance — Onramp input."""
        wallet = Web3.to_checksum_address(wallet)
        source_contract = web3.eth.contract(address=self.source_asset, abi=self._erc20_abi)
        return source_contract.functions.balanceOf(wallet).call()

    # =========================================================================
    # Position ID Calculation
    # =========================================================================

    def get_collection_id(
        self,
        condition_id: bytes,
        index_set: int,
        parent_collection_id: bytes = ZERO_BYTES32,
    ) -> bytes:
        """Calculate collection ID for an outcome.

        Args:
            condition_id: Condition ID (32 bytes)
            index_set: Outcome index set (1=YES, 2=NO for binary)
            parent_collection_id: Parent collection (default: root)

        Returns:
            Collection ID (32 bytes)
        """
        # Collection ID = keccak256(parentCollectionId, conditionId, indexSet)
        encoded = abi_encode(
            ["bytes32", "bytes32", "uint256"],
            [parent_collection_id, condition_id, index_set],
        )
        return Web3.keccak(encoded)

    def get_position_id(self, collateral: str, collection_id: bytes) -> int:
        """Calculate ERC-1155 position ID from collection ID.

        Args:
            collateral: Collateral token address (pUSD in V2)
            collection_id: Collection ID (32 bytes)

        Returns:
            Position ID (uint256)
        """
        collateral = Web3.to_checksum_address(collateral)
        encoded = abi_encode(["address", "bytes32"], [collateral, collection_id])
        return int(Web3.keccak(encoded).hex(), 16)

    def get_token_ids_for_condition(self, condition_id: str | bytes) -> tuple[int, int]:
        """Get YES and NO token IDs for a binary condition.

        Args:
            condition_id: Condition ID (hex string or bytes)

        Returns:
            Tuple of (yes_token_id, no_token_id)
        """
        if isinstance(condition_id, str):
            condition_id = bytes.fromhex(condition_id.replace("0x", ""))

        yes_collection = self.get_collection_id(condition_id, INDEX_SET_YES)
        no_collection = self.get_collection_id(condition_id, INDEX_SET_NO)

        yes_token_id = self.get_position_id(self.pusd, yes_collection)
        no_token_id = self.get_position_id(self.pusd, no_collection)

        return yes_token_id, no_token_id

    # =========================================================================
    # Split / Merge / Redeem Operations
    # =========================================================================

    def build_split_tx(
        self,
        condition_id: str | bytes,
        amount: int,
        sender: str,
    ) -> TransactionData:
        """Build split position transaction.

        Splits pUSD into YES and NO conditional tokens.
        Requires pUSD approval for the Conditional Tokens contract.

        Args:
            condition_id: Condition ID (hex string or bytes)
            amount: Amount of pUSD to split (in base units)
            sender: Transaction sender address

        Returns:
            TransactionData for the split operation
        """
        condition_bytes = (
            bytes.fromhex(condition_id.replace("0x", "")) if isinstance(condition_id, str) else condition_id
        )

        # Encode splitPosition(IERC20, bytes32, bytes32, uint256[], uint256)
        selector = bytes(Web3.keccak(text="splitPosition(address,bytes32,bytes32,uint256[],uint256)")[:4])
        data = selector + abi_encode(
            ["address", "bytes32", "bytes32", "uint256[]", "uint256"],
            [self.pusd, ZERO_BYTES32, condition_bytes, BINARY_PARTITION, amount],
        )

        return TransactionData(
            to=self.conditional_tokens,
            data="0x" + data.hex(),
            gas_estimate=GAS_ESTIMATES["split_position"],
            description=f"Split {amount} pUSD into YES/NO tokens",
        )

    def build_merge_tx(
        self,
        condition_id: str | bytes,
        amount: int,
        sender: str,
    ) -> TransactionData:
        """Build merge positions transaction.

        Merges equal amounts of YES and NO tokens back into pUSD.
        Requires ERC-1155 approval for Conditional Tokens contract.

        Args:
            condition_id: Condition ID (hex string or bytes)
            amount: Amount of each outcome token to merge
            sender: Transaction sender address

        Returns:
            TransactionData for the merge operation
        """
        condition_bytes = (
            bytes.fromhex(condition_id.replace("0x", "")) if isinstance(condition_id, str) else condition_id
        )

        # Encode mergePositions(IERC20, bytes32, bytes32, uint256[], uint256)
        selector = bytes(Web3.keccak(text="mergePositions(address,bytes32,bytes32,uint256[],uint256)")[:4])
        data = selector + abi_encode(
            ["address", "bytes32", "bytes32", "uint256[]", "uint256"],
            [self.pusd, ZERO_BYTES32, condition_bytes, BINARY_PARTITION, amount],
        )

        return TransactionData(
            to=self.conditional_tokens,
            data="0x" + data.hex(),
            gas_estimate=GAS_ESTIMATES["merge_positions"],
            description=f"Merge {amount} YES+NO tokens into pUSD",
        )

    def build_redeem_tx(
        self,
        condition_id: str | bytes,
        index_sets: list[int],
        sender: str,
    ) -> TransactionData:
        """Build redeem positions transaction.

        Redeems winning positions after market resolution.
        Only works if the condition has been resolved.

        Args:
            condition_id: Condition ID (hex string or bytes)
            index_sets: List of index sets to redeem (e.g., [1, 2] for both)
            sender: Transaction sender address

        Returns:
            TransactionData for the redemption
        """
        condition_bytes = (
            bytes.fromhex(condition_id.replace("0x", "")) if isinstance(condition_id, str) else condition_id
        )

        # Encode redeemPositions(IERC20, bytes32, bytes32, uint256[])
        selector = bytes(Web3.keccak(text="redeemPositions(address,bytes32,bytes32,uint256[])")[:4])
        data = selector + abi_encode(
            ["address", "bytes32", "bytes32", "uint256[]"],
            [self.pusd, ZERO_BYTES32, condition_bytes, index_sets],
        )

        return TransactionData(
            to=self.conditional_tokens,
            data="0x" + data.hex(),
            gas_estimate=GAS_ESTIMATES["redeem_positions"],
            description="Redeem winning positions",
        )

    # =========================================================================
    # Condition Resolution
    # =========================================================================

    def get_condition_resolution(self, condition_id: str | bytes, web3: Any) -> ResolutionStatus:
        """Get resolution status of a condition.

        Checks if a condition has been resolved and returns payout information.

        Args:
            condition_id: Condition ID (hex string or bytes)
            web3: Web3 instance

        Returns:
            ResolutionStatus with resolution information
        """
        if isinstance(condition_id, str):
            condition_id_str = condition_id
            condition_id = bytes.fromhex(condition_id.replace("0x", ""))
        else:
            condition_id_str = "0x" + condition_id.hex()

        ctf_contract = web3.eth.contract(address=self.conditional_tokens, abi=self._conditional_tokens_abi)

        # Get payout denominator (0 if not resolved)
        payout_denom = ctf_contract.functions.payoutDenominator(condition_id).call()

        is_resolved = payout_denom > 0

        # Get payout numerators for each outcome
        payout_numerators = []
        winning_outcome = None

        if is_resolved:
            # Binary markets have 2 outcomes (YES=0, NO=1)
            try:
                outcome_count = ctf_contract.functions.getOutcomeSlotCount(condition_id).call()
            except Exception:
                outcome_count = 2  # Default for binary markets

            for i in range(outcome_count):
                numerator = ctf_contract.functions.payoutNumerators(condition_id, i).call()
                payout_numerators.append(numerator)

                # Winning outcome has non-zero numerator
                if numerator > 0 and winning_outcome is None:
                    winning_outcome = i

        return ResolutionStatus(
            condition_id=condition_id_str,
            is_resolved=is_resolved,
            payout_denominator=payout_denom,
            payout_numerators=payout_numerators,
            winning_outcome=winning_outcome,
        )


__all__ = [
    "CtfSDK",
    "TransactionData",
    "AllowanceStatus",
    "CollateralBreakdown",
    "ResolutionStatus",
    "MAX_UINT256",
    "SUFFICIENT_ALLOWANCE_THRESHOLD",
    "ZERO_BYTES32",
    "INDEX_SET_YES",
    "INDEX_SET_NO",
    "BINARY_PARTITION",
    "GAS_ESTIMATES",
]
