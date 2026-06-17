"""Connector-owned compiler for Fluid vault (NFT-CDP) lending intents.

Phase 3 (VIB-5031), protocol key ``fluid_vault`` — the second thin manifest
over the fluid package (ADR r2 Q0). Every lifecycle intent compiles to the
vault's single signed-delta entrypoint
``operate(nftId, newCol, newDebt, to)`` with the OTHER delta zeroed:

==================  =============================================
SUPPLY              ``operate(nft_or_0, +col, 0, wallet)``
BORROW              ``operate(nft_or_0, +col, +debt, wallet)`` (single atomic call)
REPAY / DELEVERAGE  ``operate(nftId, 0, −amount, wallet)``
REPAY(repay_full)   ``operate(nftId, 0, INT256_MIN, wallet)``
WITHDRAW            ``operate(nftId, −amount, 0, wallet)``
WITHDRAW(all)       ``operate(nftId, INT256_MIN, 0, wallet)``
==================  =============================================

Hard rules encoded here (ADR §2.2–§2.4):

- ``market_id`` (the vault address) is REQUIRED and must be a pinned
  type-1 vault; fToken addresses are refused with a message naming both
  universes; native-DEBT vaults and non-arbitrum/base chains are refused.
- The nftId is resolved FRESH from chain on every compile (VIB-5010:
  chain is the source of truth). A resolution READ FAILURE fails the
  compile — it NEVER falls back to ``nftId=0`` (minting a duplicate
  position is the disaster case). ``nftId=0`` mints only on the measured
  "wallet holds no NFT on this vault" answer.
- ``INT256_MIN`` is reachable ONLY via ``repay_full=True`` /
  ``withdraw_all=True``. Partial repays pre-flight ``amount <= debt_now``
  (debt only grows, so a passing check cannot 31015 at execution); full
  repays pre-flight the wallet balance at ``debt_now × (1 + headroom)``
  and approve exactly that bound — never ``MAX_UINT256``.
- Native-ETH collateral legs (vault id 1): no approve; the operate
  ``TransactionData.value`` carries the collateral wei.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import ROUND_CEILING, Decimal
from typing import Any, ClassVar

from almanak.connectors._fluid_core.addresses import FLUID_VAULT_MARKETS
from almanak.connectors._fluid_core.sdk import FluidSDKError
from almanak.connectors._fluid_core.vault_sdk import (
    INT256_MIN,
    FluidVaultData,
    FluidVaultPosition,
    FluidVaultSDK,
)
from almanak.connectors._strategy_base.base.compiler import BaseCompilerContext
from almanak.connectors._strategy_base.base.lending import BaseLendingCompiler
from almanak.framework.intents._compiler_helpers import assemble_action_bundle, sum_transaction_gas
from almanak.framework.intents.compiler_constants import get_gas_estimate
from almanak.framework.intents.compiler_models import CompilationResult, CompilationStatus, TransactionData
from almanak.framework.intents.vocabulary import BorrowIntent, RepayIntent, SupplyIntent, WithdrawIntent

logger = logging.getLogger(__name__)

#: Repay-full funding/approval headroom over ``debt_now`` (0.1 % — hours of
#: interest at double-digit APRs). Bounded approval by design: the vault
#: pulls only the exact debt, so over-allowance beyond the pull is harmless
#: while an unbounded MAX_UINT approval is a standing-risk anti-pattern.
DEFAULT_REPAY_HEADROOM = Decimal("0.001")

#: Fluid oracle exchange-rate scale: ``debt_units = col_units × price / 1e27``.
_ORACLE_PRICE_SCALE = 10**27
_BPS = 10**4


_ERC20_APPROVE_SELECTOR = "0x095ea7b3"


def _exact_erc20_approve_txs(token_address: str, spender: str, amount: int, chain: str) -> list[TransactionData]:
    """Build an ERC-20 approve with the EXACT ``amount`` (no headroom buffer).

    The framework's ``ctx.services.build_approve_tx`` adds a 10% slippage buffer
    designed for swap approvals. For ERC-20 collateral SUPPLY the vault pulls
    exactly the declared amount — no accrual, no slippage — so an exact approval
    is both correct and the minimum-trust posture (UAT card D2.M1 contract).
    """
    spender_padded = spender.lower().replace("0x", "").zfill(64)
    amount_padded = hex(amount)[2:].zfill(64)
    return [
        TransactionData(
            to=token_address,
            value=0,
            data=_ERC20_APPROVE_SELECTOR + spender_padded + amount_padded,
            gas_estimate=get_gas_estimate(chain, "approve"),
            description=f"Approve {spender[:10]}... to spend {amount} (exact)",
            tx_type="approve",
        )
    ]


def _failed(intent: Any, error: str, *, is_transient: bool = False) -> CompilationResult:
    """Build a FAILED result.

    ``is_transient=True`` marks a retryable (orchestration-level) failure so
    the teardown manager treats it as ``retryable`` rather than a hard,
    permanent failure — used for the time/utilization-gated Liquidity-layer
    limit branches whose messages say "limit-gated (retryable)".
    """
    return CompilationResult(
        status=CompilationStatus.FAILED,
        error=error,
        intent_id=getattr(intent, "intent_id", ""),
        is_transient=is_transient,
    )


@dataclass(frozen=True)
class _VaultSetup:
    """Validated per-compile context shared by every intent path."""

    vault: str  # lowercased vault address — the canonical market_id form
    entry: dict[str, Any]  # pinned FLUID_VAULT_MARKETS row
    sdk: FluidVaultSDK
    wallet: str
    chain: str
    discovery: bool  # permission discovery compiles for calldata SHAPE only


class FluidVaultCompiler(BaseLendingCompiler):
    """Compile ``fluid_vault`` SUPPLY / BORROW / REPAY / WITHDRAW / DELEVERAGE.

    DELEVERAGE routes to :meth:`compile_repay` via the
    :class:`BaseLendingCompiler` dispatch (framework convention: a
    deleverage is a repay with risk context).
    """

    protocols: ClassVar[frozenset[str]] = frozenset({"fluid_vault"})
    chains: ClassVar[frozenset[str]] = frozenset({"arbitrum", "base"})
    #: Checkpoint-1 scope — kept as compile-time defense-in-depth even though
    #: the manifest already declares exactly this universe.
    VAULT_CHAINS: ClassVar[frozenset[str]] = frozenset({"arbitrum", "base"})
    REPAY_HEADROOM: ClassVar[Decimal] = DEFAULT_REPAY_HEADROOM

    # =========================================================================
    # Shared setup / validation helpers
    # =========================================================================

    def _vault_setup(self, ctx: BaseCompilerContext, intent: Any) -> _VaultSetup | CompilationResult:
        """Chain gate → market_id validation → SDK transport. Fail closed."""
        if ctx.chain not in self.VAULT_CHAINS:
            return _failed(
                intent,
                f"Fluid vault lending (fluid_vault) is enabled on {sorted(self.VAULT_CHAINS)} "
                f"(Checkpoint-1 scope, VIB-5031); chain '{ctx.chain}' is not supported. "
                f"Additional chains ship only after on-chain validation.",
            )
        validated = self._validate_market_id(ctx, intent)
        if isinstance(validated, CompilationResult):
            return validated
        vault, entry = validated
        sdk = self._build_sdk(ctx, intent)
        if isinstance(sdk, CompilationResult):
            return sdk
        return _VaultSetup(
            vault=vault,
            entry=entry,
            sdk=sdk,
            wallet=ctx.wallet_address,
            chain=ctx.chain,
            discovery=bool(getattr(ctx, "permission_discovery", False)),
        )

    def _validate_market_id(
        self, ctx: BaseCompilerContext, intent: Any
    ) -> tuple[str, dict[str, Any]] | CompilationResult:
        """Re-validate + lowercase the vault ``market_id`` (capability handles
        construction; the compiler re-validates as defense-in-depth)."""
        market_id = getattr(intent, "market_id", None)
        if not market_id:
            return _failed(
                intent,
                "fluid_vault intents require market_id (the type-1 vault address). "
                "Fluid vaults are isolated markets — there is no default vault.",
            )
        vault = str(market_id).strip().lower()
        entry = FLUID_VAULT_MARKETS.get(ctx.chain, {}).get(vault)
        if entry is None:
            return self._unknown_market_failure(ctx, intent, vault)
        if int(entry.get("vault_type", 0)) != 10000:
            return _failed(
                intent,
                f"Fluid vault {vault} on {ctx.chain} is not a type-1 (T1) vault "
                f"(vault_type={entry.get('vault_type')}). VIB-5031 ships type-1 vaults only; "
                f"smart vaults (types 2/3/4) are Phase 4 (VIB-5032) scope.",
            )
        if bool(entry.get("native_debt", False)):
            return _failed(
                intent,
                f"Fluid vault {vault} on {ctx.chain} has a NATIVE debt leg, which is out of "
                f"v1 scope (msg.value refund behaviour for full native repays is not "
                f"on-chain-validated). Use a vault with an ERC-20 debt token.",
            )
        return vault, entry

    @staticmethod
    def _unknown_market_failure(ctx: BaseCompilerContext, intent: Any, vault: str) -> CompilationResult:
        """Distinct messages for fToken-address vs genuinely unknown market_id."""
        from almanak.connectors._fluid_core.lending_read import FLUID_FTOKEN_MARKETS

        ftoken_addresses = {
            str(market.get("comet_address", "")).lower(): symbol
            for symbol, market in FLUID_FTOKEN_MARKETS.get(ctx.chain, {}).items()
        }
        if vault in ftoken_addresses:
            return _failed(
                intent,
                f"market_id {vault} is the Fluid fToken (ERC-4626) market for "
                f"'{ftoken_addresses[vault]}' on {ctx.chain} — fToken lending uses "
                f"protocol='fluid' (or 'fluid_lending') with NO market_id. "
                f"protocol='fluid_vault' takes a type-1 VAULT address (NFT-CDP borrow "
                f"positions). The two universes never mix.",
            )
        known = sorted(FLUID_VAULT_MARKETS.get(ctx.chain, {}))
        return _failed(
            intent,
            f"Unknown Fluid vault market_id {vault} on {ctx.chain}: not in the pinned "
            f"type-1 vault universe {known}. If this is a real Fluid vault, it must be "
            f"validated and pinned (FLUID_VAULT_MARKETS) before compiling positions into "
            f"it — failing closed rather than borrowing against an unverified market. "
            f"(fToken addresses belong on protocol='fluid'.)",
        )

    @staticmethod
    def _build_sdk(ctx: BaseCompilerContext, intent: Any) -> FluidVaultSDK | CompilationResult:
        """Gateway-client-preferred transport (Phase-1 ``_build_sdk`` pattern).

        A PRESENT-but-disconnected gateway client fails the compile CLOSED:
        silently nulling it would turn a gateway outage into a direct-RPC
        bypass via ``ctx.rpc_url``. The rpc_url fallback exists ONLY for
        contexts with no gateway client configured at all (the intent-test
        harness speaking to local Anvil).
        """
        gateway_client = ctx.gateway_client
        if gateway_client is not None and not getattr(gateway_client, "is_connected", False):
            return _failed(
                intent,
                "Fluid vault compilation requires a connected gateway client or RPC URL "
                "(the position nftId and vault state are resolved on-chain at compile time): "
                "a gateway client is configured but DISCONNECTED — failing closed rather "
                "than silently bypassing the gateway over direct RPC.",
            )
        if gateway_client is None and not ctx.rpc_url:
            return _failed(
                intent,
                "Fluid vault compilation requires a connected gateway client or RPC URL "
                "(the position nftId and vault state are resolved on-chain at compile time).",
            )
        return FluidVaultSDK(
            chain=ctx.chain,
            rpc_url=None if gateway_client is not None else ctx.rpc_url,
            gateway_client=gateway_client,
        )

    @staticmethod
    def _resolve_nft(setup: _VaultSetup, intent: Any, *, allow_mint: bool) -> int | CompilationResult:
        """Resolve the wallet's nftId FRESH from chain (never persisted state).

        A read failure is a FAILED compile — never a silent ``nftId=0`` mint
        (the duplicate-position disaster, D3.F3). ``None`` (the measured
        no-position answer) mints only where minting is legal.
        """
        if setup.discovery:
            return 0  # calldata shape only — synthetic wallets hold no position
        try:
            nft_id = setup.sdk.resolve_user_nft_for_vault(setup.wallet, setup.vault)
        except FluidSDKError as exc:
            return _failed(
                intent,
                f"Fluid vault nftId resolution failed for {setup.vault} on {setup.chain}: {exc}. "
                f"Refusing to compile — falling back to a mint (nftId=0) on a read failure "
                f"could create a duplicate position (fail closed).",
            )
        if nft_id is not None:
            return nft_id
        if allow_mint:
            return 0
        return _failed(
            intent,
            f"No Fluid vault position exists for wallet {setup.wallet} on vault "
            f"{setup.vault} ({setup.chain}) — nothing to repay/withdraw. Open the "
            f"position first (SUPPLY or BORROW mints the NFT).",
        )

    def _position_state(
        self, setup: _VaultSetup, intent: Any, nft_id: int
    ) -> tuple[FluidVaultPosition, FluidVaultData] | CompilationResult:
        """Read the live position + vault data; read failures fail CLOSED."""
        try:
            return setup.sdk.position_by_nft_id(nft_id)
        except FluidSDKError as exc:
            return _failed(
                intent,
                f"Fluid vault position read failed for nftId={nft_id} on {setup.vault} "
                f"({setup.chain}): {exc}. Refusing to compile without the on-chain debt/"
                f"collateral pre-flight (fail closed).",
            )

    def _vault_data(self, setup: _VaultSetup, intent: Any) -> FluidVaultData | CompilationResult:
        """Read live vault config (oracle price, factors); fail CLOSED."""
        try:
            return setup.sdk.get_vault_entire_data(setup.vault)
        except FluidSDKError as exc:
            return _failed(
                intent,
                f"Fluid vault data read failed for {setup.vault} on {setup.chain}: {exc}. "
                f"Refusing to compile without the on-chain pre-flight (fail closed).",
            )

    def _leg_token(
        self,
        ctx: BaseCompilerContext,
        setup: _VaultSetup,
        intent: Any,
        symbol: str,
        *,
        leg: str,
    ) -> Any | CompilationResult:
        """Resolve ``symbol`` and require it to BE the vault's ``leg`` token."""
        token = ctx.services.resolve_token(symbol)
        if token is None:
            return _failed(intent, f"Unknown token: {symbol}")
        if leg == "collateral":
            expected_symbol = setup.entry["collateral_token"]
            matches = (
                bool(token.is_native)
                if setup.entry["native_collateral"]
                else token.address.lower() == str(setup.entry["collateral_address"]).lower()
            )
        else:
            expected_symbol = setup.entry["loan_token"]
            matches = token.address.lower() == str(setup.entry["loan_address"]).lower()
        if not matches:
            return _failed(
                intent,
                f"Token {symbol} is not the {leg} token of Fluid vault {setup.vault} on "
                f"{setup.chain} (vault pair: {setup.entry['collateral_token']} -> "
                f"{setup.entry['loan_token']}; expected {leg}: {expected_symbol}).",
            )
        return token

    @staticmethod
    def _wei_amount(intent: Any, amount: Any, token: Any, label: str) -> int | CompilationResult:
        """Concrete Decimal -> base units. ``amount='all'`` must be pre-resolved."""
        if amount == "all":
            return _failed(
                intent,
                f"{label}='all' must be resolved to a concrete Decimal before compilation "
                f"(Intent.set_resolved_amount() / the runner's amount resolver). The "
                f"int-min protocol sentinel is reachable only via repay_full/withdraw_all.",
            )
        wei = int(Decimal(amount) * Decimal(10**token.decimals))
        if wei <= 0:
            return _failed(intent, f"{label} resolves to 0 base units of {token.symbol}")
        return wei

    def _operate_transaction(
        self,
        setup: _VaultSetup,
        nft_id: int,
        col_delta: int,
        debt_delta: int,
        *,
        value: int,
        description: str,
        tx_type: str,
    ) -> TransactionData:
        tx = setup.sdk.build_operate_tx(setup.vault, nft_id, col_delta, debt_delta, setup.wallet, value=value)
        return TransactionData(
            to=tx["to"],
            value=tx["value"],
            data=tx["data"],
            gas_estimate=tx["gas"],
            description=description,
            tx_type=tx_type,
        )

    @staticmethod
    def _erc20_balance(ctx: BaseCompilerContext, intent: Any, token: Any, action: str) -> int | CompilationResult:
        """Wallet-balance pre-flight; an unreadable balance fails CLOSED."""
        balance = ctx.services.query_erc20_balance(token.address, ctx.wallet_address)
        if balance is None:
            return _failed(
                intent,
                f"Wallet {token.symbol} balance read failed on {ctx.chain} — refusing to "
                f"compile the {action} without the funding pre-flight (fail closed).",
            )
        return int(balance)

    def _result(
        self, intent: Any, transactions: list[TransactionData], metadata: dict[str, Any], warnings: list[str]
    ) -> CompilationResult:
        result = CompilationResult(status=CompilationStatus.SUCCESS, intent_id=intent.intent_id)
        result.transactions = transactions
        result.total_gas_estimate = sum_transaction_gas(transactions)
        result.action_bundle = assemble_action_bundle(
            intent_type=intent.intent_type.value,
            transactions=transactions,
            metadata=metadata,
        )
        result.warnings = warnings
        return result

    def _base_metadata(self, setup: _VaultSetup, nft_id: int) -> dict[str, Any]:
        # ``market_id`` is the canonical (lowercased) vault address — the L3
        # writer / L5 valuer both lowercase again, so keys stay byte-identical
        # for any caller spelling. ``nft_id`` here is compile-time context;
        # the persisted receipt-truth nftId rides extracted_data_json (§6.3).
        return {
            "protocol": "fluid_vault",
            "chain": setup.chain,
            "market_id": setup.vault,
            "nft_id": str(nft_id),
        }

    # =========================================================================
    # SUPPLY — operate(nft_or_0, +col, 0, wallet)
    # =========================================================================

    def compile_supply(self, ctx: BaseCompilerContext, intent: SupplyIntent) -> CompilationResult:
        setup = self._vault_setup(ctx, intent)
        if isinstance(setup, CompilationResult):
            return setup
        token = self._leg_token(ctx, setup, intent, intent.token, leg="collateral")
        if isinstance(token, CompilationResult):
            return token
        col_wei = self._wei_amount(intent, intent.amount, token, "amount")
        if isinstance(col_wei, CompilationResult):
            return col_wei
        nft_id = self._resolve_nft(setup, intent, allow_mint=True)
        if isinstance(nft_id, CompilationResult):
            return nft_id

        warnings: list[str] = []
        transactions: list[TransactionData] = []
        value = 0
        if setup.entry["native_collateral"]:
            value = col_wei
            warnings.append("Native-collateral supply: amount sent as msg.value to the vault (no approve)")
        else:
            # Exact approval: the vault pulls exactly the declared collateral amount
            # (no accrual, no slippage). The framework's build_approve_tx adds a 10%
            # buffer for swap approvals; for a supply that buffer is incorrect.
            transactions.extend(_exact_erc20_approve_txs(token.address, setup.vault, col_wei, setup.chain))
        if nft_id == 0:
            warnings.append(f"No existing position on vault {setup.vault} — operate(nftId=0) mints a new NFT")

        transactions.append(
            self._operate_transaction(
                setup,
                nft_id,
                col_wei,
                0,
                value=value,
                description=(
                    f"Supply {ctx.services.format_amount(col_wei, token.decimals)} {token.symbol} "
                    f"into Fluid vault {setup.vault} (nftId={nft_id or 'mint'})"
                ),
                tx_type="lending_supply",
            )
        )
        metadata = self._base_metadata(setup, nft_id) | {
            "supply_token": token.to_dict(),
            "supply_amount": str(col_wei),
        }
        logger.info(
            "Compiled fluid_vault SUPPLY: %s %s -> vault %s (nftId=%s)",
            ctx.services.format_amount(col_wei, token.decimals),
            token.symbol,
            setup.vault,
            nft_id,
        )
        return self._result(intent, transactions, metadata, warnings)

    # =========================================================================
    # BORROW — operate(nft_or_0, +col, +debt, wallet) — single atomic call
    # =========================================================================

    def compile_borrow(self, ctx: BaseCompilerContext, intent: BorrowIntent) -> CompilationResult:
        setup = self._vault_setup(ctx, intent)
        if isinstance(setup, CompilationResult):
            return setup
        collateral_token = self._leg_token(ctx, setup, intent, intent.collateral_token, leg="collateral")
        if isinstance(collateral_token, CompilationResult):
            return collateral_token
        borrow_token = self._leg_token(ctx, setup, intent, intent.borrow_token, leg="debt")
        if isinstance(borrow_token, CompilationResult):
            return borrow_token

        if intent.collateral_amount == "all":
            return _failed(
                intent,
                "collateral_amount='all' must be resolved before compilation. "
                "Use Intent.set_resolved_amount() to resolve chained amounts.",
            )
        col_amount = Decimal(intent.collateral_amount)
        if col_amount < 0:
            # BorrowIntent's model validator already rejects negative Decimals,
            # but the compiler is the last gate before calldata: a negative
            # collateral leg would encode operate(nft, -col, +debt) — a
            # withdraw smuggled into a BORROW. Fail closed, never encode it.
            return _failed(
                intent,
                f"collateral_amount must be non-negative (got {col_amount}) — a negative "
                f"collateral leg would encode a withdraw inside a BORROW operate(). "
                f"Use WithdrawIntent to remove collateral.",
            )
        col_wei = int(col_amount * Decimal(10**collateral_token.decimals))
        debt_wei = self._wei_amount(intent, intent.borrow_amount, borrow_token, "borrow_amount")
        if isinstance(debt_wei, CompilationResult):
            return debt_wei

        # allow_mint=True: an atomic open (col > 0) legally mints. The
        # col == 0 + no-NFT case gets its own borrow-flavoured failure below.
        nft_id = self._resolve_nft(setup, intent, allow_mint=True)
        if isinstance(nft_id, CompilationResult):
            return nft_id
        if nft_id == 0 and col_wei <= 0 and not setup.discovery:
            return _failed(
                intent,
                f"Borrow against existing collateral requested (collateral_amount=0) but wallet "
                f"{setup.wallet} holds no position on vault {setup.vault} — supply collateral "
                f"first or set collateral_amount > 0 for an atomic open.",
            )

        preflight = self._borrow_preflight(setup, intent, nft_id, col_wei, debt_wei)
        if isinstance(preflight, CompilationResult):
            return preflight

        warnings: list[str] = []
        transactions: list[TransactionData] = []
        value = 0
        if col_wei > 0:
            if setup.entry["native_collateral"]:
                value = col_wei
                warnings.append("Native-collateral leg: collateral sent as msg.value to the vault (no approve)")
            else:
                # Exact collateral approval (same rationale as compile_supply).
                transactions.extend(
                    _exact_erc20_approve_txs(collateral_token.address, setup.vault, col_wei, setup.chain)
                )
        if nft_id == 0:
            warnings.append(f"No existing position on vault {setup.vault} — operate(nftId=0) mints a new NFT")

        col_label = ctx.services.format_amount(col_wei, collateral_token.decimals) if col_wei else "0"
        transactions.append(
            self._operate_transaction(
                setup,
                nft_id,
                col_wei,
                debt_wei,
                value=value,
                description=(
                    f"Borrow {ctx.services.format_amount(debt_wei, borrow_token.decimals)} "
                    f"{borrow_token.symbol} against {col_label} {collateral_token.symbol} on "
                    f"Fluid vault {setup.vault} (nftId={nft_id or 'mint'})"
                ),
                tx_type="lending_borrow",
            )
        )
        metadata = self._base_metadata(setup, nft_id) | {
            "borrow_token": borrow_token.to_dict(),
            "borrow_amount": str(debt_wei),
            "collateral_token": collateral_token.to_dict(),
            "collateral_amount": str(col_wei),
        }
        logger.info(
            "Compiled fluid_vault BORROW: +%s col, +%s debt on vault %s (nftId=%s)",
            col_wei,
            debt_wei,
            setup.vault,
            nft_id,
        )
        return self._result(intent, transactions, metadata, warnings)

    def _borrow_preflight(
        self, setup: _VaultSetup, intent: Any, nft_id: int, col_wei: int, debt_wei: int
    ) -> None | CompilationResult:
        """Post-borrow position must stay under the vault's collateral factor
        AND under the vault's live borrowable liquidity.

        ``operate`` reverts above CF (``Vault__PositionAboveCF``) — compiling
        a doomed borrow wastes gas and trips sadflow, so the compiler
        pre-flights against the SAME on-chain boundary using the vault's own
        oracle price (protocol truth, not our price oracle). The second gate
        is the Liquidity layer's time/utilization-expanding borrow cap
        (``limitsAndAvailability.borrowable``): exceeding it reverts with
        ``UserModule__MaxUtilizationReached`` (errorId 11011), so it gets the
        DISTINCT limit-gated (retryable) failure — the Phase-2 ``maxWithdraw``
        precedent (VIB-5104). Read failures fail CLOSED (never skip the
        pre-flight — D3.F3).
        """
        if setup.discovery:
            return None
        vault_data = self._vault_data(setup, intent)
        if isinstance(vault_data, CompilationResult):
            return vault_data
        existing_supply = existing_borrow = 0
        if nft_id != 0:
            state = self._position_state(setup, intent, nft_id)
            if isinstance(state, CompilationResult):
                return state
            position, _ = state
            existing_supply, existing_borrow = position.supply, position.borrow

        post_col = existing_supply + col_wei
        post_debt = existing_borrow + debt_wei
        col_in_debt_units = post_col * vault_data.oracle_price_operate // _ORACLE_PRICE_SCALE
        max_debt = col_in_debt_units * vault_data.collateral_factor // _BPS
        if post_debt > max_debt:
            return _failed(
                intent,
                f"Borrow would exceed Fluid vault {setup.vault}'s collateral factor "
                f"({vault_data.collateral_factor / 100:.0f}%): post-borrow debt {post_debt} > "
                f"max borrowable {max_debt} (vault-oracle terms). The on-chain operate() "
                f"would revert (Vault__PositionAboveCF) — reduce borrow_amount or add collateral.",
            )
        if debt_wei > vault_data.borrowable:
            # The Liquidity layer's availability cap, NOT a CF/health failure:
            # it is time/utilization-gated and expands on its own — the same
            # funds-safe class as the fToken maxWithdraw gate (VIB-5104).
            return _failed(
                intent,
                f"Fluid borrow limit-gated (retryable): requested borrow {debt_wei} exceeds the "
                f"vault's currently borrowable {max(vault_data.borrowable, 0)} (debt-token base "
                f"units) on Fluid vault {setup.vault}. This cap is time/utilization-gated — the "
                f"on-chain operate() would revert (UserModule__MaxUtilizationReached, errorId "
                f"11011) and Fluid's Liquidity-layer limits expand over time, so retry later or "
                f"reduce borrow_amount. Not a hard failure; no position risk.",
                is_transient=True,
            )
        return None

    # =========================================================================
    # REPAY / DELEVERAGE — operate(nftId, 0, −amount | INT256_MIN, wallet)
    # =========================================================================

    def compile_repay(self, ctx: BaseCompilerContext, intent: RepayIntent) -> CompilationResult:
        setup = self._vault_setup(ctx, intent)
        if isinstance(setup, CompilationResult):
            return setup
        token = self._leg_token(ctx, setup, intent, intent.token, leg="debt")
        if isinstance(token, CompilationResult):
            return token
        nft_id = self._resolve_nft(setup, intent, allow_mint=False)
        if isinstance(nft_id, CompilationResult):
            return nft_id

        if setup.discovery:
            # Calldata shape only: approve + a 1-base-unit partial repay.
            transactions = [
                *_exact_erc20_approve_txs(token.address, setup.vault, 1, setup.chain),
                self._operate_transaction(
                    setup,
                    nft_id,
                    0,
                    -1,
                    value=0,
                    description=f"Repay {token.symbol} on Fluid vault {setup.vault} (discovery shape)",
                    tx_type="lending_repay",
                ),
            ]
            metadata = self._base_metadata(setup, nft_id) | {"repay_token": token.to_dict(), "repay_amount": "1"}
            return self._result(intent, transactions, metadata, [])

        state = self._position_state(setup, intent, nft_id)
        if isinstance(state, CompilationResult):
            return state
        position, _ = state
        debt_now = position.borrow

        if getattr(intent, "repay_full", False):
            return self._compile_repay_full(ctx, setup, intent, token, nft_id, debt_now)
        return self._compile_repay_partial(ctx, setup, intent, token, nft_id, debt_now)

    def _compile_repay_full(
        self,
        ctx: BaseCompilerContext,
        setup: _VaultSetup,
        intent: Any,
        token: Any,
        nft_id: int,
        debt_now: int,
    ) -> CompilationResult:
        """Full repay = the INT256_MIN sentinel — NEVER an explicit amount.

        The vault computes the exact debt at execution time, so interest
        accrued between compile and execute is structurally covered. The
        wallet must hold ``debt_now × (1 + headroom)`` (the execution-time
        debt exceeds ``debt_now``); the ERC-20 approval is the same bound —
        not MAX_UINT256 (the vault pulls only the exact debt).
        """
        if debt_now <= 0:
            return _failed(
                intent,
                f"Fluid vault position nftId={nft_id} on {setup.vault} has no outstanding debt — nothing to repay.",
            )
        required = int((Decimal(debt_now) * (1 + self.REPAY_HEADROOM)).to_integral_value(rounding=ROUND_CEILING))
        balance = self._erc20_balance(ctx, intent, token, "full repay")
        if isinstance(balance, CompilationResult):
            return balance
        if balance < required:
            return _failed(
                intent,
                f"repay_full pre-flight: wallet holds {ctx.services.format_amount(balance, token.decimals)} "
                f"{token.symbol} but the full repayment needs "
                f"{ctx.services.format_amount(required, token.decimals)} "
                f"(current debt {ctx.services.format_amount(debt_now, token.decimals)} + "
                f"{self.REPAY_HEADROOM:%} accrual headroom). Fund the wallet or repay partially.",
            )
        transactions = [
            *_exact_erc20_approve_txs(token.address, setup.vault, required, setup.chain),
            self._operate_transaction(
                setup,
                nft_id,
                0,
                INT256_MIN,
                value=0,
                description=(
                    f"Repay FULL {token.symbol} debt (~"
                    f"{ctx.services.format_amount(debt_now, token.decimals)}, int-min sentinel) "
                    f"on Fluid vault {setup.vault} (nftId={nft_id})"
                ),
                tx_type="lending_repay",
            ),
        ]
        metadata = self._base_metadata(setup, nft_id) | {
            "repay_token": token.to_dict(),
            # Compile-time debt snapshot for description/pre-flight surfaces;
            # the receipt-truth amount comes from the vault LogOperate deltas.
            "repay_amount": str(debt_now),
            "mode": "repay_full_int_min",
            "approval_bound": str(required),
        }
        logger.info("Compiled fluid_vault REPAY(full): int-min sentinel on vault %s (nftId=%s)", setup.vault, nft_id)
        return self._result(intent, transactions, metadata, [])

    def _compile_repay_partial(
        self,
        ctx: BaseCompilerContext,
        setup: _VaultSetup,
        intent: Any,
        token: Any,
        nft_id: int,
        debt_now: int,
    ) -> CompilationResult:
        """Partial repays are monotonically safe: debt only grows with time,
        so any explicit ``amount <= debt_now`` cannot over-repay (31015 is
        unreachable from a compile that passed this check)."""
        amount_wei = self._wei_amount(intent, intent.amount, token, "amount")
        if isinstance(amount_wei, CompilationResult):
            return amount_wei
        if amount_wei > debt_now:
            return _failed(
                intent,
                f"Repay amount {ctx.services.format_amount(amount_wei, token.decimals)} {token.symbol} "
                f"exceeds the current debt {ctx.services.format_amount(debt_now, token.decimals)} "
                f"on Fluid vault {setup.vault} (nftId={nft_id}) — an explicit over-repay reverts "
                f"on-chain (Vault__ExcessDebtPayback, 31015). Use repay_full=True for a full "
                f"repayment (the protocol resolves the exact debt at execution time).",
            )
        balance = self._erc20_balance(ctx, intent, token, "repay")
        if isinstance(balance, CompilationResult):
            return balance
        if balance < amount_wei:
            return _failed(
                intent,
                f"Repay pre-flight: wallet holds {ctx.services.format_amount(balance, token.decimals)} "
                f"{token.symbol} but the repay needs "
                f"{ctx.services.format_amount(amount_wei, token.decimals)}. Fund the wallet or "
                f"reduce the amount.",
            )
        transactions = [
            *_exact_erc20_approve_txs(token.address, setup.vault, amount_wei, setup.chain),
            self._operate_transaction(
                setup,
                nft_id,
                0,
                -amount_wei,
                value=0,
                description=(
                    f"Repay {ctx.services.format_amount(amount_wei, token.decimals)} {token.symbol} "
                    f"on Fluid vault {setup.vault} (nftId={nft_id})"
                ),
                tx_type="lending_repay",
            ),
        ]
        metadata = self._base_metadata(setup, nft_id) | {
            "repay_token": token.to_dict(),
            "repay_amount": str(amount_wei),
        }
        logger.info("Compiled fluid_vault REPAY: -%s debt on vault %s (nftId=%s)", amount_wei, setup.vault, nft_id)
        return self._result(intent, transactions, metadata, [])

    # =========================================================================
    # WITHDRAW — operate(nftId, −amount | INT256_MIN, 0, wallet)
    # =========================================================================

    def compile_withdraw(self, ctx: BaseCompilerContext, intent: WithdrawIntent) -> CompilationResult:
        setup = self._vault_setup(ctx, intent)
        if isinstance(setup, CompilationResult):
            return setup
        token = self._leg_token(ctx, setup, intent, intent.token, leg="collateral")
        if isinstance(token, CompilationResult):
            return token
        nft_id = self._resolve_nft(setup, intent, allow_mint=False)
        if isinstance(nft_id, CompilationResult):
            return nft_id

        if setup.discovery:
            transactions = [
                self._operate_transaction(
                    setup,
                    nft_id,
                    -1,
                    0,
                    value=0,
                    description=f"Withdraw {token.symbol} from Fluid vault {setup.vault} (discovery shape)",
                    tx_type="lending_withdraw",
                )
            ]
            metadata = self._base_metadata(setup, nft_id) | {
                "withdraw_token": token.to_dict(),
                "withdraw_amount": "1",
            }
            return self._result(intent, transactions, metadata, [])

        state = self._position_state(setup, intent, nft_id)
        if isinstance(state, CompilationResult):
            return state
        position, vault_data = state

        if getattr(intent, "withdraw_all", False):
            return self._compile_withdraw_all(ctx, setup, intent, token, nft_id, position)
        return self._compile_withdraw_exact(ctx, setup, intent, token, nft_id, position, vault_data)

    def _compile_withdraw_all(
        self,
        ctx: BaseCompilerContext,
        setup: _VaultSetup,
        intent: Any,
        token: Any,
        nft_id: int,
        position: FluidVaultPosition,
    ) -> CompilationResult:
        """Full withdraw = the INT256_MIN sentinel; requires zero debt.

        Removing ALL collateral with any outstanding debt is below every
        possible health floor — the protocol would revert. The teardown
        staircase's final ordering (repay_full THEN withdraw_all) satisfies
        this by construction.
        """
        if position.supply <= 0:
            return _failed(
                intent,
                f"Fluid vault position nftId={nft_id} on {setup.vault} holds no collateral — nothing to withdraw.",
            )
        if position.borrow > 0:
            return _failed(
                intent,
                f"withdraw_all refused: Fluid vault position nftId={nft_id} on {setup.vault} "
                f"still has {ctx.services.format_amount(position.borrow, 0)} raw units of "
                f"outstanding debt. Repay the debt first (repay_full=True) — withdrawing all "
                f"collateral with debt outstanding would revert / be instantly liquidatable.",
            )
        transactions = [
            self._operate_transaction(
                setup,
                nft_id,
                INT256_MIN,
                0,
                value=0,
                description=(
                    f"Withdraw ALL {token.symbol} collateral (int-min sentinel) from Fluid "
                    f"vault {setup.vault} (nftId={nft_id})"
                ),
                tx_type="lending_withdraw",
            )
        ]
        metadata = self._base_metadata(setup, nft_id) | {
            "withdraw_token": token.to_dict(),
            "withdraw_amount": str(position.supply),
            "mode": "withdraw_all_int_min",
        }
        logger.info("Compiled fluid_vault WITHDRAW(all): int-min sentinel on vault %s (nftId=%s)", setup.vault, nft_id)
        return self._result(intent, transactions, metadata, [])

    def _compile_withdraw_exact(
        self,
        ctx: BaseCompilerContext,
        setup: _VaultSetup,
        intent: Any,
        token: Any,
        nft_id: int,
        position: FluidVaultPosition,
        vault_data: FluidVaultData,
    ) -> CompilationResult:
        """Exact withdraws are capped at the CF-aware withdrawable amount AND
        the vault's live withdrawable liquidity (limitsAndAvailability) —
        the latter is time/utilization-gated, so it gets the DISTINCT
        limit-gated (retryable) failure (the Phase-2 maxWithdraw precedent,
        VIB-5104)."""
        amount_wei = self._wei_amount(intent, intent.amount, token, "amount")
        if isinstance(amount_wei, CompilationResult):
            return amount_wei
        withdrawable = self._withdrawable_now(position, vault_data)
        if amount_wei > withdrawable:
            return _failed(
                intent,
                f"Withdraw amount {ctx.services.format_amount(amount_wei, token.decimals)} {token.symbol} "
                f"exceeds the currently withdrawable "
                f"{ctx.services.format_amount(max(withdrawable, 0), token.decimals)} on Fluid vault "
                f"{setup.vault} (nftId={nft_id}; collateral factor "
                f"{vault_data.collateral_factor / 100:.0f}% with outstanding debt). Repay debt "
                f"first or reduce the amount — the on-chain operate() would revert.",
            )
        if amount_wei > vault_data.withdrawable:
            # The Liquidity layer's availability cap, NOT the CF/health cap
            # above: funds are safe and the limit expands on its own.
            return _failed(
                intent,
                f"Fluid withdraw limit-gated (retryable): requested withdraw "
                f"{ctx.services.format_amount(amount_wei, token.decimals)} {token.symbol} exceeds the "
                f"vault's currently withdrawable liquidity "
                f"{ctx.services.format_amount(max(vault_data.withdrawable, 0), token.decimals)} "
                f"{token.symbol} on Fluid vault {setup.vault} (nftId={nft_id}). This cap is "
                f"time/utilization-gated — Fluid's Liquidity-layer limits expand over time, so "
                f"retry later or reduce the amount. Not a hard failure; no position risk.",
                is_transient=True,
            )
        transactions = [
            self._operate_transaction(
                setup,
                nft_id,
                -amount_wei,
                0,
                value=0,
                description=(
                    f"Withdraw {ctx.services.format_amount(amount_wei, token.decimals)} {token.symbol} "
                    f"from Fluid vault {setup.vault} (nftId={nft_id})"
                ),
                tx_type="lending_withdraw",
            )
        ]
        metadata = self._base_metadata(setup, nft_id) | {
            "withdraw_token": token.to_dict(),
            "withdraw_amount": str(amount_wei),
        }
        logger.info("Compiled fluid_vault WITHDRAW: -%s col on vault %s (nftId=%s)", amount_wei, setup.vault, nft_id)
        return self._result(intent, transactions, metadata, [])

    @staticmethod
    def _withdrawable_now(position: FluidVaultPosition, vault_data: FluidVaultData) -> int:
        """Collateral withdrawable while keeping debt under the vault's CF.

        ``min_col`` is the smallest collateral (raw units, vault-oracle
        terms) whose CF-discounted value still covers the current debt;
        ceil-divided so rounding can never overstate the withdrawable slice.
        """
        if position.borrow <= 0:
            return position.supply
        if vault_data.oracle_price_operate <= 0 or vault_data.collateral_factor <= 0:
            return 0  # unpriceable / misconfigured vault — nothing is provably safe
        numerator = position.borrow * _ORACLE_PRICE_SCALE * _BPS
        denominator = vault_data.oracle_price_operate * vault_data.collateral_factor
        min_col = -(-numerator // denominator)  # ceil division
        return max(position.supply - min_col, 0)


__all__ = ["DEFAULT_REPAY_HEADROOM", "FluidVaultCompiler"]
